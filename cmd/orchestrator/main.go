// Package main implements the Go orchestrator binary. It runs a continuous
// poll cycle: find eligible tasks → claim → dispatch → reap → merge-poll → sleep.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	"github.com/tiendv89/workflow-orchestrator/internal/database"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	gh "github.com/tiendv89/workflow-orchestrator/internal/github"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("load config")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("open database")
	}
	defer database.Close(pool)

	workspaceID, err := uuid.Parse(cfg.WorkspaceID)
	if err != nil {
		log.Fatal().Err(err).Str("workspace_id", cfg.WorkspaceID).Msg("parse workspace ID")
	}

	rdbOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatal().Err(err).Msg("parse redis URL")
	}
	rdb := redis.NewClient(rdbOpts)
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Warn().Err(err).Msg("close redis client")
		}
	}()

	streamer := orchestrator.NewRedisStreamer(rdb)
	q := db.New(pool)
	dispatcher := orchestrator.NewDispatcher(cfg.BrokerURL, streamer, nil, q)
	hs := orchestrator.NewHandleStore()
	ghClient := gh.NewClient(cfg.GitHubToken)

	go serveHealthz(ctx, cfg.HealthPort)

	executorID := fmt.Sprintf("go-orchestrator/%s", cfg.WorkspaceID)

	log.Info().
		Int("poll_interval_seconds", cfg.PollIntervalSeconds).
		Str("workspace_id", cfg.WorkspaceID).
		Msg("orchestrator started")

	lc := loopConfig{
		findEligible: func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error) {
			return orchestrator.FindEligibleTasks(ctx, pool, wsID)
		},
		claimTask: func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID, featureName, taskName, executor string) (bool, error) {
			return orchestrator.ClaimTask(ctx, pool, wsID, taskID, featureName, taskName, executor)
		},
		rollbackClaim: func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID) (bool, error) {
			return orchestrator.RollbackClaim(ctx, pool, wsID, taskID)
		},
		dispatch: func(ctx context.Context, c *config.Config, task db.WorkspaceTask, handle string) error {
			return dispatcher.Dispatch(ctx, c, task, handle)
		},
		findReviewable: func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error) {
			return orchestrator.FindReviewableTasks(ctx, pool, wsID)
		},
		dispatchReviewer: func(ctx context.Context, pool *pgxpool.Pool, c *config.Config, wsID uuid.UUID, task db.WorkspaceTask, hs *orchestrator.HandleStore) (bool, error) {
			return orchestrator.DispatchReviewer(ctx, pool, c, wsID, task, dispatcher, hs, ghClient)
		},
		findFixable: func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error) {
			return orchestrator.FindFixableTasks(ctx, pool, wsID)
		},
		dispatchFix: func(ctx context.Context, pool *pgxpool.Pool, c *config.Config, wsID uuid.UUID, task db.WorkspaceTask, hs *orchestrator.HandleStore) (bool, error) {
			return orchestrator.DispatchFix(ctx, pool, c, wsID, task, dispatcher, hs)
		},
		reapCompleted: func(ctx context.Context, c *config.Config, pool *pgxpool.Pool, hs *orchestrator.HandleStore) error {
			return orchestrator.ReapCompleted(ctx, c, pool, hs)
		},
		reconcileStuck: func(ctx context.Context, c *config.Config, pool *pgxpool.Pool) error {
			return orchestrator.ReconcileStuckDispatches(ctx, c, pool, dispatcher, ghClient)
		},
		pollMergedPRs: func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) error {
			return orchestrator.PollMergedPRs(ctx, ghClient, pool, wsID)
		},
		newHandle: uuid.New,
	}

	ps := newPollState(cfg.PollIntervalSeconds)

	// Run the first cycle immediately before the first backoff sleep.
	hadError := runCycle(ctx, cfg, pool, workspaceID, hs, executorID, lc)

	for {
		sleep := ps.next(hadError)
		log.Debug().Dur("sleep_ms", sleep).Msg("poll: sleeping before next cycle")
		select {
		case <-ctx.Done():
			log.Info().Msg("orchestrator shutting down")
			return
		case <-time.After(sleep):
		}
		hadError = runCycle(ctx, cfg, pool, workspaceID, hs, executorID, lc)
	}
}

// pollState tracks the current backoff interval for the poll loop. On
// consecutive errors the interval doubles up to maxBackoff; on success it
// resets to base. A ±20% jitter is applied each call to spread load.
type pollState struct {
	base       time.Duration
	maxBackoff time.Duration
	current    time.Duration
}

func newPollState(intervalSecs int) pollState {
	base := time.Duration(intervalSecs) * time.Second
	return pollState{
		base:       base,
		maxBackoff: 5 * base,
		current:    base,
	}
}

// next returns the next sleep duration and updates internal state. On error
// the interval doubles (capped at maxBackoff); on success it resets to base.
// A uniform ±20% jitter is applied to prevent thundering-herd across instances.
func (s *pollState) next(hadError bool) time.Duration {
	if hadError {
		s.current = min(s.current*2, s.maxBackoff)
	} else {
		s.current = s.base
	}
	factor := 0.8 + rand.Float64()*0.4 //nolint:gosec // non-cryptographic jitter
	return time.Duration(float64(s.current) * factor)
}

// loopConfig holds injectable functions for each step of the poll cycle.
// Production code wires real implementations; tests inject stubs.
type loopConfig struct {
	findEligible     func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error)
	claimTask        func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID, featureName, taskName, executor string) (bool, error)
	rollbackClaim    func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID) (bool, error)
	dispatch         func(ctx context.Context, cfg *config.Config, task db.WorkspaceTask, handle string) error
	findReviewable   func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error)
	dispatchReviewer func(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, wsID uuid.UUID, task db.WorkspaceTask, hs *orchestrator.HandleStore) (bool, error)
	findFixable      func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error)
	dispatchFix      func(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, wsID uuid.UUID, task db.WorkspaceTask, hs *orchestrator.HandleStore) (bool, error)
	reapCompleted    func(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, hs *orchestrator.HandleStore) error
	reconcileStuck   func(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool) error
	pollMergedPRs    func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) error
	newHandle        func() uuid.UUID
}

// runCycle executes one full poll iteration. Each step's errors are logged and
// do not crash the loop — the next cycle will retry. Returns true if any step
// encountered an error (used by the caller to drive backoff).
func runCycle(
	ctx context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	workspaceID uuid.UUID,
	hs *orchestrator.HandleStore,
	executorID string,
	lc loopConfig,
) bool {
	log.Debug().Msg("poll cycle start")
	hadError := false

	// Step a: find eligible tasks and claim + dispatch each.
	tasks, err := lc.findEligible(ctx, pool, workspaceID)
	if err != nil {
		log.Error().Err(err).Msg("poll: FindEligibleTasks")
		hadError = true
	} else {
		for _, task := range tasks {
			handle := lc.newHandle().String()

			won, claimErr := lc.claimTask(ctx, pool, workspaceID, task.TaskID, task.FeatureName, task.TaskName, executorID)
			if claimErr != nil {
				log.Error().Err(claimErr).Str("task", task.TaskName).Msg("poll: ClaimTask error")
				hadError = true
				continue
			}
			if !won {
				log.Debug().Str("task", task.TaskName).Msg("poll: claim lost — another instance won")
				continue
			}

			if dispatchErr := lc.dispatch(ctx, cfg, task, handle); dispatchErr != nil {
				log.Error().Err(dispatchErr).Str("task", task.TaskName).Msg("poll: Dispatch error — rolling back claim")
				hadError = true
				if _, rbErr := lc.rollbackClaim(ctx, pool, workspaceID, task.TaskID); rbErr != nil {
					log.Error().Err(rbErr).Str("task", task.TaskName).Msg("poll: RollbackClaim error — task may be stuck in in_progress")
				}
				continue
			}

			hs.Register(handle, orchestrator.HandleEntry{
				FeatureUUID: task.FeatureID,
				TaskUUID:    task.TaskID,
				FeatureName: task.FeatureName,
				TaskName:    task.TaskName,
			})

			log.Info().
				Str("handle", handle).
				Str("task", task.TaskName).
				Str("feature", task.FeatureName).
				Msg("poll: task dispatched")
		}
	}

	// Step b: find reviewable tasks and dispatch a reviewer for each.
	// findReviewable/dispatchReviewer and findFixable/dispatchFix are nil-guarded
	// like reconcileStuck below — existing callers that don't wire the reviewer
	// cluster still work.
	if lc.findReviewable != nil && lc.dispatchReviewer != nil {
		reviewable, err := lc.findReviewable(ctx, pool, workspaceID)
		if err != nil {
			log.Error().Err(err).Msg("poll: FindReviewableTasks")
			hadError = true
		} else {
			for _, task := range reviewable {
				won, reviewErr := lc.dispatchReviewer(ctx, pool, cfg, workspaceID, task, hs)
				if reviewErr != nil {
					log.Error().Err(reviewErr).Str("task", task.TaskName).Msg("poll: DispatchReviewer error")
					hadError = true
					continue
				}
				if !won {
					log.Debug().Str("task", task.TaskName).Msg("poll: reviewer claim lost — another instance won")
					continue
				}
				log.Info().
					Str("task", task.TaskName).
					Str("feature", task.FeatureName).
					Msg("poll: reviewer dispatched")
			}
		}
	}

	// Step c: find change_requested tasks and dispatch a fix agent for each.
	if lc.findFixable != nil && lc.dispatchFix != nil {
		fixable, err := lc.findFixable(ctx, pool, workspaceID)
		if err != nil {
			log.Error().Err(err).Msg("poll: FindFixableTasks")
			hadError = true
		} else {
			for _, task := range fixable {
				won, fixErr := lc.dispatchFix(ctx, pool, cfg, workspaceID, task, hs)
				if fixErr != nil {
					log.Error().Err(fixErr).Str("task", task.TaskName).Msg("poll: DispatchFix error")
					hadError = true
					continue
				}
				if !won {
					log.Debug().Str("task", task.TaskName).Msg("poll: fix claim lost — another instance won")
					continue
				}
				log.Info().
					Str("task", task.TaskName).
					Str("feature", task.FeatureName).
					Msg("poll: fix agent dispatched")
			}
		}
	}

	// Step d: reap completed tasks from the broker.
	if err := lc.reapCompleted(ctx, cfg, pool, hs); err != nil {
		log.Error().Err(err).Msg("poll: ReapCompleted error")
		hadError = true
	}

	// Step e: reconcile stuck dispatches (crash/timeout recovery).
	if lc.reconcileStuck != nil {
		if err := lc.reconcileStuck(ctx, cfg, pool); err != nil {
			log.Error().Err(err).Msg("poll: ReconcileStuck error")
			hadError = true
		}
	}

	// Step f: poll GitHub for merged PRs (ground truth for done transitions).
	if err := lc.pollMergedPRs(ctx, pool, workspaceID); err != nil {
		log.Error().Err(err).Msg("poll: PollMergedPRs error")
		hadError = true
	}

	log.Debug().Msg("poll cycle complete")
	return hadError
}

// serveHealthz starts a minimal HTTP server that returns 200 OK on /healthz.
// Used by docker-compose health checks. A port-bind failure is logged but does
// not terminate the orchestrator process.
func serveHealthz(ctx context.Context, port int) {
	startHealthzServer(ctx, fmt.Sprintf(":%d", port))
}

// startHealthzServer binds addr and serves /healthz. Extracted for
// testability — callers can supply an arbitrary address.
func startHealthzServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error().Err(err).Str("addr", addr).Msg("healthz server error — orchestrator continues")
	}
}
