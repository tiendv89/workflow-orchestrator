// Package main implements the Go orchestrator binary. It runs a continuous
// poll cycle: find eligible tasks → claim → dispatch → reap → merge-poll → sleep.
package main

import (
	"context"
	"fmt"
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
	defer rdb.Close()

	streamer := orchestrator.NewRedisStreamer(rdb)
	dispatcher := orchestrator.NewDispatcher(cfg.BrokerURL, streamer, nil)
	hs := orchestrator.NewHandleStore()
	ghClient := gh.NewClient(cfg.GitHubToken)

	go serveHealthz(ctx)

	executorID := fmt.Sprintf("go-orchestrator/%s", cfg.WorkspaceID)

	log.Info().
		Int("poll_interval_seconds", cfg.PollIntervalSeconds).
		Str("workspace_id", cfg.WorkspaceID).
		Msg("orchestrator started")

	lc := loopConfig{
		findEligible: func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error) {
			return orchestrator.FindEligibleTasks(ctx, pool, wsID)
		},
		claimTask: func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID, executor string) (bool, error) {
			return orchestrator.ClaimTask(ctx, pool, wsID, taskID, executor)
		},
		rollbackClaim: func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID) (bool, error) {
			return orchestrator.RollbackClaim(ctx, pool, wsID, taskID)
		},
		dispatch: func(ctx context.Context, c *config.Config, task db.WorkspaceTask, handle string) error {
			return dispatcher.Dispatch(ctx, c, task, handle)
		},
		reapCompleted: func(ctx context.Context, c *config.Config, pool *pgxpool.Pool, hs *orchestrator.HandleStore) error {
			return orchestrator.ReapCompleted(ctx, c, pool, hs)
		},
		pollMergedPRs: func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) error {
			return orchestrator.PollMergedPRs(ctx, ghClient, pool, wsID)
		},
		newHandle: uuid.New,
	}

	ticker := time.NewTicker(time.Duration(cfg.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	// Run the first cycle immediately before waiting for the first tick.
	runCycle(ctx, cfg, pool, workspaceID, hs, executorID, lc)

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("orchestrator shutting down")
			return
		case <-ticker.C:
			runCycle(ctx, cfg, pool, workspaceID, hs, executorID, lc)
		}
	}
}

// loopConfig holds injectable functions for each step of the poll cycle.
// Production code wires real implementations; tests inject stubs.
type loopConfig struct {
	findEligible  func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) ([]db.WorkspaceTask, error)
	claimTask     func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID, executor string) (bool, error)
	rollbackClaim func(ctx context.Context, pool *pgxpool.Pool, wsID, taskID uuid.UUID) (bool, error)
	dispatch      func(ctx context.Context, cfg *config.Config, task db.WorkspaceTask, handle string) error
	reapCompleted func(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, hs *orchestrator.HandleStore) error
	pollMergedPRs func(ctx context.Context, pool *pgxpool.Pool, wsID uuid.UUID) error
	newHandle     func() uuid.UUID
}

// runCycle executes one full poll iteration. Each step's errors are logged and
// do not crash the loop — the next cycle will retry.
func runCycle(
	ctx context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	workspaceID uuid.UUID,
	hs *orchestrator.HandleStore,
	executorID string,
	lc loopConfig,
) {
	log.Debug().Msg("poll cycle start")

	// Step a: find eligible tasks and claim + dispatch each.
	tasks, err := lc.findEligible(ctx, pool, workspaceID)
	if err != nil {
		log.Error().Err(err).Msg("poll: FindEligibleTasks")
	} else {
		for _, task := range tasks {
			handle := lc.newHandle().String()

			won, claimErr := lc.claimTask(ctx, pool, workspaceID, task.TaskID, executorID)
			if claimErr != nil {
				log.Error().Err(claimErr).Str("task", task.TaskName).Msg("poll: ClaimTask error")
				continue
			}
			if !won {
				log.Debug().Str("task", task.TaskName).Msg("poll: claim lost — another instance won")
				continue
			}

			if dispatchErr := lc.dispatch(ctx, cfg, task, handle); dispatchErr != nil {
				log.Error().Err(dispatchErr).Str("task", task.TaskName).Msg("poll: Dispatch error — rolling back claim")
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

	// Step b: reap completed tasks from the broker.
	if err := lc.reapCompleted(ctx, cfg, pool, hs); err != nil {
		log.Error().Err(err).Msg("poll: ReapCompleted error")
	}

	// Step c: poll GitHub for merged PRs.
	if err := lc.pollMergedPRs(ctx, pool, workspaceID); err != nil {
		log.Error().Err(err).Msg("poll: PollMergedPRs error")
	}

	log.Debug().Msg("poll cycle complete")
}

// serveHealthz starts a minimal HTTP server that returns 200 OK on /healthz.
// Used by docker-compose health checks to confirm the orchestrator is running.
// When ctx is cancelled the listener is shut down cleanly.
func serveHealthz(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("healthz server error")
	}
}
