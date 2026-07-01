package orchestrator_test

import (
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// --- Headroom unit tests ---

func TestHeadroom_FullAvailable(t *testing.T) {
	h := orchestrator.Headroom(5, 0)
	if h != 5 {
		t.Errorf("Headroom(5, 0) = %d, want 5", h)
	}
}

func TestHeadroom_PartiallyConsumed(t *testing.T) {
	h := orchestrator.Headroom(5, 3)
	if h != 2 {
		t.Errorf("Headroom(5, 3) = %d, want 2", h)
	}
}

func TestHeadroom_ExactlyFull(t *testing.T) {
	h := orchestrator.Headroom(5, 5)
	if h != 0 {
		t.Errorf("Headroom(5, 5) = %d, want 0", h)
	}
}

func TestHeadroom_Overshot_ClampedToZero(t *testing.T) {
	// Multi-instance races can push inflight above max — clamp to 0, never negative.
	h := orchestrator.Headroom(5, 7)
	if h != 0 {
		t.Errorf("Headroom(5, 7) = %d, want 0 (overshoot bounded)", h)
	}
}

func TestHeadroom_ZeroMax(t *testing.T) {
	h := orchestrator.Headroom(0, 0)
	if h != 0 {
		t.Errorf("Headroom(0, 0) = %d, want 0", h)
	}
}
