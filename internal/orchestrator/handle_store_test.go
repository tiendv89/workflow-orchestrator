package orchestrator

import (
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestHandleStore_RegisterAndLookup(t *testing.T) {
	s := NewHandleStore()
	id := uuid.New()
	entry := HandleEntry{TaskUUID: id, FeatureName: "feat-1", TaskName: "T1"}

	s.Register("h1", entry)

	got, ok := s.Lookup("h1")
	if !ok {
		t.Fatal("Lookup returned false after Register")
	}
	if got.TaskUUID != id {
		t.Errorf("TaskUUID = %v, want %v", got.TaskUUID, id)
	}
	if got.FeatureName != "feat-1" {
		t.Errorf("FeatureName = %q, want feat-1", got.FeatureName)
	}
	if got.TaskName != "T1" {
		t.Errorf("TaskName = %q, want T1", got.TaskName)
	}
}

func TestHandleStore_LookupMissing(t *testing.T) {
	s := NewHandleStore()
	_, ok := s.Lookup("nonexistent")
	if ok {
		t.Fatal("Lookup returned true for unknown handle")
	}
}

func TestHandleStore_Delete(t *testing.T) {
	s := NewHandleStore()
	s.Register("h2", HandleEntry{TaskName: "T2"})
	s.Delete("h2")

	_, ok := s.Lookup("h2")
	if ok {
		t.Fatal("Lookup returned true after Delete")
	}
}

func TestHandleStore_DeleteMissing(t *testing.T) {
	s := NewHandleStore()
	// Should not panic on a delete of an unknown handle.
	s.Delete("unknown")
}

func TestHandleStore_Overwrite(t *testing.T) {
	s := NewHandleStore()
	id1, id2 := uuid.New(), uuid.New()
	s.Register("h", HandleEntry{TaskUUID: id1, TaskName: "T1"})
	s.Register("h", HandleEntry{TaskUUID: id2, TaskName: "T2"})

	got, ok := s.Lookup("h")
	if !ok {
		t.Fatal("Lookup returned false")
	}
	if got.TaskUUID != id2 {
		t.Errorf("overwrite: TaskUUID = %v, want %v", got.TaskUUID, id2)
	}
}

func TestHandleStore_Concurrent(t *testing.T) {
	s := NewHandleStore()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n * 3)

	for i := 0; i < n; i++ {
		handle := "h" + string(rune('a'+i%26))
		entry := HandleEntry{TaskName: "T"}

		go func(h string, e HandleEntry) {
			defer wg.Done()
			s.Register(h, e)
		}(handle, entry)

		go func(h string) {
			defer wg.Done()
			s.Lookup(h)
		}(handle)

		go func(h string) {
			defer wg.Done()
			s.Delete(h)
		}(handle)
	}
	wg.Wait()
}
