package orchestrator

import (
	"sync"

	"github.com/google/uuid"
)

// HandleEntry records the DB identifiers for a dispatched task handle.
type HandleEntry struct {
	FeatureUUID uuid.UUID
	TaskUUID    uuid.UUID
	FeatureName string
	TaskName    string
}

// HandleStore is an in-process, thread-safe map from handle string to HandleEntry.
// It is used by the reap loop (T12) to resolve broker completions back to DB
// rows without an extra DB round-trip.
type HandleStore struct {
	mu      sync.RWMutex
	entries map[string]HandleEntry
}

// NewHandleStore returns an empty HandleStore ready for use.
func NewHandleStore() *HandleStore {
	return &HandleStore{entries: make(map[string]HandleEntry)}
}

// Register stores handle → entry. Overwrites any existing entry for the handle.
func (s *HandleStore) Register(handle string, entry HandleEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[handle] = entry
}

// Lookup returns the entry for handle and true, or the zero value and false if
// the handle is not registered.
func (s *HandleStore) Lookup(handle string) (HandleEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[handle]
	return e, ok
}

// Delete removes the entry for handle. No-op if the handle is not registered.
func (s *HandleStore) Delete(handle string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, handle)
}
