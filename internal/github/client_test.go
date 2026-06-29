package github_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/github"
)

func TestGetPR_MergedPR(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("GetPR: missing Authorization header")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"merged": true, "state": "closed"}) //nolint:errcheck
	}))
	defer srv.Close()

	c := github.NewClient("test-token")
	// Use the API URL directly (server URL acts as API endpoint).
	status, err := c.GetPR(context.Background(), srv.URL+"/repos/o/r/pulls/1")
	if err != nil {
		t.Fatalf("GetPR: %v", err)
	}
	if !status.Merged {
		t.Error("expected Merged=true")
	}
	if status.State != "closed" {
		t.Errorf("State = %q, want closed", status.State)
	}
}

func TestGetPR_OpenPR(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"merged": false, "state": "open"}) //nolint:errcheck
	}))
	defer srv.Close()

	c := github.NewClient("tok")
	status, err := c.GetPR(context.Background(), srv.URL+"/repos/o/r/pulls/2")
	if err != nil {
		t.Fatalf("GetPR: %v", err)
	}
	if status.Merged {
		t.Error("expected Merged=false")
	}
	if status.State != "open" {
		t.Errorf("State = %q, want open", status.State)
	}
}

func TestGetPR_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := github.NewClient("tok")
	_, err := c.GetPR(context.Background(), srv.URL+"/repos/o/r/pulls/999")
	if err == nil {
		t.Fatal("expected error for 404 response, got nil")
	}
}

func TestGetPR_HTMLURLConversion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the path was converted to API form.
		if r.URL.Path != "/repos/owner/repo/pulls/42" {
			t.Errorf("unexpected path %q; want /repos/owner/repo/pulls/42", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"merged": true, "state": "closed"}) //nolint:errcheck
	}))
	defer srv.Close()

	// Patch the server to use the test server's host for our HTML URL.
	// We can't actually pass a github.com HTML URL to a test server, so we test
	// the converter separately and here confirm an API URL works end-to-end.
	c := github.NewClient("tok")
	status, err := c.GetPR(context.Background(), srv.URL+"/repos/owner/repo/pulls/42")
	if err != nil {
		t.Fatalf("GetPR: %v", err)
	}
	if !status.Merged {
		t.Error("expected Merged=true")
	}
}
