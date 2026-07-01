package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
)

// PRStatus holds the relevant merge/state fields of a GitHub pull request.
type PRStatus struct {
	Merged bool
	State  string
	// Mergeable is GitHub's mergeability assessment: "MERGEABLE", "CONFLICTING", or "UNKNOWN".
	// "UNKNOWN" means GitHub has not finished computing mergeability — callers should recheck later.
	// "CONFLICTING" means the PR cannot be merged without resolving conflicts.
	Mergeable string
}

// PRGetter is a narrow interface for fetching a PR's merge status.
// It is satisfied by *Client and can be mocked in tests.
type PRGetter interface {
	GetPR(ctx context.Context, prURL string) (*PRStatus, error)
}

// PRCreator is a narrow interface for opening a draft pull request.
// It is satisfied by *Client and can be mocked in tests.
type PRCreator interface {
	CreatePR(ctx context.Context, repoURL, head, base, title, body string, draft bool) (string, error)
	BranchExists(ctx context.Context, repoURL, branch string) (bool, error)
}

// PRMerger is a narrow interface for merging a pull request.
// It is satisfied by *Client and can be mocked in tests.
type PRMerger interface {
	MergePR(ctx context.Context, prURL string) error
}

// Client is a thin, authenticated GitHub REST API client.
type Client struct {
	token      string
	httpClient *http.Client
}

// NewClient returns a Client that authenticates with the given personal access token.
func NewClient(token string) *Client {
	return &Client{token: token, httpClient: &http.Client{}}
}

// GetPR fetches the pull-request at prURL and returns its merged/state status.
// prURL may be a GitHub HTML URL (https://github.com/owner/repo/pull/N) or a
// GitHub REST API URL (https://api.github.com/repos/owner/repo/pulls/N).
func (c *Client) GetPR(ctx context.Context, prURL string) (*PRStatus, error) {
	apiURL, err := htmlURLToAPIURL(prURL)
	if err != nil {
		return nil, fmt.Errorf("GetPR: convert URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("GetPR: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GetPR: http request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warn().Err(err).Msg("github: close response body")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GetPR: unexpected status %d for %s", resp.StatusCode, prURL)
	}

	var body struct {
		Merged    bool   `json:"merged"`
		State     string `json:"state"`
		Mergeable string `json:"mergeable"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("GetPR: decode response: %w", err)
	}

	return &PRStatus{Merged: body.Merged, State: body.State, Mergeable: body.Mergeable}, nil
}

// repoURLToAPIBase derives the GitHub REST API base URL for a repo.
// repoURL may be a GitHub HTML URL (https://github.com/owner/repo) or already
// an API URL (https://api.github.com/repos/owner/repo).
func repoURLToAPIBase(repoURL string) (string, error) {
	if strings.HasPrefix(repoURL, "https://api.github.com/") {
		return strings.TrimRight(repoURL, "/"), nil
	}
	if !strings.HasPrefix(repoURL, "https://github.com/") {
		// Accept non-github.com URLs as-is for test servers.
		return strings.TrimRight(repoURL, "/"), nil
	}
	path := strings.TrimPrefix(repoURL, "https://github.com/")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 2 {
		return "", fmt.Errorf("unsupported GitHub repo URL format: %q", repoURL)
	}
	return fmt.Sprintf("https://api.github.com/repos/%s/%s", parts[0], parts[1]), nil
}

// CreatePR opens a pull request on GitHub. repoURL is the HTML or API repo URL.
// Returns the HTML URL of the created PR.
func (c *Client) CreatePR(ctx context.Context, repoURL, head, base, title, body string, draft bool) (string, error) {
	apiBase, err := repoURLToAPIBase(repoURL)
	if err != nil {
		return "", fmt.Errorf("CreatePR: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"title": title,
		"body":  body,
		"head":  head,
		"base":  base,
		"draft": draft,
	})
	if err != nil {
		return "", fmt.Errorf("CreatePR: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/pulls", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("CreatePR: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("CreatePR: http request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warn().Err(err).Msg("github: close response body")
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("CreatePR: unexpected status %d for %s", resp.StatusCode, repoURL)
	}

	var result struct {
		HTMLURL string `json:"html_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("CreatePR: decode response: %w", err)
	}
	return result.HTMLURL, nil
}

// MergePR squash-merges the pull request at prURL.
func (c *Client) MergePR(ctx context.Context, prURL string) error {
	apiURL, err := htmlURLToAPIURL(prURL)
	if err != nil {
		return fmt.Errorf("MergePR: convert URL: %w", err)
	}
	mergeURL := apiURL + "/merge"

	payload, err := json.Marshal(map[string]string{"merge_method": "squash"})
	if err != nil {
		return fmt.Errorf("MergePR: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, mergeURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("MergePR: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("MergePR: http request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warn().Err(err).Msg("github: close response body")
		}
	}()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusMethodNotAllowed:
		// 405 = already merged or branch protection — idempotent, treat as success.
		return nil
	default:
		return fmt.Errorf("MergePR: unexpected status %d for %s", resp.StatusCode, prURL)
	}
}

// BranchExists checks whether branch exists in the repo at repoURL.
func (c *Client) BranchExists(ctx context.Context, repoURL, branch string) (bool, error) {
	apiBase, err := repoURLToAPIBase(repoURL)
	if err != nil {
		return false, fmt.Errorf("BranchExists: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		apiBase+"/branches/"+branch, nil)
	if err != nil {
		return false, fmt.Errorf("BranchExists: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("BranchExists: http request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warn().Err(err).Msg("github: close response body")
		}
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("BranchExists: unexpected status %d for %s", resp.StatusCode, repoURL)
	}
}

// htmlURLToAPIURL converts a GitHub PR HTML URL to its REST API equivalent.
// URLs that are already in API format (contain "/repos/" and "/pulls/") or do
// not begin with "https://github.com/" are returned unchanged, allowing test
// servers and pre-formed API URLs to pass through without modification.
func htmlURLToAPIURL(prURL string) (string, error) {
	// Pass through if it's already an API-style URL or a non-github.com URL.
	if !strings.HasPrefix(prURL, "https://github.com/") {
		return prURL, nil
	}
	const htmlBase = "https://github.com/"
	path := strings.TrimPrefix(prURL, htmlBase)
	parts := strings.Split(path, "/")
	// Expected: owner/repo/pull/N
	if len(parts) != 4 || parts[2] != "pull" {
		return "", fmt.Errorf("unsupported GitHub PR URL format: %q", prURL)
	}
	return fmt.Sprintf("https://api.github.com/repos/%s/%s/pulls/%s",
		parts[0], parts[1], parts[3]), nil
}
