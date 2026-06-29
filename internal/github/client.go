package github

import (
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
}

// PRGetter is a narrow interface for fetching a PR's merge status.
// It is satisfied by *Client and can be mocked in tests.
type PRGetter interface {
	GetPR(ctx context.Context, prURL string) (*PRStatus, error)
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
		Merged bool   `json:"merged"`
		State  string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("GetPR: decode response: %w", err)
	}

	return &PRStatus{Merged: body.Merged, State: body.State}, nil
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
