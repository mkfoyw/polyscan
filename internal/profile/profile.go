package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
	"github.com/mkfoyw/polyscan/internal/services"
)

const (
	baseURL = "https://gamma-api.polymarket.com/public-profile"
)

// Profile is the public profile returned by Polymarket.
type Profile struct {
	Name                  string `json:"name"`
	Pseudonym             string `json:"pseudonym"`
	ProfileImage          string `json:"profileImage"`
	DisplayUsernamePublic *bool  `json:"displayUsernamePublic"`
}

// DisplayName returns the best display name: user-chosen name first, else pseudonym.
func (p *Profile) DisplayName() string {
	if p.Name != "" {
		return p.Name
	}
	return p.Pseudonym
}

// Client fetches Polymarket public profiles, caching in a SQLite profiles table.
// Profiles are refreshed from the external API only when the cached entry is stale.
type Client struct {
	http          *http.Client
	logger        *slog.Logger
	profileSvc    *services.ProfileService
	staleDuration time.Duration
}

// NewClient creates a new profile client backed by a ProfileStore.
// staleDuration controls how long a cached profile is considered fresh (0 = 24h default).
func NewClient(logger *slog.Logger, profileSvc *services.ProfileService, staleDuration time.Duration) *Client {
	if staleDuration <= 0 {
		staleDuration = 24 * time.Hour
	}
	return &Client{
		http:          &http.Client{Timeout: 10 * time.Second},
		logger:        logger,
		profileSvc:    profileSvc,
		staleDuration: staleDuration,
	}
}

// Lookup returns the display name for a wallet address.
// Checks the DB cache first; only calls the external API if the cache is stale or missing.
// Returns "" if lookup fails.
func (c *Client) Lookup(ctx context.Context, proxyWallet string) string {
	if proxyWallet == "" {
		return ""
	}

	// Check DB cache
	if c.profileSvc != nil {
		rec, err := c.profileSvc.Get(ctx, proxyWallet)
		if err == nil && rec != nil && time.Since(rec.FetchedAt) < c.staleDuration {
			return rec.DisplayName()
		}
	}

	// Fetch from external API
	p, err := c.fetch(ctx, proxyWallet)
	if err != nil {
		c.logger.Debug("profile lookup failed", "wallet", proxyWallet, "error", err)
		return ""
	}

	// Persist to DB cache
	if c.profileSvc != nil {
		_ = c.profileSvc.Upsert(ctx, &repository.ProfileRecord{
			Address:   proxyWallet,
			Name:      p.Name,
			Pseudonym: p.Pseudonym,
			FetchedAt: time.Now(),
		})
	}

	return p.DisplayName()
}

func (c *Client) fetch(ctx context.Context, wallet string) (*Profile, error) {
	url := fmt.Sprintf("%s?address=%s", baseURL, wallet)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var p Profile
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}
