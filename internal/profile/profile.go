package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	baseURL    = "https://gamma-api.polymarket.com/public-profile"
	maxCacheSz = 10000
)

// Profile is the public profile returned by Polymarket.
type Profile struct {
	Name                   string `json:"name"`
	Pseudonym              string `json:"pseudonym"`
	ProfileImage           string `json:"profileImage"`
	DisplayUsernamePublic  *bool  `json:"displayUsernamePublic"`
}

// DisplayName returns the best display name: user-chosen name first, else pseudonym.
func (p *Profile) DisplayName() string {
	if p.Name != "" {
		return p.Name
	}
	return p.Pseudonym
}

// Client fetches and caches Polymarket public profiles.
type Client struct {
	http   *http.Client
	logger *slog.Logger

	mu    sync.RWMutex
	cache map[string]*cacheEntry // proxyWallet -> entry
}

type cacheEntry struct {
	profile *Profile
	at      time.Time
}

// NewClient creates a new profile client.
func NewClient(logger *slog.Logger) *Client {
	return &Client{
		http: &http.Client{Timeout: 10 * time.Second},
		logger: logger,
		cache:  make(map[string]*cacheEntry, 256),
	}
}

// Lookup returns the display name for a wallet address.
// Results are cached for 1 hour. Returns "" if lookup fails.
func (c *Client) Lookup(ctx context.Context, proxyWallet string) string {
	if proxyWallet == "" {
		return ""
	}

	// Check cache
	c.mu.RLock()
	if e, ok := c.cache[proxyWallet]; ok && time.Since(e.at) < time.Hour {
		c.mu.RUnlock()
		return e.profile.DisplayName()
	}
	c.mu.RUnlock()

	// Fetch from API
	p, err := c.fetch(ctx, proxyWallet)
	if err != nil {
		c.logger.Debug("profile lookup failed", "wallet", proxyWallet, "error", err)
		return ""
	}

	// Cache result
	c.mu.Lock()
	// Evict randomly if cache too large
	if len(c.cache) >= maxCacheSz {
		for k := range c.cache {
			delete(c.cache, k)
			if len(c.cache) < maxCacheSz/2 {
				break
			}
		}
	}
	c.cache[proxyWallet] = &cacheEntry{profile: p, at: time.Now()}
	c.mu.Unlock()

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
