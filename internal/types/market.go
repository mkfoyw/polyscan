package types

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FlexFloat handles JSON values that can be either a number or a string-encoded number.
type FlexFloat float64

func (f *FlexFloat) UnmarshalJSON(data []byte) error {
	// Try number first
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		*f = FlexFloat(n)
		return nil
	}
	// Try string
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*f = 0
		return nil
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*f = FlexFloat(n)
	return nil
}

// MarketInfo holds metadata about a Polymarket market.
type MarketInfo struct {
	ID              string    `json:"id"`
	ConditionID     string    `json:"conditionId"`
	Question        string    `json:"question"`
	Slug            string    `json:"slug"`
	Outcomes        string    `json:"outcomes"`        // JSON string: ["Yes","No"]
	OutcomePrices   string    `json:"outcomePrices"`   // JSON string: ["0.45","0.55"]
	ClobTokenIDs    string    `json:"clobTokenIds"`    // JSON string: ["token_yes","token_no"]
	Volume          FlexFloat `json:"volume"`
	VolumeNum       float64   `json:"volumeNum"`
	Active          bool      `json:"active"`
	Closed          bool      `json:"closed"`
	EventSlug       string    `json:"-"`
	EnableOrderBook bool      `json:"enableOrderBook"`
	Image           string    `json:"image"`
	Icon            string    `json:"icon"`

	// Parsed fields (not from JSON)
	YesTokenID  string    `json:"-"`
	NoTokenID   string    `json:"-"`
	Category    string    `json:"-"`
	FirstSeenAt time.Time `json:"-"`
}

// ParseTokenIDs parses the ClobTokenIDs JSON string into YesTokenID and NoTokenID.
func (m *MarketInfo) ParseTokenIDs() error {
	var ids []string
	if err := json.Unmarshal([]byte(m.ClobTokenIDs), &ids); err != nil {
		return err
	}
	if len(ids) >= 1 {
		m.YesTokenID = ids[0]
	}
	if len(ids) >= 2 {
		m.NoTokenID = ids[1]
	}
	return nil
}

// PolymarketURL returns the full Polymarket URL for this market.
func (m *MarketInfo) PolymarketURL() string {
	if m.EventSlug != "" {
		return "https://polymarket.com/event/" + m.EventSlug
	}
	return "https://polymarket.com/market/" + m.Slug
}

// GammaEventTag represents a tag from the Gamma API.
type GammaEventTag struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Slug  string `json:"slug"`
}

// GammaEvent represents a top-level event from the Gamma API.
type GammaEvent struct {
	ID      string           `json:"id"`
	Slug    string           `json:"slug"`
	Title   string           `json:"title"`
	Markets []*MarketInfo    `json:"markets"`
	Tags    []GammaEventTag  `json:"tags"`
	Image   string           `json:"image"`
}

// MarketStore is a thread-safe store for market metadata keyed by asset ID.
type MarketStore struct {
	mu      sync.RWMutex
	byAsset map[string]*MarketInfo // assetID -> MarketInfo
	byCond  map[string]*MarketInfo // conditionID -> MarketInfo
}

// NewMarketStore creates a new MarketStore.
func NewMarketStore() *MarketStore {
	return &MarketStore{
		byAsset: make(map[string]*MarketInfo),
		byCond:  make(map[string]*MarketInfo),
	}
}

// Upsert inserts or updates a market. Returns true if the market is newly added.
func (s *MarketStore) Upsert(m *MarketInfo) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, exists := s.byCond[m.ConditionID]
	if exists {
		m.FirstSeenAt = existing.FirstSeenAt
	} else {
		m.FirstSeenAt = time.Now()
	}
	s.byCond[m.ConditionID] = m
	if m.YesTokenID != "" {
		s.byAsset[m.YesTokenID] = m
	}
	if m.NoTokenID != "" {
		s.byAsset[m.NoTokenID] = m
	}
	return !exists
}

// GetByAsset returns the market info for a given asset (token) ID.
func (s *MarketStore) GetByAsset(assetID string) *MarketInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byAsset[assetID]
}

// GetByCondition returns the market info for a given condition ID.
func (s *MarketStore) GetByCondition(conditionID string) *MarketInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byCond[conditionID]
}

// AllAssetIDs returns all tracked asset IDs.
func (s *MarketStore) AllAssetIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.byAsset))
	for id := range s.byAsset {
		ids = append(ids, id)
	}
	return ids
}

// Count returns the number of tracked markets (by condition ID).
func (s *MarketStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byCond)
}

// AllMarkets returns all tracked markets (deduplicated by condition ID).
func (s *MarketStore) AllMarkets() []*MarketInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*MarketInfo, 0, len(s.byCond))
	for _, m := range s.byCond {
		result = append(result, m)
	}
	return result
}

// OutcomeForAsset returns "Yes" or "No" depending on which token ID matches.
func (s *MarketStore) OutcomeForAsset(assetID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.byAsset[assetID]
	if !ok {
		return "Unknown"
	}
	if assetID == m.YesTokenID {
		return "Yes"
	}
	return "No"
}

// PruneOlderThan removes markets whose FirstSeenAt is before cutoff.
// Returns the number of pruned markets.
func (s *MarketStore) PruneOlderThan(cutoff time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pruned int
	for cond, m := range s.byCond {
		if m.FirstSeenAt.Before(cutoff) {
			delete(s.byCond, cond)
			if m.YesTokenID != "" {
				delete(s.byAsset, m.YesTokenID)
			}
			if m.NoTokenID != "" {
				delete(s.byAsset, m.NoTokenID)
			}
			pruned++
		}
	}
	return pruned
}

// RecentNew returns the most recently discovered markets, sorted by FirstSeenAt desc.
// If before is non-zero, only returns markets discovered before that time.
func (s *MarketStore) RecentNew(limit int, before time.Time) []*MarketInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*MarketInfo, 0, len(s.byCond))
	for _, m := range s.byCond {
		if !before.IsZero() && !m.FirstSeenAt.Before(before) {
			continue
		}
		all = append(all, m)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].FirstSeenAt.After(all[j].FirstSeenAt)
	})
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}

// RecentNewByCategory returns recently discovered markets filtered by category.
// If before is non-zero, only returns markets discovered before that time.
func (s *MarketStore) RecentNewByCategory(category string, limit int, before time.Time) []*MarketInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var filtered []*MarketInfo
	for _, m := range s.byCond {
		if !strings.EqualFold(m.Category, category) {
			continue
		}
		if !before.IsZero() && !m.FirstSeenAt.Before(before) {
			continue
		}
		filtered = append(filtered, m)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].FirstSeenAt.After(filtered[j].FirstSeenAt)
	})
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered
}
