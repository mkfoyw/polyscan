package whale

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/mkfoyw/polyscan/internal/store"
)

// WhaleInfo holds metadata about a tracked whale wallet.
type WhaleInfo struct {
	Address     string    `json:"address"`
	Alias       string    `json:"alias"`
	Source      string    `json:"source"` // "auto" or "manual"
	FirstSeenAt time.Time `json:"first_seen_at"`
	TotalVolume float64   `json:"total_volume"`
	LastPollTS  int64     `json:"last_poll_ts"` // last polled trade timestamp
}

// DisplayName returns alias if set, otherwise the full address.
func (w *WhaleInfo) DisplayName() string {
	if w.Alias != "" {
		return w.Alias
	}
	return w.Address
}

// Tracker manages the set of tracked whale wallets.
// It uses an in-memory cache backed by MongoDB for persistence.
type Tracker struct {
	mu         sync.RWMutex
	whales     map[string]*WhaleInfo // address -> WhaleInfo
	maxTracked int
	logger     *slog.Logger
	whaleStore *store.WhaleStore
}

// NewTracker creates a new whale tracker backed by MongoDB.
func NewTracker(maxTracked int, whaleStore *store.WhaleStore, logger *slog.Logger) *Tracker {
	return &Tracker{
		whales:     make(map[string]*WhaleInfo),
		maxTracked: maxTracked,
		logger:     logger,
		whaleStore: whaleStore,
	}
}

// Load loads the whale list from MongoDB into the in-memory cache.
func (t *Tracker) Load(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	records, err := t.whaleStore.GetAll(ctx)
	if err != nil {
		return err
	}

	for _, r := range records {
		t.whales[r.Address] = &WhaleInfo{
			Address:     r.Address,
			Alias:       r.Alias,
			Source:      r.Source,
			FirstSeenAt: r.FirstSeenAt,
			TotalVolume: r.TotalVolume,
			LastPollTS:  r.LastPollTS,
		}
	}

	t.logger.Info("loaded whale data", "count", len(t.whales))
	return nil
}

// Save persists the entire in-memory whale list to MongoDB.
func (t *Tracker) Save(ctx context.Context) error {
	t.mu.RLock()
	whales := make([]*WhaleInfo, 0, len(t.whales))
	for _, w := range t.whales {
		whales = append(whales, w)
	}
	t.mu.RUnlock()

	for _, w := range whales {
		rec := &store.WhaleRecord{
			Address:     w.Address,
			Alias:       w.Alias,
			Source:      w.Source,
			FirstSeenAt: w.FirstSeenAt,
			TotalVolume: w.TotalVolume,
			LastPollTS:  w.LastPollTS,
		}
		if _, err := t.whaleStore.Upsert(ctx, rec); err != nil {
			t.logger.Error("failed to upsert whale to MongoDB", "address", w.Address, "error", err)
		}
	}
	return nil
}

// AddManual adds a manually configured whale address.
func (t *Tracker) AddManual(ctx context.Context, address, alias string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if w, ok := t.whales[address]; ok {
		w.Alias = alias
	} else {
		t.whales[address] = &WhaleInfo{
			Address:     address,
			Alias:       alias,
			Source:      "manual",
			FirstSeenAt: time.Now(),
		}
	}

	// Persist to MongoDB
	rec := &store.WhaleRecord{
		Address:     address,
		Alias:       alias,
		Source:      "manual",
		FirstSeenAt: time.Now(),
	}
	if _, err := t.whaleStore.Upsert(ctx, rec); err != nil {
		t.logger.Error("failed to upsert manual whale", "address", address, "error", err)
	}
}

// AddAuto adds an automatically detected whale. Returns true if newly added.
func (t *Tracker) AddAuto(ctx context.Context, address string, volume float64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if w, ok := t.whales[address]; ok {
		w.TotalVolume += volume
		// Async update volume in MongoDB
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = t.whaleStore.IncrVolume(bgCtx, address, volume)
		}()
		return false
	}

	// Check capacity
	if len(t.whales) >= t.maxTracked {
		t.evictLowest(ctx)
	}

	now := time.Now()
	t.whales[address] = &WhaleInfo{
		Address:     address,
		Source:      "auto",
		FirstSeenAt: now,
		TotalVolume: volume,
	}

	// Persist to MongoDB
	rec := &store.WhaleRecord{
		Address:     address,
		Source:      "auto",
		FirstSeenAt: now,
		TotalVolume: volume,
	}
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := t.whaleStore.Upsert(bgCtx, rec); err != nil {
			t.logger.Error("failed to persist auto whale", "address", address, "error", err)
		}
	}()

	return true
}

// evictLowest removes the whale with the lowest total volume (auto-tracked only).
// Must be called with lock held.
func (t *Tracker) evictLowest(ctx context.Context) {
	var candidates []*WhaleInfo
	for _, w := range t.whales {
		if w.Source == "auto" {
			candidates = append(candidates, w)
		}
	}

	if len(candidates) == 0 {
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].TotalVolume < candidates[j].TotalVolume
	})

	victim := candidates[0]
	delete(t.whales, victim.Address)
	t.logger.Info("evicted whale", "address", victim.Address, "volume", victim.TotalVolume)

	// Also remove from MongoDB
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = t.whaleStore.DeleteLowestVolume(bgCtx)
	}()
}

// GetAll returns a copy of all tracked whales.
func (t *Tracker) GetAll() []*WhaleInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*WhaleInfo, 0, len(t.whales))
	for _, w := range t.whales {
		cp := *w
		result = append(result, &cp)
	}
	return result
}

// Get returns info for a specific whale, or nil if not tracked.
func (t *Tracker) Get(address string) *WhaleInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	w, ok := t.whales[address]
	if !ok {
		return nil
	}
	cp := *w
	return &cp
}

// UpdateLastPollTS updates the last poll timestamp for a whale.
func (t *Tracker) UpdateLastPollTS(ctx context.Context, address string, ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if w, ok := t.whales[address]; ok {
		if ts > w.LastPollTS {
			w.LastPollTS = ts
			go func() {
				bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = t.whaleStore.UpdateLastPollTS(bgCtx, address, ts)
			}()
		}
	}
}

// UpdateVolume adds to the total volume for a whale.
func (t *Tracker) UpdateVolume(ctx context.Context, address string, amount float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if w, ok := t.whales[address]; ok {
		w.TotalVolume += amount
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = t.whaleStore.IncrVolume(bgCtx, address, amount)
		}()
	}
}

// Count returns the number of tracked whales.
func (t *Tracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.whales)
}

// IsTracked returns true if the address is being tracked.
func (t *Tracker) IsTracked(address string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.whales[address]
	return ok
}
