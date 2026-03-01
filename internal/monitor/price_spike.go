package monitor

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/mkfoyw/polyscan/internal/config"
	"github.com/mkfoyw/polyscan/internal/store"
	"github.com/mkfoyw/polyscan/internal/types"
)

// priceSample represents a single price observation.
type priceSample struct {
	ts       time.Time
	midpoint float64
}

// assetWindow holds the sliding window of price samples for one asset.
type assetWindow struct {
	samples  []priceSample
	maxAge   time.Duration // max window duration to keep
	cooldown map[string]time.Time // ruleKey -> last alert time
}

func newAssetWindow(maxAge time.Duration) *assetWindow {
	return &assetWindow{
		samples:  make([]priceSample, 0, 128),
		maxAge:   maxAge,
		cooldown: make(map[string]time.Time),
	}
}

func (w *assetWindow) add(ts time.Time, midpoint float64) {
	w.samples = append(w.samples, priceSample{ts: ts, midpoint: midpoint})
	w.trim(ts)
}

// trim removes samples older than maxAge.
func (w *assetWindow) trim(now time.Time) {
	cutoff := now.Add(-w.maxAge - time.Minute) // small buffer
	i := 0
	for i < len(w.samples) && w.samples[i].ts.Before(cutoff) {
		i++
	}
	if i > 0 {
		copy(w.samples, w.samples[i:])
		w.samples = w.samples[:len(w.samples)-i]
	}
}

// priceAt returns the midpoint at or just after the given time.
// Returns 0, false if no sample is found.
func (w *assetWindow) priceAt(t time.Time) (float64, bool) {
	for _, s := range w.samples {
		if !s.ts.Before(t) {
			return s.midpoint, true
		}
	}
	return 0, false
}

// latest returns the most recent sample.
func (w *assetWindow) latest() (priceSample, bool) {
	if len(w.samples) == 0 {
		return priceSample{}, false
	}
	return w.samples[len(w.samples)-1], true
}

// PriceSpike monitors WebSocket best_bid_ask events for rapid price increases.
type PriceSpike struct {
	rules        []config.PriceSpikeRule
	maxAge       time.Duration
	cooldown     time.Duration
	store        *types.MarketStore
	priceEventDB *store.PriceEventStore // MongoDB price event persistence
	alerts       chan<- types.Alert
	logger       *slog.Logger

	mu      sync.Mutex
	windows map[string]*assetWindow // assetID -> sliding window
}

// NewPriceSpike creates a new price spike monitor.
func NewPriceSpike(
	rules []config.PriceSpikeRule,
	cooldown time.Duration,
	store *types.MarketStore,
	priceEventDB *store.PriceEventStore,
	alerts chan<- types.Alert,
	logger *slog.Logger,
) *PriceSpike {
	// Find the max window duration
	var maxAge time.Duration
	for _, r := range rules {
		if r.Window.Duration > maxAge {
			maxAge = r.Window.Duration
		}
	}
	// Add buffer for lookback
	maxAge += time.Minute

	return &PriceSpike{
		rules:        rules,
		maxAge:       maxAge,
		cooldown:     cooldown,
		store:        store,
		priceEventDB: priceEventDB,
		alerts:       alerts,
		logger:       logger,
		windows:      make(map[string]*assetWindow),
	}
}

// Run consumes WebSocket best_bid_ask events and checks for price spikes.
// Blocks until ctx is cancelled.
func (ps *PriceSpike) Run(ctx context.Context, bidAskCh <-chan types.WSBestBidAsk) {
	for {
		select {
		case <-ctx.Done():
			ps.logger.Info("price spike monitor shutting down")
			return
		case msg, ok := <-bidAskCh:
			if !ok {
				return
			}
			ps.processEvent(ctx, msg)
		}
	}
}

func (ps *PriceSpike) processEvent(ctx context.Context, msg types.WSBestBidAsk) {
	bestBid, err := strconv.ParseFloat(msg.BestBid, 64)
	if err != nil {
		return
	}
	bestAsk, err := strconv.ParseFloat(msg.BestAsk, 64)
	if err != nil {
		return
	}

	midpoint := (bestBid + bestAsk) / 2.0
	if midpoint <= 0 {
		return
	}

	// Parse timestamp (milliseconds)
	tsMs, err := strconv.ParseInt(msg.Timestamp, 10, 64)
	if err != nil {
		return
	}
	ts := time.UnixMilli(tsMs)

	ps.mu.Lock()
	w, ok := ps.windows[msg.AssetID]
	if !ok {
		w = newAssetWindow(ps.maxAge)
		ps.windows[msg.AssetID] = w
	}
	w.add(ts, midpoint)
	ps.mu.Unlock()

	// Check each rule
	for _, rule := range ps.rules {
		ps.checkRule(ctx, msg.AssetID, w, rule, ts, midpoint)
	}
}

func (ps *PriceSpike) checkRule(ctx context.Context, assetID string, w *assetWindow, rule config.PriceSpikeRule, now time.Time, currentPrice float64) {
	windowStart := now.Add(-rule.Window.Duration)
	oldPrice, ok := w.priceAt(windowStart)
	if !ok {
		return
	}

	if oldPrice <= 0 {
		return
	}

	pctChange := ((currentPrice - oldPrice) / oldPrice) * 100.0
	if pctChange < rule.Percent {
		return
	}

	// Check cooldown
	ruleKey := assetID + ":" + rule.Window.Duration.String()
	ps.mu.Lock()
	if lastAlert, ok := w.cooldown[ruleKey]; ok && time.Since(lastAlert) < ps.cooldown {
		ps.mu.Unlock()
		return
	}
	w.cooldown[ruleKey] = time.Now()
	ps.mu.Unlock()

	// Enrich with market info
	market := ps.store.GetByAsset(assetID)
	question := "Unknown Market"
	outcome := ps.store.OutcomeForAsset(assetID)
	url := ""
	if market != nil {
		question = market.Question
		url = market.PolymarketURL()
	}

	windowStr := formatDuration(rule.Window.Duration)

	ps.logger.Info("price spike detected",
		"market", question,
		"outcome", outcome,
		"window", windowStr,
		"pct_change", pctChange,
		"old_price", oldPrice,
		"new_price", currentPrice,
	)

	alert := types.FormatPriceSpikeAlert(question, outcome, windowStr, pctChange, oldPrice, currentPrice, url)
	select {
	case ps.alerts <- alert:
	case <-ctx.Done():
	}

	// Persist price event to MongoDB
	if ps.priceEventDB != nil {
		conditionID := ""
		if market != nil {
			conditionID = market.ConditionID
		}
		rec := &store.PriceEventRecord{
			AssetID:     assetID,
			Market:      conditionID,
			Question:    question,
			Outcome:     outcome,
			OldPrice:    oldPrice,
			NewPrice:    currentPrice,
			PctChange:   pctChange,
			Window:      windowStr,
			DetectedAt:  now,
		}
		if err := ps.priceEventDB.Insert(ctx, rec); err != nil {
			ps.logger.Error("failed to persist price event", "error", err)
		}
	}
}

func formatDuration(d time.Duration) string {
	if d >= time.Hour {
		h := int(d.Hours())
		if h == 1 {
			return "1小时"
		}
		return strconv.Itoa(h) + "小时"
	}
	m := int(d.Minutes())
	return strconv.Itoa(m) + "分钟"
}

// TopMovers returns markets with the largest price changes over the given window.
// Results are deduplicated by condition_id and sorted by absolute price change.
func (ps *PriceSpike) TopMovers(window time.Duration, limit int) []types.PriceMover {
	now := time.Now()

	type rawMover struct {
		assetID       string
		currentPrice  float64
		previousPrice float64
	}

	ps.mu.Lock()
	raw := make([]rawMover, 0, len(ps.windows))
	for assetID, w := range ps.windows {
		current, ok := w.latest()
		if !ok || now.Sub(current.ts) > 30*time.Minute {
			continue
		}
		windowStart := now.Add(-window)
		oldPrice, ok := w.priceAt(windowStart)
		if !ok || oldPrice <= 0 {
			continue
		}
		raw = append(raw, rawMover{
			assetID:       assetID,
			currentPrice:  current.midpoint,
			previousPrice: oldPrice,
		})
	}
	ps.mu.Unlock()

	// Enrich with market info and deduplicate by condition_id
	dedup := make(map[string]types.PriceMover)
	for _, r := range raw {
		market := ps.store.GetByAsset(r.assetID)
		if market == nil {
			continue
		}
		pctChange := ((r.currentPrice - r.previousPrice) / r.previousPrice) * 100.0
		priceChange := (r.currentPrice - r.previousPrice) * 100.0 // cents
		outcome := ps.store.OutcomeForAsset(r.assetID)

		mover := types.PriceMover{
			ConditionID:   market.ConditionID,
			Question:      market.Question,
			Outcome:       outcome,
			Slug:          market.Slug,
			ImageURL:      market.Image,
			CurrentPrice:  r.currentPrice,
			PreviousPrice: r.previousPrice,
			PriceChange:   priceChange,
			PctChange:     pctChange,
			URL:           market.PolymarketURL(),
		}

		if existing, ok := dedup[market.ConditionID]; ok {
			if math.Abs(mover.PriceChange) > math.Abs(existing.PriceChange) {
				dedup[market.ConditionID] = mover
			}
		} else {
			dedup[market.ConditionID] = mover
		}
	}

	result := make([]types.PriceMover, 0, len(dedup))
	for _, m := range dedup {
		result = append(result, m)
	}
	sort.Slice(result, func(i, j int) bool {
		return math.Abs(result[i].PriceChange) > math.Abs(result[j].PriceChange)
	})

	if len(result) > limit {
		result = result[:limit]
	}
	return result
}
