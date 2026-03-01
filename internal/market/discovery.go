package market

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mkfoyw/polyscan/internal/store"
	"github.com/mkfoyw/polyscan/internal/types"
)

const gammaAPI = "https://gamma-api.polymarket.com"

// Discovery periodically fetches all active markets from the Gamma API
// and maintains the MarketStore.
type Discovery struct {
	store           *types.MarketStore
	settlementStore *store.SettlementStore
	client          *http.Client
	logger          *slog.Logger
	interval        time.Duration

	// OnNewMarkets is called whenever new markets are discovered.
	// The callback receives a list of new asset IDs to subscribe to.
	OnNewMarkets func(assetIDs []string)

	// OnNewSettlement is called when a settlement is synced (for SSE push).
	OnNewSettlement func(store.SettlementRecord)

	// OnNewMarketInfo is called when a new market is discovered (for SSE push).
	OnNewMarketInfo func(*types.MarketInfo)
}

// NewDiscovery creates a new market Discovery.
func NewDiscovery(mktStore *types.MarketStore, settlementStore *store.SettlementStore, interval time.Duration, logger *slog.Logger) *Discovery {
	return &Discovery{
		store:           mktStore,
		settlementStore: settlementStore,
		client:          &http.Client{Timeout: 30 * time.Second},
		logger:          logger,
		interval:        interval,
	}
}

// Run starts the periodic market sync loop. It blocks until ctx is cancelled.
func (d *Discovery) Run(ctx context.Context) {
	// Initial sync
	d.sync(ctx)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("market discovery shutting down")
			return
		case <-ticker.C:
			d.sync(ctx)
		}
	}
}

// sync fetches all active markets and updates the store.
func (d *Discovery) sync(ctx context.Context) {
	d.logger.Info("syncing markets from Gamma API")

	offset := 0
	limit := 100
	totalNew := 0
	var newAssetIDs []string

	for {
		events, err := d.fetchEvents(ctx, offset, limit)
		if err != nil {
			d.logger.Error("failed to fetch events", "offset", offset, "error", err)
			break
		}

		if len(events) == 0 {
			break
		}

		for _, event := range events {
			for _, m := range event.Markets {
				if m.ConditionID == "" || m.ClobTokenIDs == "" {
					continue
				}
				if err := m.ParseTokenIDs(); err != nil {
					d.logger.Warn("failed to parse token IDs",
						"market", m.Question,
						"error", err,
					)
					continue
				}
				// Inherit event slug if market doesn't have one
				if m.EventSlug == "" {
					m.EventSlug = event.Slug
				}
				// Propagate event image and category to market
				if m.Image == "" {
					m.Image = event.Image
				}
				if len(event.Tags) > 0 {
					m.Category = normalizeCategory(event.Tags[0].Slug)
				}
				if isNew := d.store.Upsert(m); isNew {
					totalNew++
					if m.YesTokenID != "" {
						newAssetIDs = append(newAssetIDs, m.YesTokenID)
					}
					if m.NoTokenID != "" {
						newAssetIDs = append(newAssetIDs, m.NoTokenID)
					}
					// SSE push for new markets
					if d.OnNewMarketInfo != nil {
						info := d.store.GetByCondition(m.ConditionID)
						if info != nil {
							d.OnNewMarketInfo(info)
						}
					}
				}
			}
		}

		if len(events) < limit {
			break
		}
		offset += limit
	}

	d.logger.Info("market sync complete",
		"total_markets", d.store.Count(),
		"new_markets", totalNew,
	)

	if len(newAssetIDs) > 0 && d.OnNewMarkets != nil {
		d.OnNewMarkets(newAssetIDs)
	}

	// Sync recently settled markets
	d.syncSettlements(ctx)
}

// fetchEvents fetches a page of active events from the Gamma API with retry.
func (d *Discovery) fetchEvents(ctx context.Context, offset, limit int) ([]types.GammaEvent, error) {
	url := fmt.Sprintf("%s/events?active=true&closed=false&limit=%d&offset=%d&order=volume24hr&ascending=false",
		gammaAPI, limit, offset)
	return d.fetchEventsFromURL(ctx, url)
}

// fetchEventsFromURL fetches events from a given URL with retry logic.
func (d *Discovery) fetchEventsFromURL(ctx context.Context, url string) ([]types.GammaEvent, error) {

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt*2) * time.Second):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) polyscan/1.0")
		req.Header.Set("Accept", "application/json")

		resp, err := d.client.Do(req)
		if err != nil {
			lastErr = err
			d.logger.Warn("gamma API request failed, retrying", "attempt", attempt+1, "error", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("gamma API returned %d: %s", resp.StatusCode, string(body))
			d.logger.Warn("gamma API bad status, retrying", "attempt", attempt+1, "status", resp.StatusCode)
			continue
		}

		var events []types.GammaEvent
		if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("decode events: %w", err)
		}
		resp.Body.Close()
		return events, nil
	}

	return nil, fmt.Errorf("gamma API failed after 3 attempts: %w", lastErr)
}

// syncSettlements fetches recently closed events and stores settlement records.
func (d *Discovery) syncSettlements(ctx context.Context) {
	if d.settlementStore == nil {
		return
	}

	url := fmt.Sprintf("%s/events?active=false&closed=true&limit=20&order=volume24hr&ascending=false", gammaAPI)
	events, err := d.fetchEventsFromURL(ctx, url)
	if err != nil {
		d.logger.Warn("failed to fetch settled events", "error", err)
		return
	}

	count := 0
	for _, event := range events {
		for _, m := range event.Markets {
			if m.ConditionID == "" {
				continue
			}
			outcome := resolvedOutcome(m.OutcomePrices)
			if outcome == "" {
				continue
			}
			imgURL := m.Image
			if imgURL == "" {
				imgURL = event.Image
			}
			rec := &store.SettlementRecord{
				ConditionID: m.ConditionID,
				Question:    m.Question,
				Slug:        m.Slug,
				EventSlug:   event.Slug,
				Outcome:     outcome,
				ImageURL:    imgURL,
			}
			if err := d.settlementStore.Upsert(ctx, rec); err != nil {
				d.logger.Error("failed to store settlement", "error", err)
			} else {
				count++
				if d.OnNewSettlement != nil {
					d.OnNewSettlement(*rec)
				}
			}
		}
	}
	if count > 0 {
		d.logger.Info("synced settlements", "count", count)
	}
}

// resolvedOutcome parses outcomePrices JSON to determine the winning outcome.
func resolvedOutcome(outcomePrices string) string {
	if outcomePrices == "" {
		return ""
	}
	var prices []string
	if err := json.Unmarshal([]byte(outcomePrices), &prices); err != nil {
		return ""
	}
	if len(prices) >= 2 {
		if prices[0] == "1" || prices[0] == "1.0" || prices[0] == "1.00" {
			return "YES"
		}
		if prices[1] == "1" || prices[1] == "1.0" || prices[1] == "1.00" {
			return "NO"
		}
	}
	return ""
}

// normalizeCategory maps Gamma API tag strings to display categories.
func normalizeCategory(tag string) string {
	tag = strings.ToLower(strings.TrimSpace(tag))
	switch tag {
	case "politics":
		return "Politics"
	case "sports":
		return "Sports"
	case "crypto", "cryptocurrency":
		return "Crypto"
	case "finance":
		return "Finance"
	case "geopolitics":
		return "Geopolitics"
	case "earnings":
		return "Earnings"
	case "tech", "technology":
		return "Tech"
	case "pop-culture", "culture", "entertainment":
		return "Culture"
	case "world":
		return "World"
	case "economy", "economics":
		return "Economy"
	case "climate", "science", "climate-science", "climate-&-science":
		return "Climate & Science"
	case "elections":
		return "Elections"
	case "mentions":
		return "Mentions"
	default:
		if len(tag) > 0 {
			return strings.ToUpper(tag[:1]) + tag[1:]
		}
		return tag
	}
}
