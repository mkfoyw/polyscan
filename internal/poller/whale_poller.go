package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
	"github.com/mkfoyw/polyscan/internal/services"
	"github.com/mkfoyw/polyscan/internal/types"
	"github.com/mkfoyw/polyscan/internal/whale"
)

// WhalePoller periodically polls the Data API for recent trades by tracked whale wallets.
type WhalePoller struct {
	tracker          *whale.Tracker
	whaleTradDB      *services.WhaleTradeService
	interval         time.Duration
	minDisplayAmount float64 // skip saving trades below this USD amount
	client           *http.Client
	alerts           chan<- types.Alert
	logger           *slog.Logger

	// ProfileLookup resolves a wallet address to a Polymarket display name.
	ProfileLookup func(ctx context.Context, proxyWallet string) string

	// saveInterval controls how often the whale data is persisted
	saveInterval time.Duration
}

// NewWhalePoller creates a new whale poller.
func NewWhalePoller(
	tracker *whale.Tracker,
	whaleTradDB *services.WhaleTradeService,
	interval time.Duration,
	minDisplayAmount float64,
	alerts chan<- types.Alert,
	logger *slog.Logger,
) *WhalePoller {
	return &WhalePoller{
		tracker:          tracker,
		whaleTradDB:      whaleTradDB,
		interval:         interval,
		minDisplayAmount: minDisplayAmount,
		client:           &http.Client{Timeout: 15 * time.Second},
		alerts:           alerts,
		logger:           logger,
		saveInterval:     5 * time.Minute,
	}
}

// Run starts the periodic whale polling loop. Blocks until ctx is cancelled.
func (wp *WhalePoller) Run(ctx context.Context) {
	pollTicker := time.NewTicker(wp.interval)
	defer pollTicker.Stop()

	saveTicker := time.NewTicker(wp.saveInterval)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			wp.logger.Info("whale poller shutting down")
			// Final save with fresh context since main ctx is canceled
			saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := wp.tracker.Save(saveCtx); err != nil {
				wp.logger.Error("failed to save whale data on shutdown", "error", err)
			}
			cancel()
			return
		case <-pollTicker.C:
			wp.pollAll(ctx)
		case <-saveTicker.C:
			if err := wp.tracker.Save(ctx); err != nil {
				wp.logger.Error("failed to save whale data", "error", err)
			}
		}
	}
}

func (wp *WhalePoller) pollAll(ctx context.Context) {
	whales := wp.tracker.GetAll()
	if len(whales) == 0 {
		return
	}

	wp.logger.Debug("polling whale activity", "whale_count", len(whales))

	for _, w := range whales {
		select {
		case <-ctx.Done():
			return
		default:
		}

		activities, err := wp.fetchActivity(ctx, w.Address)
		if err != nil {
			wp.logger.Error("failed to fetch whale activity",
				"whale", w.DisplayName(),
				"error", err,
			)
			continue
		}

		newCount := 0
		profileUpdated := false
		for _, act := range activities {
			// Only process trades newer than last poll
			if act.Timestamp <= w.LastPollTS {
				continue
			}

			// Only process trade-type activities
			if act.Type != "TRADE" {
				continue
			}

			// Skip small trades
			if wp.minDisplayAmount > 0 && act.USDCSize < wp.minDisplayAmount {
				continue
			}

			newCount++
			wp.tracker.UpdateVolume(ctx, w.Address, act.USDCSize)
			wp.sendAlert(ctx, w, &act)

			// Update whale profile from activity data (name/pseudonym).
			// Only do this once per poll cycle.
			if !profileUpdated && (act.Name != "" || act.Pseudonym != "") {
				wp.tracker.UpdateProfile(ctx, w.Address, act.Name, act.Pseudonym)
				profileUpdated = true
			}

			// Persist whale trade to dedicated whale_trades table
			if wp.whaleTradDB != nil {
				rec := &repository.WhaleTrade{
					ProxyWallet:     w.Address,
					Side:            act.Side,
					Asset:           act.Asset,
					ConditionID:     act.ConditionID,
					Size:            act.Size,
					Price:           act.Price,
					USDValue:        act.USDCSize,
					Timestamp:       act.Timestamp,
					Title:           act.Title,
					Slug:            act.Slug,
					EventSlug:       act.EventSlug,
					Outcome:         act.Outcome,
					TransactionHash: act.TransactionHash,
				}
				if err := wp.whaleTradDB.Insert(ctx, rec); err != nil {
					wp.logger.Error("failed to persist whale trade", "error", err)
				}
			}
		}

		// If activity didn't provide profile name and whale has no name/alias yet,
		// try the external profile API (primes cache + stores on whale_users)
		if !profileUpdated && w.Alias == "" && w.Name == "" && wp.ProfileLookup != nil {
			if name := wp.ProfileLookup(ctx, w.Address); name != "" {
				wp.tracker.UpdateProfile(ctx, w.Address, name, "")
			}
		}

		// Update last poll timestamp
		if len(activities) > 0 {
			maxTS := activities[0].Timestamp
			for _, act := range activities[1:] {
				if act.Timestamp > maxTS {
					maxTS = act.Timestamp
				}
			}
			wp.tracker.UpdateLastPollTS(ctx, w.Address, maxTS)
		}

		if newCount > 0 {
			wp.logger.Debug("found new whale trades",
				"whale", w.DisplayName(),
				"count", newCount,
			)
		}

		// Small delay between whale polls to avoid hammering the API
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (wp *WhalePoller) fetchActivity(ctx context.Context, address string) ([]types.Activity, error) {
	url := fmt.Sprintf("%s/activity?user=%s&limit=20", dataAPI, address)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := wp.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("data API returned %d: %s", resp.StatusCode, string(body))
	}

	var activities []types.Activity
	if err := json.NewDecoder(resp.Body).Decode(&activities); err != nil {
		return nil, fmt.Errorf("decode activity: %w", err)
	}

	return activities, nil
}

func (wp *WhalePoller) sendAlert(ctx context.Context, w *whale.WhaleInfo, act *types.Activity) {
	url := ""
	if act.EventSlug != "" {
		url = "https://polymarket.com/event/" + act.EventSlug
	} else if act.Slug != "" {
		url = "https://polymarket.com/market/" + act.Slug
	}

	alert := types.FormatWhaleActivityAlert(
		w.DisplayName(),
		act.Side,
		act.Outcome,
		act.USDCSize,
		act.Size,
		act.Price,
		act.Title,
		url,
	)

	select {
	case wp.alerts <- alert:
	case <-ctx.Done():
	}
}
