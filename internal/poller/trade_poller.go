package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/mkfoyw/polyscan/internal/types"
)

const dataAPI = "https://data-api.polymarket.com"

// TradePoller periodically polls the Data API for large trades as a
// backup to the WebSocket feed.
type TradePoller struct {
	threshold float64
	interval  time.Duration
	client    *http.Client
	logger    *slog.Logger

	// OnTrade is called for each new large trade found.
	OnTrade func(ctx context.Context, trade types.Trade)

	// OnBackfill is called for initial trades (no Telegram alert, just DB storage).
	OnBackfill func(ctx context.Context, trade types.Trade)

	lastTimestamp int64
	initialized   bool // skip alerts on first poll to avoid flooding
}

// NewTradePoller creates a new trade poller.
func NewTradePoller(threshold float64, interval time.Duration, logger *slog.Logger) *TradePoller {
	return &TradePoller{
		threshold: threshold,
		interval:  interval,
		client:    &http.Client{Timeout: 15 * time.Second},
		logger:    logger,
	}
}

// Run starts the periodic polling loop. Blocks until ctx is cancelled.
func (tp *TradePoller) Run(ctx context.Context) {
	// Initial poll
	tp.poll(ctx)

	ticker := time.NewTicker(tp.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			tp.logger.Info("trade poller shutting down")
			return
		case <-ticker.C:
			tp.poll(ctx)
		}
	}
}

func (tp *TradePoller) poll(ctx context.Context) {
	url := fmt.Sprintf(
		"%s/trades?filterType=CASH&filterAmount=%.0f&limit=100&takerOnly=true",
		dataAPI, tp.threshold,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		tp.logger.Error("create trade poll request", "error", err)
		return
	}

	resp, err := tp.client.Do(req)
	if err != nil {
		tp.logger.Error("trade poll request failed", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		tp.logger.Error("trade poll bad status", "status", resp.StatusCode, "body", string(body))
		return
	}

	var trades []types.Trade
	if err := json.NewDecoder(resp.Body).Decode(&trades); err != nil {
		tp.logger.Error("decode trades", "error", err)
		return
	}

	// Process only new trades (newer than lastTimestamp)
	newCount := 0
	var maxTs int64
	for i := range trades {
		trade := trades[i]
		if trade.Timestamp <= tp.lastTimestamp {
			continue
		}
		if trade.Timestamp > maxTs {
			maxTs = trade.Timestamp
		}
		if !tp.initialized {
			// On first poll, backfill trades to DB (for dashboard) but don't alert
			if tp.OnBackfill != nil {
				tp.OnBackfill(ctx, trade)
			}
			continue
		}
		newCount++
		if tp.OnTrade != nil {
			tp.OnTrade(ctx, trade)
		}
	}

	if maxTs > tp.lastTimestamp {
		tp.lastTimestamp = maxTs
	}

	if !tp.initialized {
		tp.initialized = true
		tp.logger.Info("trade poller initialized, watermark set", "last_ts", tp.lastTimestamp, "backfilled", len(trades))
		return
	}

	if newCount > 0 {
		tp.logger.Debug("trade poll found new trades", "count", newCount)
	}
}
