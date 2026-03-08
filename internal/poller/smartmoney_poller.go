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
)

// SmartMoneyPoller periodically polls the Data API for recent trades
// by confirmed smart money wallets.
type SmartMoneyPoller struct {
	userSvc        *services.SmartMoneyUserService
	tradeSvc       *services.SmartMoneyTradeService
	interval         time.Duration
	minDisplayAmount float64
	client           *http.Client
	alerts           chan<- types.Alert
	logger           *slog.Logger

	// ProfileLookup resolves a wallet address to a display name.
	ProfileLookup func(ctx context.Context, proxyWallet string) string

	// OnTradeStored is called when a new smart money trade is stored (for SSE push).
	OnTradeStored func(rec repository.SmartMoneyTrade)
}

// NewSmartMoneyPoller creates a new smart money poller.
func NewSmartMoneyPoller(
	userSvc *services.SmartMoneyUserService,
	tradeSvc *services.SmartMoneyTradeService,
	interval time.Duration,
	minDisplayAmount float64,
	alerts chan<- types.Alert,
	logger *slog.Logger,
) *SmartMoneyPoller {
	return &SmartMoneyPoller{
		userSvc:        userSvc,
		tradeSvc:       tradeSvc,
		interval:         interval,
		minDisplayAmount: minDisplayAmount,
		client:           &http.Client{Timeout: 15 * time.Second},
		alerts:           alerts,
		logger:           logger,
	}
}

// Run starts the periodic smart money polling loop. Blocks until ctx is cancelled.
func (p *SmartMoneyPoller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("smart money poller shutting down")
			return
		case <-ticker.C:
			p.pollAll(ctx)
		}
	}
}

func (p *SmartMoneyPoller) pollAll(ctx context.Context) {
	users, err := p.userSvc.GetConfirmed(ctx)
	if err != nil {
		p.logger.Error("failed to get confirmed smart money users", "error", err)
		return
	}
	if len(users) == 0 {
		return
	}

	p.logger.Debug("polling smart money activity", "count", len(users))

	for _, u := range users {
		select {
		case <-ctx.Done():
			return
		default:
		}

		activities, err := p.fetchActivity(ctx, u.Address)
		if err != nil {
			p.logger.Error("failed to fetch smart money activity",
				"address", u.DisplayName(),
				"error", err,
			)
			continue
		}

		newCount := 0
		profileUpdated := false
		for _, act := range activities {
			// Skip old trades
			if act.Timestamp <= u.LastPollTS {
				continue
			}
			if act.Type != "TRADE" {
				continue
			}
			// Skip small trades
			if p.minDisplayAmount > 0 && act.USDCSize < p.minDisplayAmount {
				continue
			}

			newCount++
			_ = p.userSvc.IncrVolume(ctx, u.Address, act.USDCSize)

			// Update profile from activity data (once per cycle)
			if !profileUpdated && (act.Name != "" || act.Pseudonym != "") {
				_ = p.userSvc.UpdateProfile(ctx, u.Address, act.Name, act.Pseudonym)
				profileUpdated = true
			}

			// Persist trade
			rec := &repository.SmartMoneyTrade{
				ProxyWallet:     u.Address,
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
			merged, err := p.tradeSvc.UpsertMerge(ctx, rec)
			if err != nil {
				p.logger.Error("failed to persist smart money trade", "error", err)
				continue
			}

			// Send SSE event
			if p.OnTradeStored != nil {
				merged.ProfileName = u.DisplayName()
				p.OnTradeStored(*merged)
			}

			// Send alert
			p.sendAlert(ctx, &u, &act)
		}

		// Try external profile lookup if no name yet
		if !profileUpdated && u.Alias == "" && u.Name == "" && p.ProfileLookup != nil {
			if name := p.ProfileLookup(ctx, u.Address); name != "" {
				_ = p.userSvc.UpdateProfile(ctx, u.Address, name, "")
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
			_ = p.userSvc.UpdateLastPollTS(ctx, u.Address, maxTS)
		}

		if newCount > 0 {
			p.logger.Debug("found new smart money trades",
				"address", u.DisplayName(),
				"count", newCount,
			)
		}

		// Rate limit between polls
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (p *SmartMoneyPoller) fetchActivity(ctx context.Context, address string) ([]types.Activity, error) {
	url := fmt.Sprintf("%s/activity?user=%s&limit=20", dataAPI, address)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.Do(req)
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

func (p *SmartMoneyPoller) sendAlert(ctx context.Context, u *repository.SmartMoneyUser, act *types.Activity) {
	url := ""
	if act.EventSlug != "" {
		url = "https://polymarket.com/event/" + act.EventSlug
	} else if act.Slug != "" {
		url = "https://polymarket.com/market/" + act.Slug
	}

	alert := types.FormatSmartMoneyAlert(
		u.DisplayName(),
		act.Side,
		act.Outcome,
		act.USDCSize,
		act.Size,
		act.Price,
		act.Title,
		url,
	)

	select {
	case p.alerts <- alert:
	case <-ctx.Done():
	}
}
