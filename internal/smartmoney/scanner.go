package smartmoney

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/mkfoyw/polyscan/internal/config"
	"github.com/mkfoyw/polyscan/internal/store"
	"github.com/mkfoyw/polyscan/internal/types"
)

const dataAPI = "https://data-api.polymarket.com"

// Scanner discovers smart money candidates from large trades and analyzes
// their trading history to confirm or reject them.
type Scanner struct {
	cfg       config.SmartMoneyConfig
	userStore *store.SmartMoneyUserStore
	client    *http.Client
	logger    *slog.Logger

	// candidateCh receives addresses to analyze asynchronously.
	candidateCh chan candidateReq

	// ProfileLookup resolves a wallet address to a Polymarket display name.
	ProfileLookup func(ctx context.Context, proxyWallet string) string
}

type candidateReq struct {
	address  string
	usdValue float64
	question string
}

// NewScanner creates a new smart money scanner.
func NewScanner(cfg config.SmartMoneyConfig, userStore *store.SmartMoneyUserStore, logger *slog.Logger) *Scanner {
	return &Scanner{
		cfg:         cfg,
		userStore:   userStore,
		client:      &http.Client{Timeout: 15 * time.Second},
		logger:      logger,
		candidateCh: make(chan candidateReq, 200),
	}
}

// AddCandidate is called from the OnLargeTradeREST callback to register an address
// as a smart money candidate. The analysis happens asynchronously.
func (s *Scanner) AddCandidate(ctx context.Context, proxyWallet string, usdValue, price float64, side, question string) {
	// Only consider if amount meets minimum
	if s.cfg.MinCandidateAmount > 0 && usdValue < s.cfg.MinCandidateAmount {
		return
	}
	// Only consider if price is below threshold (low-price bets indicate conviction)
	if s.cfg.MaxCandidatePrice > 0 && price > s.cfg.MaxCandidatePrice {
		return
	}

	// Check if already tracked
	exists, err := s.userStore.Exists(ctx, proxyWallet)
	if err != nil {
		s.logger.Error("failed to check smart money existence", "address", proxyWallet, "error", err)
		return
	}
	if exists {
		return
	}

	// Insert as candidate
	rec := &store.SmartMoneyUser{
		Address:     proxyWallet,
		Source:      "auto",
		Status:      store.SMStatusCandidate,
		FirstSeenAt: time.Now(),
	}
	if err := s.userStore.Upsert(ctx, rec); err != nil {
		s.logger.Error("failed to insert smart money candidate", "address", proxyWallet, "error", err)
		return
	}

	s.logger.Info("smart money candidate added",
		"address", proxyWallet,
		"usd_value", usdValue,
		"question", question,
	)

	// Queue for async analysis
	select {
	case s.candidateCh <- candidateReq{address: proxyWallet, usdValue: usdValue, question: question}:
	default:
		s.logger.Warn("smart money candidate channel full, skipping analysis", "address", proxyWallet)
	}
}

// AddManual adds a manual smart money address (bypasses analysis, immediately confirmed).
func (s *Scanner) AddManual(ctx context.Context, address, alias string) error {
	rec := &store.SmartMoneyUser{
		Address:     address,
		Alias:       alias,
		Source:      "manual",
		Status:      store.SMStatusConfirmed,
		FirstSeenAt: time.Now(),
	}
	if err := s.userStore.Upsert(ctx, rec); err != nil {
		return fmt.Errorf("add manual smart money: %w", err)
	}

	// Resolve profile asynchronously
	if s.ProfileLookup != nil {
		go func() {
			lookupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if name := s.ProfileLookup(lookupCtx, address); name != "" {
				_ = s.userStore.UpdateProfile(lookupCtx, address, name, "")
			}
		}()
	}

	s.logger.Info("manual smart money added", "address", address, "alias", alias)
	return nil
}

// Run starts the async analysis worker. Blocks until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context) {
	s.logger.Info("smart money scanner started")
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("smart money scanner stopped")
			return
		case req := <-s.candidateCh:
			s.analyze(ctx, req)
			// Small delay between analyses to avoid API hammering
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

// analyze fetches the trading history for a candidate and decides whether
// to confirm or reject them as smart money.
func (s *Scanner) analyze(ctx context.Context, req candidateReq) {
	s.logger.Info("analyzing smart money candidate", "address", req.address)

	activities, err := s.fetchActivityHistory(ctx, req.address, s.cfg.AnalysisLimit)
	if err != nil {
		s.logger.Error("failed to fetch activity for analysis",
			"address", req.address,
			"error", err,
		)
		return
	}

	if len(activities) == 0 {
		s.logger.Info("no activity found, rejecting candidate", "address", req.address)
		_ = s.userStore.UpdateStatus(ctx, req.address, store.SMStatusRejected)
		return
	}

	// Analyze trading performance
	stats := s.computeStats(activities)

	// Update stats in DB
	_ = s.userStore.UpdateStats(ctx, req.address,
		stats.winRate, stats.roi,
		stats.totalTrades, stats.winningTrades, stats.totalVolume,
	)

	// Resolve profile name
	if s.ProfileLookup != nil {
		if name := s.ProfileLookup(ctx, req.address); name != "" {
			_ = s.userStore.UpdateProfile(ctx, req.address, name, "")
		}
	}

	// Also try to pick up name from activity data
	for _, act := range activities {
		if act.Name != "" || act.Pseudonym != "" {
			_ = s.userStore.UpdateProfile(ctx, req.address, act.Name, act.Pseudonym)
			break
		}
	}

	// Decision: few trades + large total buy = smart money.
	// Too many trades → permanently rejected (frequent trader, not conviction player).
	tooManyTrades := s.cfg.MaxTrades > 0 && stats.totalTrades > s.cfg.MaxTrades
	meetsMinBuy := s.cfg.MinTotalBuy <= 0 || stats.totalBuyAmount >= s.cfg.MinTotalBuy

	if tooManyTrades {
		// Exceeds max trades — permanently rejected
		_ = s.userStore.UpdateStatus(ctx, req.address, store.SMStatusRejected)
		s.logger.Info("smart money rejected: too many trades",
			"address", req.address,
			"trades", stats.totalTrades,
			"max_trades", s.cfg.MaxTrades,
			"total_buy", fmt.Sprintf("$%.0f", stats.totalBuyAmount),
		)
		return
	}

	if meetsMinBuy {
		// Few trades + large buy amount → confirmed
		confirmed, _ := s.userStore.CountByStatus(ctx, store.SMStatusConfirmed)
		if s.cfg.MaxTracked > 0 && int(confirmed) >= s.cfg.MaxTracked {
			s.logger.Warn("max smart money tracked reached, rejecting", "address", req.address)
			_ = s.userStore.UpdateStatus(ctx, req.address, store.SMStatusRejected)
			return
		}

		_ = s.userStore.UpdateStatus(ctx, req.address, store.SMStatusConfirmed)
		s.logger.Info("smart money confirmed",
			"address", req.address,
			"trades", stats.totalTrades,
			"total_buy", fmt.Sprintf("$%.0f", stats.totalBuyAmount),
			"volume", stats.totalVolume,
		)
	} else {
		// Few trades but buy amount not enough yet — reject (may re-qualify later)
		_ = s.userStore.UpdateStatus(ctx, req.address, store.SMStatusRejected)
		s.logger.Info("smart money rejected: insufficient buy amount",
			"address", req.address,
			"trades", stats.totalTrades,
			"total_buy", fmt.Sprintf("$%.0f", stats.totalBuyAmount),
			"min_total_buy", fmt.Sprintf("$%.0f", s.cfg.MinTotalBuy),
		)
	}
}

type tradeStats struct {
	totalTrades    int
	winningTrades  int
	winRate        float64
	roi            float64
	totalVolume    float64
	totalBuyAmount float64 // total USD spent on BUY trades
}

// computeStats analyzes activities to compute win rate, ROI, etc.
// It groups trades by condition_id and evaluates profit on a per-market basis.
func (s *Scanner) computeStats(activities []types.Activity) tradeStats {
	// Group buy trades by conditionID
	type position struct {
		totalCost float64 // total USDC spent buying
		totalSize float64 // total shares bought
		buys      int
		sells     int
		sellProceeds float64
	}

	positions := make(map[string]*position)
	var totalVolume float64

	var totalBuyAmount float64

	for _, act := range activities {
		if act.Type != "TRADE" {
			continue
		}
		totalVolume += act.USDCSize

		key := act.ConditionID + "|" + act.Outcome
		p, ok := positions[key]
		if !ok {
			p = &position{}
			positions[key] = p
		}

		if act.Side == "BUY" {
			p.totalCost += act.USDCSize
			p.totalSize += act.Size
			p.buys++
			totalBuyAmount += act.USDCSize
		} else {
			p.sellProceeds += act.USDCSize
			p.sells++
		}
	}

	// Evaluate each position
	var totalTrades, winningTrades int
	var totalPnL, totalInvested float64

	for _, p := range positions {
		if p.buys == 0 {
			continue
		}
		totalTrades++
		totalInvested += p.totalCost

		// Calculate PnL: sell proceeds - buy cost
		// For open positions (buys > sells), use average buy price as estimated value
		avgBuyPrice := p.totalCost / p.totalSize
		if p.sells > 0 {
			// Has some exits
			pnl := p.sellProceeds - p.totalCost
			if pnl > 0 {
				winningTrades++
			}
			totalPnL += pnl
		} else {
			// No exits yet — consider it neutral (unrealized)
			// If bought at low price, it's potentially good but unproven
			if avgBuyPrice < 0.5 {
				// Bought at low price — slight positive signal
				winningTrades++ // optimistic for low-price conviction bets
			}
		}
	}

	stats := tradeStats{
		totalTrades:    totalTrades,
		winningTrades:  winningTrades,
		totalVolume:    totalVolume,
		totalBuyAmount: totalBuyAmount,
	}

	if totalTrades > 0 {
		stats.winRate = float64(winningTrades) / float64(totalTrades)
	}
	if totalInvested > 0 {
		stats.roi = (totalPnL / totalInvested) * 100
	}

	return stats
}

// fetchActivityHistory fetches trading history for an address.
func (s *Scanner) fetchActivityHistory(ctx context.Context, address string, limit int) ([]types.Activity, error) {
	url := fmt.Sprintf("%s/activity?user=%s&limit=%d", dataAPI, address, limit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req)
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

// Remove deletes a smart money user by address.
func (s *Scanner) Remove(ctx context.Context, address string) error {
	return s.userStore.Delete(ctx, address)
}

// GetAll returns all smart money users.
func (s *Scanner) GetAll(ctx context.Context) ([]store.SmartMoneyUser, error) {
	return s.userStore.GetAll(ctx)
}

// GetConfirmed returns all confirmed smart money users.
func (s *Scanner) GetConfirmed(ctx context.Context) ([]store.SmartMoneyUser, error) {
	return s.userStore.GetConfirmed(ctx)
}
