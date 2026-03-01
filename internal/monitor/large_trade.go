package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/mkfoyw/polyscan/internal/store"
	"github.com/mkfoyw/polyscan/internal/types"
)

// LargeTrade monitors WebSocket last_trade_price events for large trades.
type LargeTrade struct {
	threshold float64 // USD threshold
	maxPrice  float64 // only alert when price < maxPrice (0 = no filter)
	store     *types.MarketStore
	alerts    chan<- types.Alert
	logger    *slog.Logger
	tradeDB   *store.TradeStore // MongoDB trade persistence
	client    *http.Client      // for REST enrichment of WS trades

	// OnLargeTrade is called when a large trade is detected.
	// Used to notify the whale tracker of the proxy wallet.
	// Note: WebSocket events don't contain wallet info, so this is
	// only triggered from REST poller data.
	OnLargeTradeREST func(proxyWallet string, usdValue float64, price float64, side string, question string)

	// OnTradeStored is called whenever a trade is stored in the database.
	// Used to push real-time updates to SSE clients.
	OnTradeStored func(store.TradeRecord)

	// ProfileLookup resolves a wallet address to a Polymarket display name.
	// Injected from main.go. Returns "" if not available.
	ProfileLookup func(ctx context.Context, proxyWallet string) string

	// Dedup: track recent trade keys to avoid duplicates
	mu       sync.Mutex
	seen     map[string]time.Time
}

// NewLargeTrade creates a new large trade monitor.
func NewLargeTrade(threshold, maxPrice float64, store *types.MarketStore, tradeDB *store.TradeStore, alerts chan<- types.Alert, client *http.Client, logger *slog.Logger) *LargeTrade {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &LargeTrade{
		threshold: threshold,
		maxPrice:  maxPrice,
		store:     store,
		tradeDB:   tradeDB,
		alerts:    alerts,
		client:    client,
		logger:    logger,
		seen:      make(map[string]time.Time),
	}
}

// RunWS consumes WebSocket last_trade_price events and checks for large trades.
// Blocks until ctx is cancelled.
func (lt *LargeTrade) RunWS(ctx context.Context, tradeCh <-chan types.WSLastTradePrice) {
	// Periodically clean up old dedup entries
	cleanTicker := time.NewTicker(5 * time.Minute)
	defer cleanTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			lt.logger.Info("large trade WS monitor shutting down")
			return
		case <-cleanTicker.C:
			lt.cleanDedup()
		case msg, ok := <-tradeCh:
			if !ok {
				return
			}
			lt.processWSEvent(ctx, msg)
		}
	}
}

func (lt *LargeTrade) processWSEvent(ctx context.Context, msg types.WSLastTradePrice) {
	size, err := strconv.ParseFloat(msg.Size, 64)
	if err != nil {
		return
	}
	price, err := strconv.ParseFloat(msg.Price, 64)
	if err != nil {
		return
	}

	usdValue := size * price
	if usdValue < lt.threshold {
		return
	}

	// Dedup key: asset + timestamp
	dedupKey := msg.AssetID + ":" + msg.Timestamp
	if lt.isDuplicate(dedupKey) {
		return
	}

	// Enrich with market info
	market := lt.store.GetByAsset(msg.AssetID)
	question := "Unknown Market"
	outcome := lt.store.OutcomeForAsset(msg.AssetID)
	url := ""
	if market != nil {
		question = market.Question
		url = market.PolymarketURL()
	}

	// Parse timestamp for DB (WS sends ms, Data API uses seconds – normalise to seconds)
	tsRaw, _ := strconv.ParseInt(msg.Timestamp, 10, 64)
	tsSec := tsRaw
	if tsRaw > 1e12 { // clearly milliseconds
		tsSec = tsRaw / 1000
	}

	lt.logger.Info("large trade detected (WS)",
		"market", question,
		"side", msg.Side,
		"outcome", outcome,
		"usd_value", usdValue,
		"size", size,
		"price", price,
	)

	// Persist to MongoDB (store all trades above threshold for dashboard)
	if lt.tradeDB != nil {
		conditionID := ""
		if market != nil {
			conditionID = market.ConditionID
		}
		rec := &store.TradeRecord{
			Side:        msg.Side,
			Asset:       msg.AssetID,
			ConditionID: conditionID,
			Size:        size,
			Price:       price,
			USDValue:    usdValue,
			Timestamp:   tsSec,
			Title:       question,
			Outcome:     outcome,
			Source:      "ws",
		}
		if err := lt.tradeDB.Insert(ctx, rec); err != nil {
			lt.logger.Error("failed to persist WS trade", "error", err)
		} else if lt.OnTradeStored != nil {
			lt.OnTradeStored(*rec)
		}

		// Async REST enrichment: look up wallet info for this trade
		go lt.enrichWSTrade(msg.AssetID, tsSec, size)
	}

	// Send Telegram alert only for low-probability outcomes (maxPrice filter)
	if lt.maxPrice > 0 && price >= lt.maxPrice {
		return
	}

	alert := types.FormatLargeTradeAlert(question, msg.Side, outcome, usdValue, size, price, "WebSocket", url)
	select {
	case lt.alerts <- alert:
	case <-ctx.Done():
	}
}

// ProcessRESTTrade processes a trade from the REST poller.
func (lt *LargeTrade) ProcessRESTTrade(ctx context.Context, trade types.Trade) {
	usdValue := trade.USDValue()
	if usdValue < lt.threshold {
		return
	}

	// Dedup by transaction hash
	if trade.TransactionHash != "" {
		if lt.isDuplicate(trade.TransactionHash) {
			return
		}
	}

	// Also dedup by asset + timestamp
	dedupKey := trade.Asset + ":" + strconv.FormatInt(trade.Timestamp, 10)
	if lt.isDuplicate(dedupKey) {
		return
	}

	url := ""
	if trade.EventSlug != "" {
		url = "https://polymarket.com/event/" + trade.EventSlug
	} else if trade.Slug != "" {
		url = "https://polymarket.com/market/" + trade.Slug
	}

	lt.logger.Info("large trade detected (REST)",
		"market", trade.Title,
		"side", trade.Side,
		"outcome", trade.Outcome,
		"usd_value", usdValue,
		"wallet", trade.ProxyWallet,
	)

	// Persist: enrich existing WS trade or insert new REST record
	if lt.tradeDB != nil {
		profileName := trade.Name // Data API returns the actual username
		if profileName == "" && lt.ProfileLookup != nil && trade.ProxyWallet != "" {
			profileName = lt.ProfileLookup(ctx, trade.ProxyWallet)
		}
		rec := &store.TradeRecord{
			ProxyWallet:     trade.ProxyWallet,
			Pseudonym:       trade.Pseudonym,
			ProfileName:     profileName,
			Side:            trade.Side,
			Asset:           trade.Asset,
			ConditionID:     trade.ConditionID,
			Size:            trade.Size,
			Price:           trade.Price,
			USDValue:        usdValue,
			Timestamp:       trade.Timestamp,
			Title:           trade.Title,
			Slug:            trade.Slug,
			EventSlug:       trade.EventSlug,
			Outcome:         trade.Outcome,
			TransactionHash: trade.TransactionHash,
			Source:          "rest",
		}
		if err := lt.tradeDB.UpsertRESTTrade(ctx, rec); err != nil {
			lt.logger.Error("failed to persist REST trade", "error", err)
		} else if lt.OnTradeStored != nil {
			lt.OnTradeStored(*rec)
		}
	}

	// Notify whale tracker
	if lt.OnLargeTradeREST != nil && trade.ProxyWallet != "" {
		lt.OnLargeTradeREST(trade.ProxyWallet, usdValue, trade.Price, trade.Side, trade.Title)
	}

	// Send Telegram alert only for low-probability outcomes (maxPrice filter)
	if lt.maxPrice > 0 && trade.Price >= lt.maxPrice {
		return
	}

	alert := types.FormatLargeTradeAlert(
		trade.Title, trade.Side, trade.Outcome,
		usdValue, trade.Size, trade.Price,
		trade.DisplayName(), url,
	)
	select {
	case lt.alerts <- alert:
	case <-ctx.Done():
	}
}

func (lt *LargeTrade) isDuplicate(key string) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if _, ok := lt.seen[key]; ok {
		return true
	}
	lt.seen[key] = time.Now()
	return false
}

func (lt *LargeTrade) cleanDedup() {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	cutoff := time.Now().Add(-10 * time.Minute)
	for k, t := range lt.seen {
		if t.Before(cutoff) {
			delete(lt.seen, k)
		}
	}
}

// BackfillRESTTrade stores a trade to DB without sending Telegram alerts.
// Used during initial startup to populate dashboard with recent trades.
func (lt *LargeTrade) BackfillRESTTrade(ctx context.Context, trade types.Trade) {
	usdValue := trade.USDValue()
	if usdValue < lt.threshold {
		return
	}

	// Dedup by transaction hash
	if trade.TransactionHash != "" {
		if lt.isDuplicate(trade.TransactionHash) {
			return
		}
	}

	if lt.tradeDB == nil {
		return
	}

	profileName := trade.Name // Data API returns the actual username
	if profileName == "" && lt.ProfileLookup != nil && trade.ProxyWallet != "" {
		profileName = lt.ProfileLookup(ctx, trade.ProxyWallet)
	}
	rec := &store.TradeRecord{
		ProxyWallet:     trade.ProxyWallet,
		Pseudonym:       trade.Pseudonym,
		ProfileName:     profileName,
		Side:            trade.Side,
		Asset:           trade.Asset,
		ConditionID:     trade.ConditionID,
		Size:            trade.Size,
		Price:           trade.Price,
		USDValue:        usdValue,
		Timestamp:       trade.Timestamp,
		Title:           trade.Title,
		Slug:            trade.Slug,
		EventSlug:       trade.EventSlug,
		Outcome:         trade.Outcome,
		TransactionHash: trade.TransactionHash,
		Source:          "rest",
	}
	if err := lt.tradeDB.Insert(ctx, rec); err != nil {
		lt.logger.Debug("backfill trade insert", "error", err)
	}
}

const dataAPIBase = "https://data-api.polymarket.com"

// enrichWSTrade asynchronously looks up a WS-detected trade via REST API
// to fill in wallet info (proxyWallet, pseudonym, transactionHash, etc.)
func (lt *LargeTrade) enrichWSTrade(assetID string, tsSec int64, size float64) {
	// Small delay to allow the trade to appear in the data API
	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// The Data API doesn't support filtering by asset, so we fetch recent large
	// trades globally and match by asset + size + approximate timestamp.
	url := fmt.Sprintf("%s/trades?filterType=CASH&filterAmount=%.0f&limit=50&takerOnly=true", dataAPIBase, lt.threshold)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := lt.client.Do(req)
	if err != nil {
		lt.logger.Info("enrich WS trade request failed", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		lt.logger.Info("enrich WS trade bad status", "status", resp.StatusCode)
		return
	}

	var trades []types.Trade
	if err := json.NewDecoder(resp.Body).Decode(&trades); err != nil {
		lt.logger.Info("enrich WS trade decode error", "error", err)
		return
	}

	lt.logger.Info("enrich WS trade searching", "asset", assetID[:20], "tsSec", tsSec, "size", size, "candidates", len(trades))

	// Find the matching trade by asset + fuzzy timestamp (±10s) + same size
	for _, t := range trades {
		if t.Asset != assetID {
			continue
		}
		diff := t.Timestamp - tsSec
		if diff < 0 {
			diff = -diff
		}
		if diff <= 10 && t.Size == size {
			if t.ProxyWallet != "" && lt.tradeDB != nil {
				profileName := t.Name // Data API returns the actual username
				if profileName == "" && lt.ProfileLookup != nil {
					profileName = lt.ProfileLookup(ctx, t.ProxyWallet)
				}
				if err := lt.tradeDB.EnrichByAssetTimestamp(ctx, assetID, tsSec, t.ProxyWallet, t.Pseudonym, profileName, t.TransactionHash); err != nil {
					lt.logger.Info("enrich WS trade DB update failed", "error", err)
				} else {
					lt.logger.Info("enriched WS trade with wallet info", "asset", assetID[:20], "wallet", t.ProxyWallet, "pseudonym", t.Pseudonym, "profile", profileName)
				}
			}
			return
		}
	}
	lt.logger.Info("enrich WS trade: no match found", "asset", assetID[:20], "tsSec", tsSec)
}
