package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mkfoyw/polyscan/internal/store"
	"github.com/mkfoyw/polyscan/internal/types"
	"github.com/mkfoyw/polyscan/internal/whale"
)

// Server provides HTTP API endpoints for the web application.
type Server struct {
	mktStore        *types.MarketStore
	tradeStore      *store.TradeStore
	alertStore      *store.AlertStore
	whaleStore      *store.WhaleStore
	whaleTradStore  *store.WhaleTradStore
	priceStore      *store.PriceEventStore
	settlementStore *store.SettlementStore
	smUserStore     *store.SmartMoneyUserStore
	smTradeStore    *store.SmartMoneyTradeStore
	tracker         *whale.Tracker
	adminTokens     map[string]struct{}
	topMoversFn     func(time.Duration, int) []types.PriceMover
	broker          *Broker
	logger          *slog.Logger
	httpServer      *http.Server

	// ProfileLookup resolves a wallet address to a display name (optional).
	ProfileLookup func(ctx context.Context, proxyWallet string) string

	// SmartMoneyScanner provides smart money management methods (optional).
	SmartMoneyScanner interface {
		AddManual(ctx context.Context, address, alias string) error
		Remove(ctx context.Context, address string) error
	}
}

// NewServer creates a new API server.
func NewServer(
	addr string,
	adminTokens []string,
	mktStore *types.MarketStore,
	tradeStore *store.TradeStore,
	alertStore *store.AlertStore,
	whaleStore *store.WhaleStore,
	whaleTradStore *store.WhaleTradStore,
	priceStore *store.PriceEventStore,
	settlementStore *store.SettlementStore,
	smUserStore *store.SmartMoneyUserStore,
	smTradeStore *store.SmartMoneyTradeStore,
	tracker *whale.Tracker,
	topMoversFn func(time.Duration, int) []types.PriceMover,
	broker *Broker,
	logger *slog.Logger,
) *Server {
	s := &Server{
		mktStore:        mktStore,
		tradeStore:      tradeStore,
		alertStore:      alertStore,
		whaleStore:      whaleStore,
		whaleTradStore:  whaleTradStore,
		priceStore:      priceStore,
		settlementStore: settlementStore,
		smUserStore:     smUserStore,
		smTradeStore:    smTradeStore,
		tracker:         tracker,
		adminTokens:     toSet(adminTokens),
		topMoversFn:     topMoversFn,
		broker:          broker,
		logger:          logger,
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(corsMiddleware())

	api := r.Group("/api")
	{
		api.GET("/health", s.handleHealth)
		api.GET("/stats", s.handleStats)
		api.GET("/markets", s.handleMarkets)
		api.GET("/trades", s.handleTrades)
		api.GET("/trades/:wallet", s.handleTradesByWallet)
		api.GET("/whale-trades", s.handleWhaleTrades)
		api.GET("/whales", s.handleWhales)
		api.GET("/whales/:address", s.handleWhaleDetail)
		api.GET("/alerts", s.handleAlerts)
		api.GET("/price-events", s.handlePriceEvents)
		api.GET("/price-moves", s.handlePriceMoves)
		api.GET("/new-markets", s.handleNewMarkets)
		api.GET("/settlements", s.handleSettlements)
		api.GET("/events", s.handleSSE) // SSE real-time push

		// Smart money endpoints
		api.GET("/smart-money", s.handleSmartMoney)
		api.GET("/smart-money-trades", s.handleSmartMoneyTrades)

		// Admin endpoints (require admin_token)
		admin := api.Group("", s.adminAuth())
		admin.POST("/whales", s.handleAddWhale)
		admin.DELETE("/whales/:address", s.handleDeleteWhale)
		admin.POST("/smart-money", s.handleAddSmartMoney)
		admin.DELETE("/smart-money/:address", s.handleDeleteSmartMoney)
		admin.PUT("/smart-money/:address/status", s.handleUpdateSmartMoneyStatus)
		admin.PATCH("/smart-money/:address", s.handleUpdateSmartMoneyAlias)
	}

	// Serve frontend pages
	r.GET("/", func(c *gin.Context) { c.File("web/index.html") })
	r.GET("/whales", func(c *gin.Context) { c.File("web/whales.html") })
	r.GET("/smartmoney", func(c *gin.Context) { c.File("web/smartmoney.html") })

	s.httpServer = &http.Server{
		Addr:        addr,
		Handler:     r,
		ReadTimeout: 10 * time.Second,
		// No WriteTimeout — SSE connections are long-lived
	}

	return s
}

// Run starts the HTTP server. Blocks until the server is shut down.
func (s *Server) Run(ctx context.Context) {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("API server shutdown error", "error", err)
		}
	}()

	s.logger.Info("API server starting", "addr", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.logger.Error("API server error", "error", err)
	}
}

// --- Handlers ---

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) handleStats(c *gin.Context) {
	ctx := c.Request.Context()
	tradeCount, _ := s.tradeStore.Count(ctx)
	alertCount, _ := s.alertStore.Count(ctx)
	whaleCount, _ := s.whaleStore.Count(ctx)
	whaleTradeCount, _ := s.whaleTradStore.Count(ctx)
	priceEventCount, _ := s.priceStore.Count(ctx)

	c.JSON(http.StatusOK, gin.H{
		"markets":      s.mktStore.Count(),
		"trades":       tradeCount,
		"alerts":       alertCount,
		"whales":       whaleCount,
		"whale_trades": whaleTradeCount,
		"price_events": priceEventCount,
	})
}

func (s *Server) handleMarkets(c *gin.Context) {
	markets := s.mktStore.AllMarkets()

	type marketResp struct {
		Question      string  `json:"question"`
		ConditionID   string  `json:"condition_id"`
		Slug          string  `json:"slug"`
		Volume        float64 `json:"volume"`
		Active        bool    `json:"active"`
		URL           string  `json:"url"`
		YesTokenID    string  `json:"yes_token_id,omitempty"`
		NoTokenID     string  `json:"no_token_id,omitempty"`
		OutcomePrices string  `json:"outcome_prices,omitempty"`
	}

	result := make([]marketResp, 0, len(markets))
	for _, m := range markets {
		result = append(result, marketResp{
			Question:      m.Question,
			ConditionID:   m.ConditionID,
			Slug:          m.Slug,
			Volume:        m.VolumeNum,
			Active:        m.Active,
			URL:           m.PolymarketURL(),
			YesTokenID:    m.YesTokenID,
			NoTokenID:     m.NoTokenID,
			OutcomePrices: m.OutcomePrices,
		})
	}
	c.JSON(http.StatusOK, result)
}

func (s *Server) handleTrades(c *gin.Context) {
	ctx := c.Request.Context()
	limit := queryInt(c, "limit", 50)
	minUSD := queryFloat(c, "min_usd", 0)
	maxPrice := queryFloat(c, "max_price", 0) // e.g. 0.3 for ≤30¢
	minPrice := queryFloat(c, "min_price", 0) // e.g. 0.1 for ≥10¢
	beforeTS := queryInt64(c, "before", 0)     // cursor: timestamp in seconds

	var trades []store.TradeRecord
	var err error
	if minUSD > 0 || maxPrice > 0 || minPrice > 0 {
		if minUSD <= 0 {
			minUSD = 0.01 // effectively no USD filter
		}
		trades, err = s.tradeStore.RecentLarge(ctx, minUSD, int64(limit), beforeTS, maxPrice, minPrice)
	} else {
		trades, err = s.tradeStore.Recent(ctx, int64(limit), beforeTS)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, trades)
}

func (s *Server) handleTradesByWallet(c *gin.Context) {
	ctx := c.Request.Context()
	wallet := c.Param("wallet")
	limit := queryInt(c, "limit", 50)

	trades, err := s.tradeStore.RecentByWallet(ctx, wallet, int64(limit))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, trades)
}

func (s *Server) handleWhales(c *gin.Context) {
	ctx := c.Request.Context()
	whales, err := s.whaleStore.GetAll(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Build response with display_name resolved from whale_users table directly
	type whaleResp struct {
		store.WhaleRecord
		DisplayName string `json:"display_name,omitempty"`
	}

	resp := make([]whaleResp, len(whales))
	for i, w := range whales {
		resp[i] = whaleResp{
			WhaleRecord: w,
			DisplayName: w.DisplayName(),
		}
	}

	c.JSON(http.StatusOK, resp)
}

func (s *Server) handleWhaleTrades(c *gin.Context) {
	ctx := c.Request.Context()
	limit := queryInt(c, "limit", 50)
	beforeTS := queryInt64(c, "before", 0)
	afterTS := queryInt64(c, "after", 0)
	minUSD := queryFloat(c, "min_usd", 0)
	maxPrice := queryFloat(c, "max_price", 0)
	walletFilter := strings.ToLower(strings.TrimSpace(c.Query("wallet")))

	// Get all tracked whale addresses
	whales, err := s.whaleStore.GetAll(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(whales) == 0 {
		c.JSON(http.StatusOK, []store.WhaleTrade{})
		return
	}

	// Build name + alias maps from whale_users table
	type whaleNames struct {
		Name      string // Polymarket profile name
		Pseudonym string
		Alias     string // user-set alias
	}
	nameMap := make(map[string]whaleNames, len(whales))
	var addrs []string
	for _, w := range whales {
		nameMap[w.Address] = whaleNames{Name: w.Name, Pseudonym: w.Pseudonym, Alias: w.Alias}
		// If wallet filter specified, only query that one address
		if walletFilter != "" {
			if strings.EqualFold(w.Address, walletFilter) {
				addrs = []string{w.Address}
			}
		} else {
			addrs = append(addrs, w.Address)
		}
	}
	if len(addrs) == 0 {
		c.JSON(http.StatusOK, []store.WhaleTrade{})
		return
	}

	trades, err := s.whaleTradStore.Recent(ctx, addrs, int64(limit), beforeTS, afterTS, minUSD, maxPrice)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Enrich trades with display names: prioritize Polymarket name, append alias in parentheses
	for i := range trades {
		if wn, ok := nameMap[trades[i].ProxyWallet]; ok {
			if dn := resolveDisplayName(wn.Name, wn.Pseudonym, wn.Alias); dn != "" {
				trades[i].ProfileName = dn
			}
		}
	}

	c.JSON(http.StatusOK, trades)
}

// toSet converts a string slice to a set for O(1) lookups.
func toSet(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		if s != "" {
			m[s] = struct{}{}
		}
	}
	return m
}

// resolveDisplayName picks the best human-readable name from name/pseudonym/alias.
// Returns empty string if nothing useful is available.
func resolveDisplayName(name, pseudonym, alias string) string {
	dn := name
	if dn == "" {
		dn = pseudonym
	}
	// Truncate address-like names (e.g. "0xDd9D902e3C...-1772597907584" → "0xDd9D...1b730")
	if strings.HasPrefix(strings.ToLower(dn), "0x") {
		// Extract just the address part (first 42 chars or up to a separator)
		addr := dn
		if idx := strings.IndexByte(addr, '-'); idx > 0 {
			addr = addr[:idx]
		}
		if len(addr) > 10 {
			dn = addr[:6] + "…" + addr[len(addr)-4:]
		}
	}
	if dn != "" {
		if alias != "" {
			return dn + " (" + alias + ")"
		}
		return dn
	}
	if alias != "" {
		return alias
	}
	return ""
}

// adminAuth returns middleware that checks the admin token.
func (s *Server) adminAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(s.adminTokens) == 0 {
			c.JSON(http.StatusForbidden, gin.H{"error": "admin API disabled (no admin_tokens configured)"})
			c.Abort()
			return
		}
		token := c.GetHeader("Authorization")
		token = strings.TrimPrefix(token, "Bearer ")
		if token == "" {
			token = c.Query("token")
		}
		if _, ok := s.adminTokens[token]; !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing token"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// handleAddWhale adds a whale address to tracking.
func (s *Server) handleAddWhale(c *gin.Context) {
	ctx := c.Request.Context()
	var req struct {
		Address string `json:"address" binding:"required"`
		Alias   string `json:"alias"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address is required"})
		return
	}

	req.Address = strings.ToLower(strings.TrimSpace(req.Address))
	if !strings.HasPrefix(req.Address, "0x") || len(req.Address) != 42 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Ethereum address"})
		return
	}

	s.tracker.AddManual(ctx, req.Address, req.Alias)
	s.logger.Info("whale added via API", "address", req.Address, "alias", req.Alias)

	// Resolve profile name asynchronously if no alias was provided
	if req.Alias == "" && s.ProfileLookup != nil {
		go func(addr string) {
			lookupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if name := s.ProfileLookup(lookupCtx, addr); name != "" {
				s.tracker.UpdateProfile(lookupCtx, addr, name, "")
				s.logger.Info("whale profile resolved", "address", addr, "name", name)
			}
		}(req.Address)
	}

	c.JSON(http.StatusOK, gin.H{"ok": true, "address": req.Address, "alias": req.Alias})
}

// handleDeleteWhale removes a whale from tracking.
func (s *Server) handleDeleteWhale(c *gin.Context) {
	ctx := c.Request.Context()
	address := strings.ToLower(strings.TrimSpace(c.Param("address")))

	s.tracker.Remove(ctx, address)
	s.logger.Info("whale removed via API", "address", address)
	c.JSON(http.StatusOK, gin.H{"ok": true, "address": address})
}

func (s *Server) handleWhaleDetail(c *gin.Context) {
	ctx := c.Request.Context()
	address := c.Param("address")

	whaleRec, err := s.whaleStore.GetByAddress(ctx, address)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if whaleRec == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "whale not found"})
		return
	}

	// Fetch recent trades from whale_trades table
	limit := queryInt(c, "limit", 20)
	trades, err := s.whaleTradStore.RecentByWallet(ctx, address, int64(limit))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Enrich trades with display name from whale_users
	dn := whaleRec.DisplayName()
	if dn != whaleRec.Address {
		for i := range trades {
			trades[i].ProfileName = dn
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"whale":  whaleRec,
		"trades": trades,
	})
}

func (s *Server) handleAlerts(c *gin.Context) {
	ctx := c.Request.Context()
	limit := queryInt(c, "limit", 50)
	alertType := c.Query("type")

	var alerts []store.AlertRecord
	var err error
	if alertType != "" {
		alerts, err = s.alertStore.RecentByType(ctx, alertType, int64(limit))
	} else {
		alerts, err = s.alertStore.Recent(ctx, int64(limit))
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, alerts)
}

func (s *Server) handlePriceEvents(c *gin.Context) {
	ctx := c.Request.Context()
	limit := queryInt(c, "limit", 50)
	assetID := c.Query("asset_id")

	var events []store.PriceEventRecord
	var err error
	if assetID != "" {
		events, err = s.priceStore.RecentByAsset(ctx, assetID, int64(limit))
	} else {
		events, err = s.priceStore.Recent(ctx, int64(limit))
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, events)
}

// --- Helpers ---

func queryInt(c *gin.Context, key string, defaultVal int) int {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return defaultVal
	}
	if v > 500 {
		return 500
	}
	return v
}

func queryInt64(c *gin.Context, key string, defaultVal int64) int64 {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return defaultVal
	}
	return v
}

func queryFloat(c *gin.Context, key string, defaultVal float64) float64 {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		return defaultVal
	}
	return v
}

// --- Dashboard Handlers ---

func (s *Server) handlePriceMoves(c *gin.Context) {
	windowStr := c.DefaultQuery("window", "5m")
	limit := queryInt(c, "limit", 20)

	window, err := time.ParseDuration(windowStr)
	if err != nil {
		window = 5 * time.Minute
	}

	// Try in-memory real-time data first
	var movers []types.PriceMover
	if s.topMoversFn != nil {
		movers = s.topMoversFn(window, limit)
	}

	// Fallback: if no in-memory movers, use recent price events from MongoDB
	if len(movers) == 0 && s.priceStore != nil {
		ctx := c.Request.Context()
		events, err := s.priceStore.Recent(ctx, int64(limit))
		if err == nil && len(events) > 0 {
			movers = make([]types.PriceMover, 0, len(events))
			for _, e := range events {
				priceChange := (e.NewPrice - e.OldPrice) * 100.0 // cents
				// Try to get market info for URL and image
				var url, imageURL, slug string
				if s.mktStore != nil {
					if m := s.mktStore.GetByCondition(e.Market); m != nil {
						url = m.PolymarketURL()
						imageURL = m.Image
						slug = m.Slug
					}
				}
				movers = append(movers, types.PriceMover{
					ConditionID:   e.Market,
					Question:      e.Question,
					Outcome:       e.Outcome,
					Slug:          slug,
					ImageURL:      imageURL,
					CurrentPrice:  e.NewPrice,
					PreviousPrice: e.OldPrice,
					PriceChange:   priceChange,
					PctChange:     e.PctChange,
					URL:           url,
				})
			}
		}
	}

	c.JSON(http.StatusOK, movers)
}

func (s *Server) handleNewMarkets(c *gin.Context) {
	category := c.DefaultQuery("category", "")
	limit := queryInt(c, "limit", 20)

	var beforeTime time.Time
	if bs := c.Query("before"); bs != "" {
		if t, err := time.Parse(time.RFC3339, bs); err == nil {
			beforeTime = t
		}
	}

	var markets []*types.MarketInfo
	if category != "" && category != "all" {
		markets = s.mktStore.RecentNewByCategory(category, limit, beforeTime)
	} else {
		markets = s.mktStore.RecentNew(limit, beforeTime)
	}

	type resp struct {
		Question  string    `json:"question"`
		Slug      string    `json:"slug"`
		EventSlug string    `json:"event_slug"`
		Category  string    `json:"category"`
		ImageURL  string    `json:"image_url"`
		URL       string    `json:"url"`
		FirstSeen time.Time `json:"first_seen"`
	}

	result := make([]resp, 0, len(markets))
	for _, m := range markets {
		result = append(result, resp{
			Question:  m.Question,
			Slug:      m.Slug,
			EventSlug: m.EventSlug,
			Category:  m.Category,
			ImageURL:  m.Image,
			URL:       m.PolymarketURL(),
			FirstSeen: m.FirstSeenAt,
		})
	}
	c.JSON(http.StatusOK, result)
}

func (s *Server) handleSettlements(c *gin.Context) {
	ctx := c.Request.Context()
	limit := queryInt(c, "limit", 20)

	if s.settlementStore == nil {
		c.JSON(http.StatusOK, []store.SettlementRecord{})
		return
	}

	var before time.Time
	if bs := c.Query("before"); bs != "" {
		if t, err := time.Parse(time.RFC3339, bs); err == nil {
			before = t
		}
	}

	records, err := s.settlementStore.Recent(ctx, int64(limit), before)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, records)
}

// --- Smart Money Handlers ---

func (s *Server) handleSmartMoney(c *gin.Context) {
	if s.smUserStore == nil {
		c.JSON(http.StatusOK, []store.SmartMoneyUser{})
		return
	}
	ctx := c.Request.Context()
	status := c.Query("status") // filter by status: candidate, confirmed, rejected

	var users []store.SmartMoneyUser
	var err error
	if status != "" {
		users, err = s.smUserStore.GetByStatus(ctx, status)
	} else {
		users, err = s.smUserStore.GetAll(ctx)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	type smResp struct {
		store.SmartMoneyUser
		DisplayName string `json:"display_name,omitempty"`
	}
	resp := make([]smResp, len(users))
	for i, u := range users {
		resp[i] = smResp{SmartMoneyUser: u, DisplayName: u.DisplayName()}
	}

	c.JSON(http.StatusOK, resp)
}

func (s *Server) handleSmartMoneyTrades(c *gin.Context) {
	if s.smUserStore == nil || s.smTradeStore == nil {
		c.JSON(http.StatusOK, []store.SmartMoneyTrade{})
		return
	}
	ctx := c.Request.Context()
	limit := queryInt(c, "limit", 50)
	beforeTS := queryInt64(c, "before", 0)
	minUSD := queryFloat(c, "min_usd", 0)
	maxPrice := queryFloat(c, "max_price", 0)
	walletFilter := strings.ToLower(strings.TrimSpace(c.Query("wallet")))

	// Get all confirmed smart money addresses
	confirmed, err := s.smUserStore.GetConfirmed(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(confirmed) == 0 {
		c.JSON(http.StatusOK, []store.SmartMoneyTrade{})
		return
	}

	// Build name map
	type smNames struct {
		Name      string
		Pseudonym string
		Alias     string
	}
	nameMap := make(map[string]smNames, len(confirmed))
	var addrs []string
	for _, u := range confirmed {
		nameMap[u.Address] = smNames{Name: u.Name, Pseudonym: u.Pseudonym, Alias: u.Alias}
		if walletFilter != "" {
			if strings.EqualFold(u.Address, walletFilter) {
				addrs = []string{u.Address}
			}
		} else {
			addrs = append(addrs, u.Address)
		}
	}
	if len(addrs) == 0 {
		c.JSON(http.StatusOK, []store.SmartMoneyTrade{})
		return
	}

	trades, err := s.smTradeStore.Recent(ctx, addrs, int64(limit), beforeTS, minUSD, maxPrice)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Enrich with display names
	for i := range trades {
		if sn, ok := nameMap[trades[i].ProxyWallet]; ok {
			if dn := resolveDisplayName(sn.Name, sn.Pseudonym, sn.Alias); dn != "" {
				trades[i].ProfileName = dn
			}
		}
	}

	c.JSON(http.StatusOK, trades)
}

func (s *Server) handleAddSmartMoney(c *gin.Context) {
	if s.SmartMoneyScanner == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "smart money not enabled"})
		return
	}
	ctx := c.Request.Context()
	var req struct {
		Address string `json:"address" binding:"required"`
		Alias   string `json:"alias"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address is required"})
		return
	}
	req.Address = strings.ToLower(strings.TrimSpace(req.Address))
	if !strings.HasPrefix(req.Address, "0x") || len(req.Address) != 42 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Ethereum address"})
		return
	}
	if err := s.SmartMoneyScanner.AddManual(ctx, req.Address, req.Alias); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.logger.Info("smart money added via API", "address", req.Address, "alias", req.Alias)
	c.JSON(http.StatusOK, gin.H{"ok": true, "address": req.Address, "alias": req.Alias})
}

func (s *Server) handleDeleteSmartMoney(c *gin.Context) {
	if s.SmartMoneyScanner == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "smart money not enabled"})
		return
	}
	ctx := c.Request.Context()
	address := strings.ToLower(strings.TrimSpace(c.Param("address")))
	if err := s.SmartMoneyScanner.Remove(ctx, address); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.logger.Info("smart money removed via API", "address", address)
	c.JSON(http.StatusOK, gin.H{"ok": true, "address": address})
}

func (s *Server) handleUpdateSmartMoneyStatus(c *gin.Context) {
	if s.smUserStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "smart money not enabled"})
		return
	}
	ctx := c.Request.Context()
	address := strings.ToLower(strings.TrimSpace(c.Param("address")))
	var req struct {
		Status string `json:"status" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "status is required"})
		return
	}
	if req.Status != store.SMStatusCandidate && req.Status != store.SMStatusConfirmed && req.Status != store.SMStatusRejected {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status, must be candidate/confirmed/rejected"})
		return
	}
	if err := s.smUserStore.UpdateStatus(ctx, address, req.Status); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.logger.Info("smart money status updated", "address", address, "status", req.Status)
	c.JSON(http.StatusOK, gin.H{"ok": true, "address": address, "status": req.Status})
}

func (s *Server) handleUpdateSmartMoneyAlias(c *gin.Context) {
	if s.smUserStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "smart money not enabled"})
		return
	}
	ctx := c.Request.Context()
	address := strings.ToLower(strings.TrimSpace(c.Param("address")))
	var req struct {
		Alias string `json:"alias"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	req.Alias = strings.TrimSpace(req.Alias)
	if err := s.smUserStore.UpdateAlias(ctx, address, req.Alias); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.logger.Info("smart money alias updated", "address", address, "alias", req.Alias)
	c.JSON(http.StatusOK, gin.H{"ok": true, "address": address, "alias": req.Alias})
}

// PublishSmartMoneyTrade sends a new smart money trade to SSE clients.
func (s *Server) PublishSmartMoneyTrade(t store.SmartMoneyTrade) {
	// Enrich with display name if not already set
	if t.ProfileName == "" && t.ProxyWallet != "" && s.smUserStore != nil {
		if u, err := s.smUserStore.GetByAddress(context.Background(), t.ProxyWallet); err == nil && u != nil {
			if dn := resolveDisplayName(u.Name, u.Pseudonym, u.Alias); dn != "" {
				t.ProfileName = dn
			}
		}
	}
	s.PublishEvent("smart_money_trade", t)
}

// corsMiddleware returns a Gin middleware that adds CORS headers.
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Header("Cache-Control", "no-store")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// handleSSE streams real-time dashboard events via Server-Sent Events.
func (s *Server) handleSSE(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")

	ch := s.broker.Subscribe()
	defer s.broker.Unsubscribe(ch)

	ctx := c.Request.Context()
	c.Stream(func(w io.Writer) bool {
		select {
		case <-ctx.Done():
			return false
		case data, ok := <-ch:
			if !ok {
				return false
			}
			c.SSEvent("message", string(data))
			return true
		}
	})
}

// Broker returns the server's SSE event broker (used by main.go to publish events).
func (s *Server) BrokerRef() *Broker {
	return s.broker
}

// PublishEvent publishes a real-time event to all connected SSE clients.
func (s *Server) PublishEvent(eventType string, data interface{}) {
	if s.broker == nil {
		return
	}
	s.broker.Publish(DashEvent{
		Type: eventType,
		Data: data,
	})
}

// NewMarketResp is the response format for new markets (used by SSE and REST).
type NewMarketResp struct {
	Question  string    `json:"question"`
	Slug      string    `json:"slug"`
	EventSlug string    `json:"event_slug"`
	Category  string    `json:"category"`
	ImageURL  string    `json:"image_url"`
	URL       string    `json:"url"`
	FirstSeen time.Time `json:"first_seen"`
}

// FormatMarketResp converts a MarketInfo to the API response format.
func FormatMarketResp(m *types.MarketInfo) NewMarketResp {
	return NewMarketResp{
		Question:  m.Question,
		Slug:      m.Slug,
		EventSlug: m.EventSlug,
		Category:  m.Category,
		ImageURL:  m.Image,
		URL:       m.PolymarketURL(),
		FirstSeen: m.FirstSeenAt,
	}
}

// PublishTrade sends a new trade to SSE clients.
func (s *Server) PublishTrade(t store.TradeRecord) {
	s.PublishEvent("trade", t)
}

// PublishSettlement sends a new settlement to SSE clients.
func (s *Server) PublishSettlement(r store.SettlementRecord) {
	s.PublishEvent("settlement", r)
}

// PublishNewMarket sends a new market to SSE clients.
func (s *Server) PublishNewMarket(m *types.MarketInfo) {
	s.PublishEvent("new_market", FormatMarketResp(m))
}

// PublishPriceMove sends a price move event to SSE clients.
func (s *Server) PublishPriceMove(pm types.PriceMover) {
	s.PublishEvent("price_move", pm)
}


