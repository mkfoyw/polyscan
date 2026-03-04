package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mkfoyw/polyscan/internal/api"
	"github.com/mkfoyw/polyscan/internal/config"
	"github.com/mkfoyw/polyscan/internal/market"
	"github.com/mkfoyw/polyscan/internal/monitor"
	"github.com/mkfoyw/polyscan/internal/notify"
	"github.com/mkfoyw/polyscan/internal/poller"
	"github.com/mkfoyw/polyscan/internal/profile"
	"github.com/mkfoyw/polyscan/internal/smartmoney"
	"github.com/mkfoyw/polyscan/internal/store"
	"github.com/mkfoyw/polyscan/internal/types"
	"github.com/mkfoyw/polyscan/internal/whale"
	"github.com/mkfoyw/polyscan/internal/ws"
)

func main() {
	// Determine config path
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	// Load config
	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	// Set proxy env vars if configured (applies to all net/http + websocket clients)
	if cfg.Proxy != "" {
		for _, env := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} {
			os.Setenv(env, cfg.Proxy)
		}
		logger.Info("proxy configured", "url", cfg.Proxy)
	}

	// Context with signal handling
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Alert channel — shared by all monitors
	alertCh := make(chan types.Alert, 200)

	// --- Initialize SQLite ---
	db, err := store.NewDB(ctx, cfg.SQLitePath, logger)
	if err != nil {
		logger.Error("failed to open SQLite", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.EnsureSchema(); err != nil {
		logger.Error("failed to ensure SQLite schema", "error", err)
		os.Exit(1)
	}
	logger.Info("SQLite ready", "path", cfg.SQLitePath)

	tradeStore := db.Trades()
	alertStore := db.Alerts()
	whaleStore := db.Whales()
	whaleTradStore := db.WhaleTrades()
	profileStore := db.Profiles()
	priceEventStore := db.PriceEvents()
	settlementStore := db.Settlements()

	// --- Initialize components ---

	// 1. Telegram notifier
	tg := notify.NewTelegram(cfg.Telegram.BotToken, cfg.Telegram.ChatID, logger)

	// 2. Market store
	mktStore := types.NewMarketStore()

	// 3. WebSocket client
	wsClient := ws.NewClient(logger)

	// 4. Market discovery
	discovery := market.NewDiscovery(mktStore, settlementStore, cfg.MarketSyncInterval.Duration, cfg.WatchSeries, logger)
	discovery.OnNewMarkets = func(assetIDs []string) {
		logger.Info("subscribing to new markets via WebSocket", "count", len(assetIDs))
		wsClient.Subscribe(assetIDs)
	}

	// 5. Large trade monitor
	largeTradeMon := monitor.NewLargeTrade(cfg.LargeTradeThreshold, cfg.LargeTradeMaxPrice, mktStore, tradeStore, alertCh, nil, logger)

	// 5b. Profile client — resolves wallet addresses to Polymarket usernames
	//     Uses the profiles table as a persistent cache; only calls external API when stale (24h).
	profileClient := profile.NewClient(logger, profileStore, 24*time.Hour)
	largeTradeMon.ProfileLookup = profileClient.Lookup

	// 6. Price spike monitor
	priceSpikeMon := monitor.NewPriceSpike(
		cfg.PriceSpikeRules,
		cfg.Whale.Cooldown.Duration, // reuse cooldown for price alerts too
		mktStore, priceEventStore, alertCh, logger,
	)

	// 7. Trade poller (REST backup)
	tradePoller := poller.NewTradePoller(cfg.LargeTradeThreshold, cfg.TradePollInterval.Duration, logger)
	tradePoller.OnTrade = func(ctx context.Context, trade types.Trade) {
		largeTradeMon.ProcessRESTTrade(ctx, trade)
	}
	tradePoller.OnBackfill = func(ctx context.Context, trade types.Trade) {
		largeTradeMon.BackfillRESTTrade(ctx, trade)
	}

	// 8. Whale tracker
	whaleTracker := whale.NewTracker(cfg.Whale.MaxTracked, whaleStore, logger)
	if err := whaleTracker.Load(ctx); err != nil {
		logger.Error("failed to load whale data from SQLite", "error", err)
	}

	logger.Info("whale tracker initialized", "tracked", whaleTracker.Count())

	// Pre-declare smart money scanner so the callback below can reference it
	var smScanner *smartmoney.Scanner

	// Wire up whale auto-tracking
	{
		minAmount := cfg.Whale.MinTradeAmount
		maxBuyPrice := cfg.Whale.MaxBuyPrice
		largeTradeMon.OnLargeTradeREST = func(proxyWallet string, usdValue float64, price float64, side string, question string) {
			if minAmount > 0 && usdValue < minAmount {
				return
			}
			if maxBuyPrice > 0 && (strings.ToUpper(side) != "BUY" || price > maxBuyPrice) {
				return
			}
			if isNew := whaleTracker.AddAuto(ctx, proxyWallet, usdValue); isNew {
				reason := fmt.Sprintf("$%.0f 大额交易于 %s", usdValue, question)
				logger.Info("auto-tracking new whale", "address", proxyWallet, "reason", reason)
				alert := types.FormatNewWhaleAlert(proxyWallet, reason)
				select {
				case alertCh <- alert:
				default:
				}
			}
		}
	}

	// Wire up smart money candidate discovery (independent, uses its own filters)
	largeTradeMon.OnSmartMoneyCandidate = func(proxyWallet string, usdValue float64, price float64, side string, question string) {
		if smScanner != nil {
			smScanner.AddCandidate(ctx, proxyWallet, usdValue, price, side, question)
		}
	}

	// 9. Whale poller
	whalePoller := poller.NewWhalePoller(whaleTracker, whaleTradStore, cfg.Whale.PollInterval.Duration, cfg.Whale.MinDisplayAmount, alertCh, logger)
	whalePoller.ProfileLookup = profileClient.Lookup

	// 10. Smart money system (optional — controlled by config)
	var smPoller *poller.SmartMoneyPoller
	var smUserStore *store.SmartMoneyUserStore
	var smTradeStore *store.SmartMoneyTradeStore
	var smDB *store.SmartMoneyDB

	if cfg.SmartMoney.Enabled {
		smDB, err = store.NewSmartMoneyDB(ctx, cfg.SmartMoney.SQLitePath, logger)
		if err != nil {
			logger.Error("failed to open SmartMoney SQLite", "error", err)
			os.Exit(1)
		}
		defer smDB.Close()
		if err := smDB.EnsureSchema(); err != nil {
			logger.Error("failed to ensure SmartMoney schema", "error", err)
			os.Exit(1)
		}
		smUserStore = smDB.Users()
		smTradeStore = smDB.Trades()

		smScanner = smartmoney.NewScanner(cfg.SmartMoney, smUserStore, logger)
		smScanner.ProfileLookup = profileClient.Lookup

		smPoller = poller.NewSmartMoneyPoller(
			smUserStore, smTradeStore,
			cfg.SmartMoney.PollInterval.Duration,
			cfg.SmartMoney.MinDisplayAmount,
			alertCh, logger,
		)
		smPoller.ProfileLookup = profileClient.Lookup

		logger.Info("smart money system initialized", "db", cfg.SmartMoney.SQLitePath)
	}

	// --- Start all goroutines ---
	var wg sync.WaitGroup

	start := func(name string, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("starting component", "name", name)
			fn(ctx)
			logger.Info("component stopped", "name", name)
		}()
	}

	// Notifier — persists alerts to SQLite then sends via Telegram
	start("telegram-notifier", func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case alert, ok := <-alertCh:
				if !ok {
					return
				}
				// Persist alert to SQLite
				if alertStore != nil {
					rec := &store.AlertRecord{
						Type:    alert.Type.String(),
						Message: alert.Message,
					}
					if err := alertStore.Insert(ctx, rec); err != nil {
						logger.Error("failed to persist alert", "error", err)
					}
				}
				// Send via Telegram — large trades, whale alerts & smart money
				switch alert.Type {
				case types.AlertLargeTrade, types.AlertWhaleActivity, types.AlertNewWhaleTracked, types.AlertSmartMoney:
					if err := tg.Send(ctx, alert); err != nil {
						logger.Error("failed to send Telegram alert", "error", err)
					}
				default:
					// Other alert types (e.g. PriceSpike) are stored but not sent to Telegram
				}
			}
		}
	})

	// Market discovery (must run first to populate store)
	start("market-discovery", func(ctx context.Context) {
		discovery.Run(ctx)
	})

	// Wait a moment for initial market sync before starting WebSocket
	// We use a simple sleep here; in production you'd use a sync mechanism
	logger.Info("waiting for initial market sync...")
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	waitForMarkets(waitCtx, mktStore)

	// Subscribe to all known asset IDs
	allAssets := mktStore.AllAssetIDs()
	if len(allAssets) > 0 {
		wsClient.Subscribe(allAssets)
		logger.Info("initial WebSocket subscription", "assets", len(allAssets))
	}

	// WebSocket client
	start("websocket", func(ctx context.Context) {
		wsClient.Run(ctx)
	})

	// Large trade WS monitor
	start("large-trade-ws", func(ctx context.Context) {
		largeTradeMon.RunWS(ctx, wsClient.TradeCh)
	})

	// Price spike monitor
	start("price-spike", func(ctx context.Context) {
		priceSpikeMon.Run(ctx, wsClient.BidAskCh)
	})

	// Trade poller (REST backup)
	start("trade-poller", func(ctx context.Context) {
		tradePoller.Run(ctx)
	})

	// Whale poller
	start("whale-poller", func(ctx context.Context) {
		whalePoller.Run(ctx)
	})

	// Smart money scanner + poller (if enabled)
	if smScanner != nil {
		start("smartmoney-scanner", func(ctx context.Context) {
			smScanner.Run(ctx)
		})
	}
	if smPoller != nil {
		start("smartmoney-poller", func(ctx context.Context) {
			smPoller.Run(ctx)
		})
	}

	// Data retention cleanup — prune old records once per day
	ret := cfg.Retention
	if ret.TradesDays > 0 || ret.MarketsDays > 0 || ret.PriceEventsDays > 0 || ret.SmartMoneyDays > 0 {
		start("data-cleanup", func(ctx context.Context) {
			// Run immediately on startup, then every 24h
			ticker := time.NewTicker(24 * time.Hour)
			defer ticker.Stop()
			for {
				logger.Info("running data retention cleanup",
					"trades_days", ret.TradesDays,
					"markets_days", ret.MarketsDays,
					"price_events_days", ret.PriceEventsDays)

				// Trades & Alerts — long retention
				if ret.TradesDays > 0 {
					cutoff := time.Now().AddDate(0, 0, -ret.TradesDays)
					if n, err := tradeStore.DeleteOlderThan(ctx, cutoff); err != nil {
						logger.Error("cleanup trades failed", "error", err)
					} else if n > 0 {
						logger.Info("pruned old trades", "deleted", n)
					}
					if n, err := whaleTradStore.DeleteOlderThan(ctx, cutoff); err != nil {
						logger.Error("cleanup whale_trades failed", "error", err)
					} else if n > 0 {
						logger.Info("pruned old whale trades", "deleted", n)
					}
					if n, err := alertStore.DeleteOlderThan(ctx, cutoff); err != nil {
						logger.Error("cleanup alerts failed", "error", err)
					} else if n > 0 {
						logger.Info("pruned old alerts", "deleted", n)
					}
				}

				// Markets (in-memory) & Settlements (MongoDB) — short retention
				if ret.MarketsDays > 0 {
					cutoff := time.Now().AddDate(0, 0, -ret.MarketsDays)
					if n := mktStore.PruneOlderThan(cutoff); n > 0 {
						logger.Info("pruned old markets (in-memory)", "deleted", n)
					}
					if n, err := settlementStore.DeleteOlderThan(ctx, cutoff); err != nil {
						logger.Error("cleanup settlements failed", "error", err)
					} else if n > 0 {
						logger.Info("pruned old settlements", "deleted", n)
					}
				}

				// Price events
				if ret.PriceEventsDays > 0 {
					cutoff := time.Now().AddDate(0, 0, -ret.PriceEventsDays)
					if n, err := priceEventStore.DeleteOlderThan(ctx, cutoff); err != nil {
						logger.Error("cleanup price_events failed", "error", err)
					} else if n > 0 {
						logger.Info("pruned old price events", "deleted", n)
					}
				}

				// Smart money trades
				if ret.SmartMoneyDays > 0 && smTradeStore != nil {
					cutoff := time.Now().AddDate(0, 0, -ret.SmartMoneyDays)
					if n, err := smTradeStore.DeleteOlderThan(ctx, cutoff); err != nil {
						logger.Error("cleanup smart_money_trades failed", "error", err)
					} else if n > 0 {
						logger.Info("pruned old smart money trades", "deleted", n)
					}
				}

				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		})
	}

	// HTTP API server
	if cfg.API.Enabled {
		broker := api.NewBroker()
		apiServer := api.NewServer(
			cfg.API.Addr,
			cfg.API.AdminTokens,
			mktStore,
			tradeStore,
			alertStore,
			whaleStore,
			whaleTradStore,
			priceEventStore,
			settlementStore,
			smUserStore,
			smTradeStore,
			whaleTracker,
			priceSpikeMon.TopMovers,
			broker,
			logger,
		)
		apiServer.ProfileLookup = profileClient.Lookup
		if smScanner != nil {
			apiServer.SmartMoneyScanner = smScanner
		}

		// Wire SSE events: new trades
		largeTradeMon.OnTradeStored = func(rec store.TradeRecord) {
			apiServer.PublishTrade(rec)
		}

		// Wire SSE events: new settlements
		discovery.OnNewSettlement = func(rec store.SettlementRecord) {
			apiServer.PublishSettlement(rec)
		}

		// Wire SSE events: new markets
		discovery.OnNewMarketInfo = func(m *types.MarketInfo) {
			apiServer.PublishNewMarket(m)
		}

		// Wire SSE events: smart money trades
		if smPoller != nil {
			smPoller.OnTradeStored = func(rec store.SmartMoneyTrade) {
				apiServer.PublishSmartMoneyTrade(rec)
			}
		}

		start("api-server", func(ctx context.Context) {
			apiServer.Run(ctx)
		})
	}

	logger.Info("polyscan started",
		"markets", mktStore.Count(),
		"whales", whaleTracker.Count(),
		"threshold", cfg.LargeTradeThreshold,
		"rules", len(cfg.PriceSpikeRules),
	)

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutting down...")

	// Save whale data before exit (use fresh context since main ctx is canceled)
	saveCtx, saveCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer saveCancel()
	if err := whaleTracker.Save(saveCtx); err != nil {
		logger.Error("failed to save whale data on exit", "error", err)
	}

	// Close alert channel to stop notifier
	close(alertCh)

	wg.Wait()
	logger.Info("polyscan stopped")
}

// waitForMarkets blocks until the market store has at least one market,
// or the context expires.
func waitForMarkets(ctx context.Context, store *types.MarketStore) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if store.Count() > 0 {
				return
			}
		}
	}
}
