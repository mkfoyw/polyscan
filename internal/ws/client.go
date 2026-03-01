package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mkfoyw/polyscan/internal/types"
	"nhooyr.io/websocket"
)

const (
	wsEndpoint    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	pingInterval  = 10 * time.Second
	maxBatchSize  = 500 // max asset IDs per subscribe message
)

// Client manages the WebSocket connection to the Polymarket market channel.
type Client struct {
	logger *slog.Logger

	// Output channels for dispatched events
	TradeCh    chan types.WSLastTradePrice
	BidAskCh   chan types.WSBestBidAsk

	mu          sync.Mutex
	conn        *websocket.Conn
	subscribedIDs map[string]struct{}
}

// NewClient creates a new WebSocket client.
func NewClient(logger *slog.Logger) *Client {
	return &Client{
		logger:        logger,
		TradeCh:       make(chan types.WSLastTradePrice, 1000),
		BidAskCh:      make(chan types.WSBestBidAsk, 1000),
		subscribedIDs: make(map[string]struct{}),
	}
}

// Run connects to the WebSocket, sends heartbeats, and dispatches messages.
// It automatically reconnects on failures. Blocks until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	backoff := 5 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		if err := c.connectAndListen(ctx); err != nil {
			if ctx.Err() != nil {
				c.logger.Info("websocket shutting down")
				return
			}
			c.logger.Error("websocket disconnected, reconnecting...", "error", err, "backoff", backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Exponential backoff with cap
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// Subscribe adds new asset IDs to the subscription.
// Can be called while connected — sends a dynamic subscribe message.
func (c *Client) Subscribe(assetIDs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var newIDs []string
	for _, id := range assetIDs {
		if _, ok := c.subscribedIDs[id]; !ok {
			c.subscribedIDs[id] = struct{}{}
			newIDs = append(newIDs, id)
		}
	}

	if len(newIDs) == 0 {
		return
	}

	if c.conn != nil {
		c.sendDynamicSubscribe(newIDs)
	}
}

// SubscribedCount returns the number of currently subscribed asset IDs.
func (c *Client) SubscribedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subscribedIDs)
}

func (c *Client) connectAndListen(ctx context.Context) error {
	// Don't connect until we have at least one asset ID to subscribe to
	c.mu.Lock()
	hasIDs := len(c.subscribedIDs) > 0
	c.mu.Unlock()
	if !hasIDs {
		c.logger.Info("no asset IDs yet, waiting for market discovery before connecting...")
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				c.mu.Lock()
				hasIDs = len(c.subscribedIDs) > 0
				c.mu.Unlock()
				if hasIDs {
					goto connect
				}
			}
		}
	}
connect:
	c.logger.Info("connecting to Polymarket WebSocket", "url", wsEndpoint)

	conn, _, err := websocket.Dial(ctx, wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	// Allow large messages (e.g. full book snapshots)
	conn.SetReadLimit(10 * 1024 * 1024)

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		conn.CloseNow()
	}()

	// Send initial subscription for all tracked IDs
	c.mu.Lock()
	allIDs := make([]string, 0, len(c.subscribedIDs))
	for id := range c.subscribedIDs {
		allIDs = append(allIDs, id)
	}
	c.mu.Unlock()

	if len(allIDs) > 0 {
		if err := c.sendInitialSubscribe(ctx, conn, allIDs); err != nil {
			return fmt.Errorf("initial subscribe: %w", err)
		}
		c.logger.Info("subscribed to assets", "count", len(allIDs))
	} else {
		c.logger.Warn("no asset IDs to subscribe to yet, waiting for market discovery...")
	}

	// Start ping goroutine
	pingCtx, pingCancel := context.WithCancel(ctx)
	defer pingCancel()
	go c.pingLoop(pingCtx, conn)

	// Read messages
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		// Ignore PONG responses
		if string(data) == "PONG" {
			continue
		}

		c.dispatch(data)
	}
}

func (c *Client) sendInitialSubscribe(ctx context.Context, conn *websocket.Conn, assetIDs []string) error {
	// Split into batches
	for i := 0; i < len(assetIDs); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(assetIDs) {
			end = len(assetIDs)
		}
		batch := assetIDs[i:end]

		msg := types.WSSubscribeMsg{
			AssetsIDs:            batch,
			Type:                 "market",
			CustomFeatureEnabled: true,
		}
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) sendDynamicSubscribe(assetIDs []string) {
	if c.conn == nil {
		return
	}

	for i := 0; i < len(assetIDs); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(assetIDs) {
			end = len(assetIDs)
		}
		batch := assetIDs[i:end]

		msg := types.WSDynamicSubMsg{
			AssetsIDs:            batch,
			Operation:            "subscribe",
			CustomFeatureEnabled: true,
		}
		data, err := json.Marshal(msg)
		if err != nil {
			c.logger.Error("marshal dynamic subscribe", "error", err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.conn.Write(ctx, websocket.MessageText, data); err != nil {
			c.logger.Error("send dynamic subscribe", "error", err)
		}
		cancel()
	}

	c.logger.Info("dynamically subscribed to new assets", "count", len(assetIDs))
}

func (c *Client) pingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Write(writeCtx, websocket.MessageText, []byte("PING"))
			cancel()
			if err != nil {
				c.logger.Warn("ping failed", "error", err)
				return
			}
		}
	}
}

func (c *Client) dispatch(data []byte) {
	eventType, err := types.ParseWSMessage(data)
	if err != nil {
		c.logger.Debug("failed to parse ws message", "error", err, "data", string(data))
		return
	}

	switch eventType {
	case "last_trade_price":
		var msg types.WSLastTradePrice
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logger.Warn("failed to unmarshal last_trade_price", "error", err)
			return
		}
		select {
		case c.TradeCh <- msg:
		default:
			c.logger.Warn("trade channel full, dropping message")
		}

	case "best_bid_ask":
		var msg types.WSBestBidAsk
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logger.Warn("failed to unmarshal best_bid_ask", "error", err)
			return
		}
		select {
		case c.BidAskCh <- msg:
		default:
			c.logger.Warn("bid_ask channel full, dropping message")
		}

	case "price_change":
		// Extract best_bid/best_ask from price_change as a fallback
		var msg types.WSPriceChange
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		for _, pc := range msg.PriceChanges {
			if pc.BestBid == "" || pc.BestAsk == "" {
				continue
			}
			bidAsk := types.WSBestBidAsk{
				EventType: "best_bid_ask",
				AssetID:   pc.AssetID,
				Market:    msg.Market,
				BestBid:   pc.BestBid,
				BestAsk:   pc.BestAsk,
				Timestamp: msg.Timestamp,
			}
			select {
			case c.BidAskCh <- bidAsk:
			default:
			}
		}

	case "book", "tick_size_change", "new_market", "market_resolved":
		// Logged but not processed
		c.logger.Debug("received ws event", "type", eventType)

	default:
		c.logger.Debug("unknown ws event", "type", eventType, "data", string(data))
	}
}
