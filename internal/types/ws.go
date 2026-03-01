package types

import "encoding/json"

// WSMessage represents a raw WebSocket message from the Polymarket market channel.
// Use EventType to determine the concrete type, then re-unmarshal.
type WSMessage struct {
	EventType string `json:"event_type"`
	AssetID   string `json:"asset_id"`
	Market    string `json:"market"` // condition ID
	Timestamp string `json:"timestamp"`
}

// WSLastTradePrice is the last_trade_price event from the WebSocket market channel.
type WSLastTradePrice struct {
	EventType  string `json:"event_type"`
	AssetID    string `json:"asset_id"`
	Market     string `json:"market"`
	Price      string `json:"price"`
	Side       string `json:"side"`
	Size       string `json:"size"`
	FeeRateBps string `json:"fee_rate_bps"`
	Timestamp  string `json:"timestamp"`
}

// WSBestBidAsk is the best_bid_ask event from the WebSocket market channel.
type WSBestBidAsk struct {
	EventType string `json:"event_type"`
	AssetID   string `json:"asset_id"`
	Market    string `json:"market"`
	BestBid   string `json:"best_bid"`
	BestAsk   string `json:"best_ask"`
	Spread    string `json:"spread"`
	Timestamp string `json:"timestamp"`
}

// WSPriceChangeItem is a single asset entry in a price_change event.
type WSPriceChangeItem struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"`
	Hash    string `json:"hash"`
	BestBid string `json:"best_bid"`
	BestAsk string `json:"best_ask"`
}

// WSPriceChange is the price_change event from the WebSocket market channel.
type WSPriceChange struct {
	EventType    string               `json:"event_type"`
	Market       string               `json:"market"`
	PriceChanges []WSPriceChangeItem  `json:"price_changes"`
	Timestamp    string               `json:"timestamp"`
}

// WSBookLevel represents a single price level in an order book.
type WSBookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// WSBook is the book (order book snapshot) event from the WebSocket market channel.
type WSBook struct {
	EventType string        `json:"event_type"`
	AssetID   string        `json:"asset_id"`
	Market    string        `json:"market"`
	Bids      []WSBookLevel `json:"bids"`
	Asks      []WSBookLevel `json:"asks"`
	Timestamp string        `json:"timestamp"`
	Hash      string        `json:"hash"`
}

// WSNewMarket is the new_market event (requires custom_feature_enabled).
type WSNewMarket struct {
	EventType string   `json:"event_type"`
	ID        string   `json:"id"`
	Question  string   `json:"question"`
	Slug      string   `json:"slug"`
	AssetsIDs []string `json:"assets_ids"`
	Outcomes  []string `json:"outcomes"`
	Timestamp string   `json:"timestamp"`
}

// WSSubscribeMsg is the initial subscription message sent to the WebSocket.
type WSSubscribeMsg struct {
	AssetsIDs            []string `json:"assets_ids"`
	Type                 string   `json:"type"`
	CustomFeatureEnabled bool     `json:"custom_feature_enabled,omitempty"`
}

// WSDynamicSubMsg is used for dynamic subscribe/unsubscribe operations.
type WSDynamicSubMsg struct {
	AssetsIDs            []string `json:"assets_ids"`
	Operation            string   `json:"operation"` // "subscribe" or "unsubscribe"
	CustomFeatureEnabled bool     `json:"custom_feature_enabled,omitempty"`
}

// ParseWSMessage parses a raw JSON message and returns the event_type.
func ParseWSMessage(data []byte) (string, error) {
	var msg WSMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return "", err
	}
	return msg.EventType, nil
}
