package types

// PriceMover represents a market with a notable price change over a time window.
type PriceMover struct {
	ConditionID   string  `json:"condition_id"`
	Question      string  `json:"question"`
	Outcome       string  `json:"outcome"`
	Slug          string  `json:"slug"`
	ImageURL      string  `json:"image_url"`
	CurrentPrice  float64 `json:"current_price"`  // 0–1 scale
	PreviousPrice float64 `json:"previous_price"` // 0–1 scale
	PriceChange   float64 `json:"price_change"`   // in cents (e.g. -20.5)
	PctChange     float64 `json:"pct_change"`     // percentage
	URL           string  `json:"url"`
}
