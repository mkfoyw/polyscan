package types

// Trade represents a trade from the Data API /trades endpoint.
type Trade struct {
	ProxyWallet           string  `json:"proxyWallet"`
	Side                  string  `json:"side"` // "BUY" or "SELL"
	Asset                 string  `json:"asset"`
	ConditionID           string  `json:"conditionId"`
	Size                  float64 `json:"size"`
	Price                 float64 `json:"price"`
	Timestamp             int64   `json:"timestamp"`
	Title                 string  `json:"title"`
	Slug                  string  `json:"slug"`
	Icon                  string  `json:"icon"`
	EventSlug             string  `json:"eventSlug"`
	Outcome               string  `json:"outcome"`
	OutcomeIndex          int     `json:"outcomeIndex"`
	Name                  string  `json:"name"`
	Pseudonym             string  `json:"pseudonym"`
	TransactionHash       string  `json:"transactionHash"`
}

// USDValue returns the estimated dollar value of this trade.
func (t *Trade) USDValue() float64 {
	return t.Size * t.Price
}

// DisplayName returns the pseudonym or shortened wallet address.
func (t *Trade) DisplayName() string {
	if t.Pseudonym != "" {
		return t.Pseudonym
	}
	if len(t.ProxyWallet) > 10 {
		return t.ProxyWallet[:6] + "..." + t.ProxyWallet[len(t.ProxyWallet)-4:]
	}
	return t.ProxyWallet
}

// Activity represents an activity entry from the Data API /activity endpoint.
type Activity struct {
	ProxyWallet    string  `json:"proxyWallet"`
	Timestamp      int64   `json:"timestamp"`
	ConditionID    string  `json:"conditionId"`
	Type           string  `json:"type"` // "TRADE", "SPLIT", "MERGE", "REDEEM", etc.
	Size           float64 `json:"size"`
	USDCSize       float64 `json:"usdcSize"`
	TransactionHash string `json:"transactionHash"`
	Price          float64 `json:"price"`
	Asset          string  `json:"asset"`
	Side           string  `json:"side"`
	OutcomeIndex   int     `json:"outcomeIndex"`
	Title          string  `json:"title"`
	Slug           string  `json:"slug"`
	Icon           string  `json:"icon"`
	EventSlug      string  `json:"eventSlug"`
	Outcome        string  `json:"outcome"`
	Name           string  `json:"name"`
	Pseudonym      string  `json:"pseudonym"`
}

// DisplayName returns the pseudonym or shortened wallet address.
func (a *Activity) DisplayName() string {
	if a.Pseudonym != "" {
		return a.Pseudonym
	}
	if len(a.ProxyWallet) > 10 {
		return a.ProxyWallet[:6] + "..." + a.ProxyWallet[len(a.ProxyWallet)-4:]
	}
	return a.ProxyWallet
}
