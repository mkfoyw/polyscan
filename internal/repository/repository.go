// Package repository defines persistence interfaces for the polyscan application.
// Concrete implementations (e.g. SQLite) live in sub-packages.
package repository

import (
	"context"
	"time"
)

// ---------- Record types ----------

// AlertRecord is an alert persisted to storage.
type AlertRecord struct {
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// PriceEventRecord is a price spike event.
type PriceEventRecord struct {
	AssetID    string    `json:"asset_id"`
	Market     string    `json:"market"`
	Question   string    `json:"question"`
	Outcome    string    `json:"outcome"`
	Window     string    `json:"window"`
	PctChange  float64   `json:"pct_change"`
	OldPrice   float64   `json:"old_price"`
	NewPrice   float64   `json:"new_price"`
	DetectedAt time.Time `json:"detected_at"`
}

// ProfileRecord is a cached user profile.
type ProfileRecord struct {
	Address   string    `json:"address"`
	Name      string    `json:"name"`
	Pseudonym string    `json:"pseudonym,omitempty"`
	FetchedAt time.Time `json:"fetched_at"`
}

// DisplayName returns the best display name: user-chosen name first, else pseudonym.
func (p *ProfileRecord) DisplayName() string {
	if p.Name != "" {
		return p.Name
	}
	return p.Pseudonym
}

// SettlementRecord is a resolved market.
type SettlementRecord struct {
	ConditionID string    `json:"condition_id"`
	Question    string    `json:"question"`
	Slug        string    `json:"slug"`
	EventSlug   string    `json:"event_slug"`
	Outcome     string    `json:"outcome"`
	ImageURL    string    `json:"image_url"`
	ResolvedAt  time.Time `json:"resolved_at"`
}

// TradeRecord is a trade persisted to storage.
type TradeRecord struct {
	ProxyWallet     string    `json:"proxy_wallet"`
	Pseudonym       string    `json:"pseudonym,omitempty"`
	ProfileName     string    `json:"profile_name,omitempty"`
	Side            string    `json:"side"`
	Asset           string    `json:"asset"`
	ConditionID     string    `json:"condition_id"`
	Size            float64   `json:"size"`
	Price           float64   `json:"price"`
	USDValue        float64   `json:"usd_value"`
	Timestamp       int64     `json:"timestamp"`
	Title           string    `json:"title"`
	Slug            string    `json:"slug"`
	EventSlug       string    `json:"event_slug,omitempty"`
	Outcome         string    `json:"outcome"`
	TransactionHash string    `json:"transaction_hash,omitempty"`
	Source          string    `json:"source"`
	CreatedAt       time.Time `json:"created_at"`
}

// WhaleRecord is a whale user.
type WhaleRecord struct {
	Address          string    `json:"address"`
	Alias            string    `json:"alias,omitempty"`
	Name             string    `json:"name,omitempty"`
	Pseudonym        string    `json:"pseudonym,omitempty"`
	Source           string    `json:"source"`
	FirstSeenAt      time.Time `json:"first_seen_at"`
	TotalVolume      float64   `json:"total_volume"`
	LastPollTS       int64     `json:"last_poll_ts"`
	ProfileFetchedAt time.Time `json:"profile_fetched_at,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// DisplayName returns the best display name: alias > name > pseudonym > address.
func (r *WhaleRecord) DisplayName() string {
	if r.Alias != "" {
		return r.Alias
	}
	if r.Name != "" {
		return r.Name
	}
	if r.Pseudonym != "" {
		return r.Pseudonym
	}
	return r.Address
}

// WhaleTrade is a trade from a tracked whale wallet.
type WhaleTrade struct {
	ProxyWallet     string    `json:"proxy_wallet"`
	ProfileName     string    `json:"profile_name,omitempty"`
	Side            string    `json:"side"`
	Asset           string    `json:"asset"`
	ConditionID     string    `json:"condition_id"`
	Size            float64   `json:"size"`
	Price           float64   `json:"price"`
	USDValue        float64   `json:"usd_value"`
	Timestamp       int64     `json:"timestamp"`
	Title           string    `json:"title"`
	Slug            string    `json:"slug"`
	EventSlug       string    `json:"event_slug,omitempty"`
	Outcome         string    `json:"outcome"`
	TransactionHash string    `json:"transaction_hash,omitempty"`
	Source          string    `json:"source"` // always "whale"
	CreatedAt       time.Time `json:"created_at"`
}

// Smart money user status constants.
const (
	SMStatusCandidate = "candidate"
	SMStatusConfirmed = "confirmed"
	SMStatusRejected  = "rejected"
)

// SmartMoneyUser is a smart money address.
type SmartMoneyUser struct {
	Address          string    `json:"address"`
	Alias            string    `json:"alias,omitempty"`
	Name             string    `json:"name,omitempty"`
	Pseudonym        string    `json:"pseudonym,omitempty"`
	Source           string    `json:"source"`
	Status           string    `json:"status"`
	WinRate          float64   `json:"win_rate"`
	ROI              float64   `json:"roi"`
	TotalTrades      int       `json:"total_trades"`
	WinningTrades    int       `json:"winning_trades"`
	TotalVolume      float64   `json:"total_volume"`
	LastPollTS       int64     `json:"last_poll_ts"`
	ProfileFetchedAt time.Time `json:"profile_fetched_at,omitempty"`
	AnalyzedAt       time.Time `json:"analyzed_at,omitempty"`
	FirstSeenAt      time.Time `json:"first_seen_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// DisplayName returns the best display name for this smart money user.
func (u *SmartMoneyUser) DisplayName() string {
	if u.Alias != "" {
		return u.Alias
	}
	if u.Name != "" {
		return u.Name
	}
	if u.Pseudonym != "" {
		return u.Pseudonym
	}
	return u.Address
}

// SmartMoneyTrade is a trade from a confirmed smart money wallet.
type SmartMoneyTrade struct {
	ProxyWallet     string    `json:"proxy_wallet"`
	ProfileName     string    `json:"profile_name,omitempty"`
	Side            string    `json:"side"`
	Asset           string    `json:"asset"`
	ConditionID     string    `json:"condition_id"`
	Size            float64   `json:"size"`
	Price           float64   `json:"price"`
	USDValue        float64   `json:"usd_value"`
	Timestamp       int64     `json:"timestamp"`
	Title           string    `json:"title"`
	Slug            string    `json:"slug"`
	EventSlug       string    `json:"event_slug,omitempty"`
	Outcome         string    `json:"outcome"`
	TransactionHash string    `json:"transaction_hash,omitempty"`
	Source          string    `json:"source"`
	CreatedAt       time.Time `json:"created_at"`
}

// ---------- Repository interfaces ----------

// AlertRepository persists alert records.
type AlertRepository interface {
	Insert(ctx context.Context, rec *AlertRecord) error
	Recent(ctx context.Context, limit int64) ([]AlertRecord, error)
	RecentByType(ctx context.Context, alertType string, limit int64) ([]AlertRecord, error)
	Count(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// PriceEventRepository persists price spike events.
type PriceEventRepository interface {
	Insert(ctx context.Context, rec *PriceEventRecord) error
	RecentByAsset(ctx context.Context, assetID string, limit int64) ([]PriceEventRecord, error)
	Recent(ctx context.Context, limit int64) ([]PriceEventRecord, error)
	Count(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// ProfileRepository persists cached user profiles.
type ProfileRepository interface {
	Upsert(ctx context.Context, rec *ProfileRecord) error
	Get(ctx context.Context, address string) (*ProfileRecord, error)
	GetMulti(ctx context.Context, addresses []string) (map[string]*ProfileRecord, error)
	Count(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// SettlementRepository persists resolved markets.
type SettlementRepository interface {
	Upsert(ctx context.Context, rec *SettlementRecord) error
	Recent(ctx context.Context, limit int64, before time.Time) ([]SettlementRecord, error)
	SettledConditionIDs(ctx context.Context, ids []string) (map[string]bool, error)
	Count(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// TradeRepository persists trades.
type TradeRepository interface {
	Insert(ctx context.Context, rec *TradeRecord) error
	RecentByWallet(ctx context.Context, wallet string, limit int64) ([]TradeRecord, error)
	RecentLarge(ctx context.Context, minUSD float64, limit int64, beforeTS int64, maxPrice float64, minPrice float64) ([]TradeRecord, error)
	ExistsByTxHash(ctx context.Context, txHash string) (bool, error)
	Recent(ctx context.Context, limit int64, beforeTS int64) ([]TradeRecord, error)
	Count(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
	EnrichByAssetTimestamp(ctx context.Context, asset string, timestamp int64, proxyWallet, pseudonym, profileName, txHash string) error
	UpsertRESTTrade(ctx context.Context, rec *TradeRecord) error
}

// WhaleRepository persists whale user records.
type WhaleRepository interface {
	Upsert(ctx context.Context, rec *WhaleRecord) (bool, error)
	GetAll(ctx context.Context) ([]WhaleRecord, error)
	GetByAddress(ctx context.Context, address string) (*WhaleRecord, error)
	UpdateLastPollTS(ctx context.Context, address string, ts int64) error
	IncrVolume(ctx context.Context, address string, amount float64) error
	UpdateProfile(ctx context.Context, address, name, pseudonym string) error
	Count(ctx context.Context) (int64, error)
	DeleteByAddress(ctx context.Context, address string) error
	DeleteLowestVolume(ctx context.Context) error
	Exists(ctx context.Context, address string) (bool, error)
}

// WhaleTradeRepository persists whale trades.
type WhaleTradeRepository interface {
	Insert(ctx context.Context, rec *WhaleTrade) error
	Recent(ctx context.Context, wallets []string, limit int64, beforeTS, afterTS int64, minUSD float64, maxPrice float64) ([]WhaleTrade, error)
	RecentByWallet(ctx context.Context, wallet string, limit int64) ([]WhaleTrade, error)
	Count(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// SmartMoneyUserRepository persists smart money user records.
type SmartMoneyUserRepository interface {
	Upsert(ctx context.Context, rec *SmartMoneyUser) error
	GetAll(ctx context.Context) ([]SmartMoneyUser, error)
	GetByStatus(ctx context.Context, status string) ([]SmartMoneyUser, error)
	GetByAddress(ctx context.Context, address string) (*SmartMoneyUser, error)
	UpdateAlias(ctx context.Context, address, alias string) error
	UpdateStatus(ctx context.Context, address, status string) error
	UpdateStats(ctx context.Context, address string, winRate, roi float64, totalTrades, winningTrades int, totalVolume float64) error
	UpdateProfile(ctx context.Context, address, name, pseudonym string) error
	UpdateLastPollTS(ctx context.Context, address string, ts int64) error
	IncrVolume(ctx context.Context, address string, amount float64) error
	Delete(ctx context.Context, address string) error
	Exists(ctx context.Context, address string) (bool, error)
	CountByStatus(ctx context.Context, status string) (int64, error)
	GetCandidates(ctx context.Context) ([]SmartMoneyUser, error)
	GetConfirmed(ctx context.Context) ([]SmartMoneyUser, error)
}

// SmartMoneyTradeRepository persists smart money trades.
type SmartMoneyTradeRepository interface {
	Insert(ctx context.Context, rec *SmartMoneyTrade) error
	UpsertMerge(ctx context.Context, rec *SmartMoneyTrade) (*SmartMoneyTrade, error)
	Recent(ctx context.Context, wallets []string, limit int64, beforeTS int64, minUSD, maxPrice float64) ([]SmartMoneyTrade, error)
	RecentByWallet(ctx context.Context, wallet string, limit int64) ([]SmartMoneyTrade, error)
	RecentSince(ctx context.Context, wallets []string, sinceTS int64) ([]SmartMoneyTrade, error)
	Count(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}
