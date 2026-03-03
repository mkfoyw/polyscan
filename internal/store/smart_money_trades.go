package store

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// SmartMoneyTrade is a trade from a confirmed smart money wallet.
type SmartMoneyTrade struct {
	ProxyWallet     string    `json:"proxy_wallet"`
	ProfileName     string    `json:"profile_name,omitempty"` // populated at query time
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
	Source          string    `json:"source"` // always "smartmoney"
	CreatedAt       time.Time `json:"created_at"`
}

// SmartMoneyTradeStore wraps the smart_money_trades table.
type SmartMoneyTradeStore struct {
	rdb *sql.DB
	wdb *sql.DB
}

const smTradeDBCols = `proxy_wallet, side, asset, condition_id,
	size, price, usd_value, timestamp, title, slug, event_slug, outcome,
	transaction_hash, created_at`

func scanSMTrade(sc interface{ Scan(...any) error }) (SmartMoneyTrade, error) {
	var r SmartMoneyTrade
	var createdUnix int64
	err := sc.Scan(
		&r.ProxyWallet, &r.Side, &r.Asset, &r.ConditionID,
		&r.Size, &r.Price, &r.USDValue, &r.Timestamp,
		&r.Title, &r.Slug, &r.EventSlug, &r.Outcome,
		&r.TransactionHash, &createdUnix,
	)
	if err != nil {
		return r, err
	}
	r.CreatedAt = time.Unix(createdUnix, 0)
	r.Source = "smartmoney"
	return r, nil
}

func scanSMTrades(rows *sql.Rows) ([]SmartMoneyTrade, error) {
	defer rows.Close()
	var out []SmartMoneyTrade
	for rows.Next() {
		r, err := scanSMTrade(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Insert inserts a smart money trade. Duplicates (same transaction_hash) are silently ignored.
func (s *SmartMoneyTradeStore) Insert(ctx context.Context, rec *SmartMoneyTrade) error {
	rec.CreatedAt = time.Now()
	_, err := s.wdb.ExecContext(ctx, `
		INSERT INTO smart_money_trades (`+smTradeDBCols+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ProxyWallet, rec.Side, rec.Asset, rec.ConditionID,
		rec.Size, rec.Price, rec.USDValue, rec.Timestamp,
		rec.Title, rec.Slug, rec.EventSlug, rec.Outcome,
		rec.TransactionHash, rec.CreatedAt.Unix(),
	)
	if err != nil && isUniqueViolation(err) {
		return nil
	}
	return err
}

// Recent returns smart money trades, most recent first.
// Only returns trades from confirmed wallets.
func (s *SmartMoneyTradeStore) Recent(ctx context.Context, wallets []string, limit int64, beforeTS int64, minUSD, maxPrice float64) ([]SmartMoneyTrade, error) {
	q := `SELECT ` + smTradeDBCols + ` FROM smart_money_trades WHERE 1=1`
	var args []any

	if len(wallets) > 0 {
		placeholders := strings.Repeat("?,", len(wallets))
		placeholders = placeholders[:len(placeholders)-1]
		q += ` AND proxy_wallet IN (` + placeholders + `)`
		for _, w := range wallets {
			args = append(args, w)
		}
	}
	if minUSD > 0 {
		q += ` AND usd_value >= ?`
		args = append(args, minUSD)
	}
	if maxPrice > 0 {
		q += ` AND price <= ?`
		args = append(args, maxPrice)
	}
	if beforeTS > 0 {
		q += ` AND timestamp < ?`
		args = append(args, beforeTS)
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanSMTrades(rows)
}

// RecentByWallet returns the N most recent smart money trades for a specific wallet.
func (s *SmartMoneyTradeStore) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]SmartMoneyTrade, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT `+smTradeDBCols+` FROM smart_money_trades
		WHERE proxy_wallet = ?
		ORDER BY timestamp DESC LIMIT ?`, wallet, limit)
	if err != nil {
		return nil, err
	}
	return scanSMTrades(rows)
}

// Count returns the total number of smart money trade records.
func (s *SmartMoneyTradeStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM smart_money_trades`).Scan(&n)
	return n, err
}

// DeleteOlderThan removes smart money trade records older than the given cutoff.
func (s *SmartMoneyTradeStore) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM smart_money_trades WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
