package store

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// WhaleTrade is a trade from a tracked whale wallet.
type WhaleTrade struct {
	ProxyWallet     string    `json:"proxy_wallet"`
	ProfileName     string    `json:"profile_name,omitempty"` // populated at query time from profiles table
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

// WhaleTradStore wraps the whale_trades table.
type WhaleTradStore struct {
	rdb *sql.DB // reader pool
	wdb *sql.DB // writer pool
}

const whaleTradeDBCols = `proxy_wallet, side, asset, condition_id,
	size, price, usd_value, timestamp, title, slug, event_slug, outcome,
	transaction_hash, created_at`

func scanWhaleTrade(sc interface{ Scan(...any) error }) (WhaleTrade, error) {
	var r WhaleTrade
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
	r.Source = "whale"
	return r, nil
}

func scanWhaleTrades(rows *sql.Rows) ([]WhaleTrade, error) {
	defer rows.Close()
	var out []WhaleTrade
	for rows.Next() {
		r, err := scanWhaleTrade(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Insert inserts a whale trade. Duplicates (same transaction_hash) are silently ignored.
func (s *WhaleTradStore) Insert(ctx context.Context, rec *WhaleTrade) error {
	rec.CreatedAt = time.Now()
	_, err := s.wdb.ExecContext(ctx, `
		INSERT INTO whale_trades (`+whaleTradeDBCols+`)
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

// Recent returns whale trades, most recent first.
// Optionally filters by wallet addresses, min USD, and time range.
func (s *WhaleTradStore) Recent(ctx context.Context, wallets []string, limit int64, beforeTS, afterTS int64, minUSD float64) ([]WhaleTrade, error) {
	q := `SELECT ` + whaleTradeDBCols + ` FROM whale_trades WHERE 1=1`
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
	if beforeTS > 0 {
		q += ` AND timestamp < ?`
		args = append(args, beforeTS)
	}
	if afterTS > 0 {
		q += ` AND timestamp > ?`
		args = append(args, afterTS)
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanWhaleTrades(rows)
}

// RecentByWallet returns the N most recent whale trades for a specific wallet.
func (s *WhaleTradStore) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]WhaleTrade, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT `+whaleTradeDBCols+` FROM whale_trades
		WHERE proxy_wallet = ?
		ORDER BY timestamp DESC LIMIT ?`, wallet, limit)
	if err != nil {
		return nil, err
	}
	return scanWhaleTrades(rows)
}

// Count returns the total number of whale trade records.
func (s *WhaleTradStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM whale_trades`).Scan(&n)
	return n, err
}

// DeleteOlderThan removes whale trade records with created_at before the given cutoff.
func (s *WhaleTradStore) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM whale_trades WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
