package store

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// TradeRecord is a trade persisted to SQLite.
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

// TradeStore wraps the trades table.
type TradeStore struct {
	db *sql.DB
}

const tradeCols = `proxy_wallet, pseudonym, profile_name, side, asset, condition_id,
	size, price, usd_value, timestamp, title, slug, event_slug, outcome,
	transaction_hash, source, created_at`

func scanTrade(sc interface{ Scan(...any) error }) (TradeRecord, error) {
	var r TradeRecord
	var createdUnix int64
	err := sc.Scan(
		&r.ProxyWallet, &r.Pseudonym, &r.ProfileName,
		&r.Side, &r.Asset, &r.ConditionID,
		&r.Size, &r.Price, &r.USDValue, &r.Timestamp,
		&r.Title, &r.Slug, &r.EventSlug, &r.Outcome,
		&r.TransactionHash, &r.Source, &createdUnix,
	)
	if err != nil {
		return r, err
	}
	r.CreatedAt = time.Unix(createdUnix, 0)
	return r, nil
}

func scanTrades(rows *sql.Rows) ([]TradeRecord, error) {
	defer rows.Close()
	var out []TradeRecord
	for rows.Next() {
		r, err := scanTrade(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Insert inserts a trade record. Duplicates (same transaction_hash) are silently ignored.
func (s *TradeStore) Insert(ctx context.Context, rec *TradeRecord) error {
	rec.CreatedAt = time.Now()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO trades (`+tradeCols+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ProxyWallet, rec.Pseudonym, rec.ProfileName,
		rec.Side, rec.Asset, rec.ConditionID,
		rec.Size, rec.Price, rec.USDValue, rec.Timestamp,
		rec.Title, rec.Slug, rec.EventSlug, rec.Outcome,
		rec.TransactionHash, rec.Source, rec.CreatedAt.Unix(),
	)
	if err != nil && isUniqueViolation(err) {
		return nil
	}
	return err
}

// RecentByWallet returns the N most recent trades for a wallet.
func (s *TradeStore) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]TradeRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+tradeCols+` FROM trades
		WHERE proxy_wallet = ?
		ORDER BY timestamp DESC LIMIT ?`, wallet, limit)
	if err != nil {
		return nil, err
	}
	return scanTrades(rows)
}

// RecentByWhales returns trades from tracked whale wallets, most recent first.
// wallets is a list of whale addresses. Supports cursor pagination via beforeTS.
// If afterTS > 0, only returns trades newer than that timestamp.
func (s *TradeStore) RecentByWhales(ctx context.Context, wallets []string, limit int64, beforeTS int64, afterTS int64, minUSD float64) ([]TradeRecord, error) {
	if len(wallets) == 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(wallets))
	placeholders = placeholders[:len(placeholders)-1] // trim trailing comma

	q := `SELECT ` + tradeCols + ` FROM trades WHERE proxy_wallet IN (` + placeholders + `)`
	args := make([]any, len(wallets))
	for i, w := range wallets {
		args[i] = w
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

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanTrades(rows)
}

// RecentLarge returns trades above a USD threshold, most recent first.
// If beforeTS > 0, only returns trades older than that timestamp (cursor pagination).
// If maxPrice > 0, only returns trades with price <= maxPrice.
func (s *TradeStore) RecentLarge(ctx context.Context, minUSD float64, limit int64, beforeTS int64, maxPrice float64) ([]TradeRecord, error) {
	q := `SELECT ` + tradeCols + ` FROM trades WHERE usd_value >= ?`
	args := []any{minUSD}
	if beforeTS > 0 {
		q += ` AND timestamp < ?`
		args = append(args, beforeTS)
	}
	if maxPrice > 0 {
		q += ` AND price <= ?`
		args = append(args, maxPrice)
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanTrades(rows)
}

// ExistsByTxHash checks if a trade with this transaction hash already exists.
func (s *TradeStore) ExistsByTxHash(ctx context.Context, txHash string) (bool, error) {
	if txHash == "" {
		return false, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM trades WHERE transaction_hash = ? LIMIT 1`, txHash).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// Recent returns the N most recent trades.
// If beforeTS > 0, only returns trades older than that timestamp (cursor pagination).
func (s *TradeStore) Recent(ctx context.Context, limit int64, beforeTS int64) ([]TradeRecord, error) {
	q := `SELECT ` + tradeCols + ` FROM trades`
	var args []any
	if beforeTS > 0 {
		q += ` WHERE timestamp < ?`
		args = append(args, beforeTS)
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanTrades(rows)
}

// Count returns the total number of trade records.
func (s *TradeStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM trades`).Scan(&n)
	return n, err
}

// DeleteOlderThan removes trade records with created_at before the given cutoff.
func (s *TradeStore) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM trades WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// EnrichByAssetTimestamp updates a WS trade record with wallet info from REST.
func (s *TradeStore) EnrichByAssetTimestamp(ctx context.Context, asset string, timestamp int64, proxyWallet, pseudonym, profileName, txHash string) error {
	q := `UPDATE trades SET proxy_wallet=?, pseudonym=?, transaction_hash=?`
	args := []any{proxyWallet, pseudonym, txHash}
	if profileName != "" {
		q += `, profile_name=?`
		args = append(args, profileName)
	}
	q += ` WHERE asset=? AND timestamp=? AND source='ws'`
	args = append(args, asset, timestamp)
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

// UpsertRESTTrade either enriches an existing WS trade (same asset, size, timestamp ±10s)
// with REST wallet info, or inserts a new REST record if no WS match found.
func (s *TradeStore) UpsertRESTTrade(ctx context.Context, rec *TradeRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Look for a matching WS trade to enrich
	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM trades
		WHERE asset=? AND size=? AND source IN ('ws','ws+rest')
		  AND timestamp BETWEEN ? AND ?
		LIMIT 1`,
		rec.Asset, rec.Size, rec.Timestamp-10, rec.Timestamp+10,
	).Scan(&id)

	if err == nil {
		// Found matching WS trade — enrich it
		_, err = tx.ExecContext(ctx, `
			UPDATE trades
			SET proxy_wallet=?, pseudonym=?, profile_name=?, transaction_hash=?,
			    slug=?, event_slug=?, source='ws+rest'
			WHERE id=?`,
			rec.ProxyWallet, rec.Pseudonym, rec.ProfileName, rec.TransactionHash,
			rec.Slug, rec.EventSlug, id,
		)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	if err != sql.ErrNoRows {
		return err
	}

	// No matching WS trade — insert as a new REST record
	rec.CreatedAt = time.Now()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO trades (`+tradeCols+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ProxyWallet, rec.Pseudonym, rec.ProfileName,
		rec.Side, rec.Asset, rec.ConditionID,
		rec.Size, rec.Price, rec.USDValue, rec.Timestamp,
		rec.Title, rec.Slug, rec.EventSlug, rec.Outcome,
		rec.TransactionHash, rec.Source, rec.CreatedAt.Unix(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return tx.Commit()
		}
		return err
	}
	return tx.Commit()
}

// isUniqueViolation checks whether the error is a SQLite UNIQUE constraint failure.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// ProfileNamesForWallets returns a map of wallet address → profile_name
// by picking the most recently used non-empty profile_name from the trades table.
func (s *TradeStore) ProfileNamesForWallets(ctx context.Context, wallets []string) (map[string]string, error) {
	if len(wallets) == 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(wallets))
	placeholders = placeholders[:len(placeholders)-1]

	q := `SELECT proxy_wallet, profile_name FROM trades
		WHERE proxy_wallet IN (` + placeholders + `) AND profile_name != ''
		GROUP BY proxy_wallet
		HAVING timestamp = MAX(timestamp)`
	args := make([]any, len(wallets))
	for i, w := range wallets {
		args[i] = w
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[string]string, len(wallets))
	for rows.Next() {
		var addr, name string
		if err := rows.Scan(&addr, &name); err != nil {
			return nil, err
		}
		m[addr] = name
	}
	return m, rows.Err()
}
