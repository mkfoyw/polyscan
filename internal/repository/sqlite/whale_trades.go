package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// WhaleTradeRepo implements repository.WhaleTradeRepository using SQLite.
type WhaleTradeRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

const whaleTradeDBCols = `proxy_wallet, side, asset, condition_id,
	size, price, usd_value, timestamp, title, slug, event_slug, outcome,
	transaction_hash, created_at`

func scanWhaleTrade(sc interface{ Scan(...any) error }) (repository.WhaleTrade, error) {
	var r repository.WhaleTrade
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

func scanWhaleTrades(rows *sql.Rows) ([]repository.WhaleTrade, error) {
	defer rows.Close()
	var out []repository.WhaleTrade
	for rows.Next() {
		r, err := scanWhaleTrade(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *WhaleTradeRepo) Insert(ctx context.Context, rec *repository.WhaleTrade) error {
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

func (s *WhaleTradeRepo) Recent(ctx context.Context, wallets []string, limit int64, beforeTS, afterTS int64, minUSD float64, maxPrice float64) ([]repository.WhaleTrade, error) {
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
	if maxPrice > 0 {
		q += ` AND price <= ?`
		args = append(args, maxPrice)
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

func (s *WhaleTradeRepo) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]repository.WhaleTrade, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT `+whaleTradeDBCols+` FROM whale_trades
		WHERE proxy_wallet = ?
		ORDER BY timestamp DESC LIMIT ?`, wallet, limit)
	if err != nil {
		return nil, err
	}
	return scanWhaleTrades(rows)
}

func (s *WhaleTradeRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM whale_trades`).Scan(&n)
	return n, err
}

func (s *WhaleTradeRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM whale_trades WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
