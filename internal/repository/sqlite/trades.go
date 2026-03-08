package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// TradeRepo implements repository.TradeRepository using SQLite.
type TradeRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

const tradeCols = `proxy_wallet, pseudonym, profile_name, side, asset, condition_id,
	size, price, usd_value, timestamp, title, slug, event_slug, outcome,
	transaction_hash, source, created_at`

func scanTrade(sc interface{ Scan(...any) error }) (repository.TradeRecord, error) {
	var r repository.TradeRecord
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

func scanTrades(rows *sql.Rows) ([]repository.TradeRecord, error) {
	defer rows.Close()
	var out []repository.TradeRecord
	for rows.Next() {
		r, err := scanTrade(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *TradeRepo) Insert(ctx context.Context, rec *repository.TradeRecord) error {
	rec.CreatedAt = time.Now()
	_, err := s.wdb.ExecContext(ctx, `
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

func (s *TradeRepo) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]repository.TradeRecord, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT `+tradeCols+` FROM trades
		WHERE proxy_wallet = ?
		ORDER BY timestamp DESC LIMIT ?`, wallet, limit)
	if err != nil {
		return nil, err
	}
	return scanTrades(rows)
}

func (s *TradeRepo) RecentLarge(ctx context.Context, minUSD float64, limit int64, beforeTS int64, maxPrice float64, minPrice float64) ([]repository.TradeRecord, error) {
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
	if minPrice > 0 {
		q += ` AND price >= ?`
		args = append(args, minPrice)
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanTrades(rows)
}

func (s *TradeRepo) ExistsByTxHash(ctx context.Context, txHash string) (bool, error) {
	if txHash == "" {
		return false, nil
	}
	var n int
	err := s.rdb.QueryRowContext(ctx,
		`SELECT 1 FROM trades WHERE transaction_hash = ? LIMIT 1`, txHash).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (s *TradeRepo) Recent(ctx context.Context, limit int64, beforeTS int64) ([]repository.TradeRecord, error) {
	q := `SELECT ` + tradeCols + ` FROM trades`
	var args []any
	if beforeTS > 0 {
		q += ` WHERE timestamp < ?`
		args = append(args, beforeTS)
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanTrades(rows)
}

func (s *TradeRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM trades`).Scan(&n)
	return n, err
}

func (s *TradeRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM trades WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *TradeRepo) EnrichByAssetTimestamp(ctx context.Context, asset string, timestamp int64, proxyWallet, pseudonym, profileName, txHash string) error {
	q := `UPDATE trades SET proxy_wallet=?, pseudonym=?, transaction_hash=?`
	args := []any{proxyWallet, pseudonym, txHash}
	if profileName != "" {
		q += `, profile_name=?`
		args = append(args, profileName)
	}
	q += ` WHERE asset=? AND timestamp=? AND source='ws'`
	args = append(args, asset, timestamp)
	_, err := s.wdb.ExecContext(ctx, q, args...)
	return err
}

func (s *TradeRepo) UpsertRESTTrade(ctx context.Context, rec *repository.TradeRecord) error {
	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM trades
		WHERE asset=? AND size=? AND source IN ('ws','ws+rest')
		  AND timestamp BETWEEN ? AND ?
		LIMIT 1`,
		rec.Asset, rec.Size, rec.Timestamp-10, rec.Timestamp+10,
	).Scan(&id)

	if err == nil {
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
