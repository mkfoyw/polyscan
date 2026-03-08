package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// PriceEventRepo implements repository.PriceEventRepository using SQLite.
type PriceEventRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

func scanPriceEvent(sc interface{ Scan(...any) error }) (repository.PriceEventRecord, error) {
	var r repository.PriceEventRecord
	var ts int64
	err := sc.Scan(&r.AssetID, &r.Market, &r.Question, &r.Outcome,
		&r.Window, &r.PctChange, &r.OldPrice, &r.NewPrice, &ts)
	if err != nil {
		return r, err
	}
	r.DetectedAt = time.Unix(ts, 0)
	return r, nil
}

func scanPriceEvents(rows *sql.Rows) ([]repository.PriceEventRecord, error) {
	defer rows.Close()
	var out []repository.PriceEventRecord
	for rows.Next() {
		r, err := scanPriceEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PriceEventRepo) Insert(ctx context.Context, rec *repository.PriceEventRecord) error {
	rec.DetectedAt = time.Now()
	_, err := s.wdb.ExecContext(ctx, `
		INSERT INTO price_events (asset_id, market, question, outcome, window, pct_change, old_price, new_price, detected_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		rec.AssetID, rec.Market, rec.Question, rec.Outcome,
		rec.Window, rec.PctChange, rec.OldPrice, rec.NewPrice,
		rec.DetectedAt.Unix(),
	)
	return err
}

func (s *PriceEventRepo) RecentByAsset(ctx context.Context, assetID string, limit int64) ([]repository.PriceEventRecord, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT asset_id, market, question, outcome, window, pct_change, old_price, new_price, detected_at
		FROM price_events WHERE asset_id = ?
		ORDER BY detected_at DESC LIMIT ?`, assetID, limit)
	if err != nil {
		return nil, err
	}
	return scanPriceEvents(rows)
}

func (s *PriceEventRepo) Recent(ctx context.Context, limit int64) ([]repository.PriceEventRecord, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT asset_id, market, question, outcome, window, pct_change, old_price, new_price, detected_at
		FROM price_events ORDER BY detected_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return scanPriceEvents(rows)
}

func (s *PriceEventRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM price_events`).Scan(&n)
	return n, err
}

func (s *PriceEventRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM price_events WHERE detected_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
