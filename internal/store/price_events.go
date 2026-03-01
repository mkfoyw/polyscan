package store

import (
	"context"
	"database/sql"
	"time"
)

// PriceEventRecord is a price spike event persisted to SQLite.
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

// PriceEventStore wraps the price_events table.
type PriceEventStore struct {
	db *sql.DB
}

func scanPriceEvent(sc interface{ Scan(...any) error }) (PriceEventRecord, error) {
	var r PriceEventRecord
	var ts int64
	err := sc.Scan(&r.AssetID, &r.Market, &r.Question, &r.Outcome,
		&r.Window, &r.PctChange, &r.OldPrice, &r.NewPrice, &ts)
	if err != nil {
		return r, err
	}
	r.DetectedAt = time.Unix(ts, 0)
	return r, nil
}

func scanPriceEvents(rows *sql.Rows) ([]PriceEventRecord, error) {
	defer rows.Close()
	var out []PriceEventRecord
	for rows.Next() {
		r, err := scanPriceEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Insert saves a price spike event.
func (s *PriceEventStore) Insert(ctx context.Context, rec *PriceEventRecord) error {
	rec.DetectedAt = time.Now()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO price_events (asset_id, market, question, outcome, window, pct_change, old_price, new_price, detected_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		rec.AssetID, rec.Market, rec.Question, rec.Outcome,
		rec.Window, rec.PctChange, rec.OldPrice, rec.NewPrice,
		rec.DetectedAt.Unix(),
	)
	return err
}

// RecentByAsset returns recent price events for a given asset.
func (s *PriceEventStore) RecentByAsset(ctx context.Context, assetID string, limit int64) ([]PriceEventRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT asset_id, market, question, outcome, window, pct_change, old_price, new_price, detected_at
		FROM price_events WHERE asset_id = ?
		ORDER BY detected_at DESC LIMIT ?`, assetID, limit)
	if err != nil {
		return nil, err
	}
	return scanPriceEvents(rows)
}

// Recent returns the N most recent price events across all assets.
func (s *PriceEventStore) Recent(ctx context.Context, limit int64) ([]PriceEventRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT asset_id, market, question, outcome, window, pct_change, old_price, new_price, detected_at
		FROM price_events ORDER BY detected_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return scanPriceEvents(rows)
}

// Count returns the total number of price event records.
func (s *PriceEventStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM price_events`).Scan(&n)
	return n, err
}

// DeleteOlderThan removes price event records with detected_at before the given cutoff.
func (s *PriceEventStore) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM price_events WHERE detected_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
