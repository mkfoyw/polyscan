package store

import (
	"context"
	"database/sql"
	"time"
)

// SettlementRecord is a resolved market persisted to SQLite.
type SettlementRecord struct {
	ConditionID string    `json:"condition_id"`
	Question    string    `json:"question"`
	Slug        string    `json:"slug"`
	EventSlug   string    `json:"event_slug"`
	Outcome     string    `json:"outcome"`
	ImageURL    string    `json:"image_url"`
	ResolvedAt  time.Time `json:"resolved_at"`
}

// SettlementStore wraps the settlements table.
type SettlementStore struct {
	db *sql.DB
}

func scanSettlement(sc interface{ Scan(...any) error }) (SettlementRecord, error) {
	var r SettlementRecord
	var ts int64
	err := sc.Scan(&r.ConditionID, &r.Question, &r.Slug, &r.EventSlug,
		&r.Outcome, &r.ImageURL, &ts)
	if err != nil {
		return r, err
	}
	r.ResolvedAt = time.Unix(ts, 0)
	return r, nil
}

func scanSettlements(rows *sql.Rows) ([]SettlementRecord, error) {
	defer rows.Close()
	var out []SettlementRecord
	for rows.Next() {
		r, err := scanSettlement(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Upsert inserts or updates a settlement record (keyed on condition_id).
// resolved_at is only set on first insert, never updated.
func (s *SettlementStore) Upsert(ctx context.Context, rec *SettlementRecord) error {
	if rec.ResolvedAt.IsZero() {
		rec.ResolvedAt = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO settlements (condition_id, question, slug, event_slug, outcome, image_url, resolved_at)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(condition_id) DO UPDATE SET
			question  = excluded.question,
			slug      = excluded.slug,
			event_slug= excluded.event_slug,
			outcome   = excluded.outcome,
			image_url = excluded.image_url`,
		rec.ConditionID, rec.Question, rec.Slug, rec.EventSlug,
		rec.Outcome, rec.ImageURL, rec.ResolvedAt.Unix(),
	)
	return err
}

// Recent returns the N most recently resolved markets.
// If before is non-zero, only returns settlements resolved before that time.
func (s *SettlementStore) Recent(ctx context.Context, limit int64, before time.Time) ([]SettlementRecord, error) {
	q := `SELECT condition_id, question, slug, event_slug, outcome, image_url, resolved_at
		  FROM settlements`
	var args []any
	if !before.IsZero() {
		q += ` WHERE resolved_at < ?`
		args = append(args, before.Unix())
	}
	q += ` ORDER BY resolved_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanSettlements(rows)
}

// Count returns the total number of settlement records.
func (s *SettlementStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM settlements`).Scan(&n)
	return n, err
}

// DeleteOlderThan removes settlement records with resolved_at before the given cutoff.
func (s *SettlementStore) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM settlements WHERE resolved_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
