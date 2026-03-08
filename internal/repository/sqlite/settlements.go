package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// SettlementRepo implements repository.SettlementRepository using SQLite.
type SettlementRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

func scanSettlement(sc interface{ Scan(...any) error }) (repository.SettlementRecord, error) {
	var r repository.SettlementRecord
	var ts int64
	err := sc.Scan(&r.ConditionID, &r.Question, &r.Slug, &r.EventSlug,
		&r.Outcome, &r.ImageURL, &ts)
	if err != nil {
		return r, err
	}
	r.ResolvedAt = time.Unix(ts, 0)
	return r, nil
}

func scanSettlements(rows *sql.Rows) ([]repository.SettlementRecord, error) {
	defer rows.Close()
	var out []repository.SettlementRecord
	for rows.Next() {
		r, err := scanSettlement(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SettlementRepo) Upsert(ctx context.Context, rec *repository.SettlementRecord) error {
	if rec.ResolvedAt.IsZero() {
		rec.ResolvedAt = time.Now()
	}
	_, err := s.wdb.ExecContext(ctx, `
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

func (s *SettlementRepo) Recent(ctx context.Context, limit int64, before time.Time) ([]repository.SettlementRecord, error) {
	q := `SELECT condition_id, question, slug, event_slug, outcome, image_url, resolved_at
		  FROM settlements`
	var args []any
	if !before.IsZero() {
		q += ` WHERE resolved_at < ?`
		args = append(args, before.Unix())
	}
	q += ` ORDER BY resolved_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanSettlements(rows)
}

func (s *SettlementRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM settlements`).Scan(&n)
	return n, err
}

func (s *SettlementRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM settlements WHERE resolved_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
