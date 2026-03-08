package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// AlertRepo implements repository.AlertRepository using SQLite.
type AlertRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

func scanAlert(sc interface{ Scan(...any) error }) (repository.AlertRecord, error) {
	var r repository.AlertRecord
	var ts int64
	err := sc.Scan(&r.Type, &r.Message, &ts)
	if err != nil {
		return r, err
	}
	r.CreatedAt = time.Unix(ts, 0)
	return r, nil
}

func scanAlerts(rows *sql.Rows) ([]repository.AlertRecord, error) {
	defer rows.Close()
	var out []repository.AlertRecord
	for rows.Next() {
		r, err := scanAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *AlertRepo) Insert(ctx context.Context, rec *repository.AlertRecord) error {
	rec.CreatedAt = time.Now()
	_, err := s.wdb.ExecContext(ctx,
		`INSERT INTO alerts (type, message, created_at) VALUES (?,?,?)`,
		rec.Type, rec.Message, rec.CreatedAt.Unix())
	return err
}

func (s *AlertRepo) Recent(ctx context.Context, limit int64) ([]repository.AlertRecord, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT type, message, created_at FROM alerts ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return scanAlerts(rows)
}

func (s *AlertRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM alerts WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *AlertRepo) RecentByType(ctx context.Context, alertType string, limit int64) ([]repository.AlertRecord, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT type, message, created_at FROM alerts
		 WHERE type = ? ORDER BY created_at DESC LIMIT ?`, alertType, limit)
	if err != nil {
		return nil, err
	}
	return scanAlerts(rows)
}

func (s *AlertRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM alerts`).Scan(&n)
	return n, err
}
