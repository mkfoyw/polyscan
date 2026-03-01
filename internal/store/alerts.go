package store

import (
	"context"
	"database/sql"
	"time"
)

// AlertRecord is an alert persisted to SQLite.
type AlertRecord struct {
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// AlertStore wraps the alerts table.
type AlertStore struct {
	db *sql.DB
}

func scanAlert(sc interface{ Scan(...any) error }) (AlertRecord, error) {
	var r AlertRecord
	var ts int64
	err := sc.Scan(&r.Type, &r.Message, &ts)
	if err != nil {
		return r, err
	}
	r.CreatedAt = time.Unix(ts, 0)
	return r, nil
}

func scanAlerts(rows *sql.Rows) ([]AlertRecord, error) {
	defer rows.Close()
	var out []AlertRecord
	for rows.Next() {
		r, err := scanAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Insert saves an alert record.
func (s *AlertStore) Insert(ctx context.Context, rec *AlertRecord) error {
	rec.CreatedAt = time.Now()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO alerts (type, message, created_at) VALUES (?,?,?)`,
		rec.Type, rec.Message, rec.CreatedAt.Unix())
	return err
}

// Recent returns the N most recent alerts.
func (s *AlertStore) Recent(ctx context.Context, limit int64) ([]AlertRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT type, message, created_at FROM alerts ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return scanAlerts(rows)
}

// DeleteOlderThan removes alert records with created_at before the given cutoff.
func (s *AlertStore) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM alerts WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// RecentByType returns recent alerts of a given type.
func (s *AlertStore) RecentByType(ctx context.Context, alertType string, limit int64) ([]AlertRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT type, message, created_at FROM alerts
		 WHERE type = ? ORDER BY created_at DESC LIMIT ?`, alertType, limit)
	if err != nil {
		return nil, err
	}
	return scanAlerts(rows)
}

// Count returns the total number of alert records.
func (s *AlertStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM alerts`).Scan(&n)
	return n, err
}
