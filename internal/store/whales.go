package store

import (
	"context"
	"database/sql"
	"time"
)

// WhaleRecord is a whale wallet persisted to SQLite.
type WhaleRecord struct {
	Address     string    `json:"address"`
	Alias       string    `json:"alias,omitempty"`
	Source      string    `json:"source"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	TotalVolume float64   `json:"total_volume"`
	LastPollTS  int64     `json:"last_poll_ts"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// WhaleStore wraps the whales table.
type WhaleStore struct {
	db *sql.DB
}

// Upsert inserts or updates a whale record. Returns true if newly inserted.
func (s *WhaleStore) Upsert(ctx context.Context, rec *WhaleRecord) (bool, error) {
	rec.UpdatedAt = time.Now()

	// Check if exists first (for return value)
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM whales WHERE address = ?`, rec.Address).Scan(&exists)
	isNew := err == sql.ErrNoRows

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO whales (address, alias, source, first_seen_at, total_volume, last_poll_ts, updated_at)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(address) DO UPDATE SET
			alias        = excluded.alias,
			source       = excluded.source,
			total_volume = excluded.total_volume,
			last_poll_ts = excluded.last_poll_ts,
			updated_at   = excluded.updated_at`,
		rec.Address, rec.Alias, rec.Source,
		rec.FirstSeenAt.Unix(), rec.TotalVolume, rec.LastPollTS,
		rec.UpdatedAt.Unix(),
	)
	if err != nil {
		return false, err
	}
	return isNew, nil
}

// GetAll returns all tracked whales.
func (s *WhaleStore) GetAll(ctx context.Context) ([]WhaleRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT address, alias, source, first_seen_at, total_volume, last_poll_ts, updated_at FROM whales`)
	if err != nil {
		return nil, err
	}
	return scanWhales(rows)
}

// GetByAddress returns a single whale record.
func (s *WhaleStore) GetByAddress(ctx context.Context, address string) (*WhaleRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT address, alias, source, first_seen_at, total_volume, last_poll_ts, updated_at
		 FROM whales WHERE address = ?`, address)
	r, err := scanWhale(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// UpdateLastPollTS updates the last poll timestamp.
func (s *WhaleStore) UpdateLastPollTS(ctx context.Context, address string, ts int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE whales SET last_poll_ts = ?, updated_at = ? WHERE address = ?`,
		ts, time.Now().Unix(), address)
	return err
}

// IncrVolume atomically increments the total volume.
func (s *WhaleStore) IncrVolume(ctx context.Context, address string, amount float64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE whales SET total_volume = total_volume + ?, updated_at = ? WHERE address = ?`,
		amount, time.Now().Unix(), address)
	return err
}

// Count returns the total number of tracked whales.
func (s *WhaleStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM whales`).Scan(&n)
	return n, err
}

// DeleteByAddress deletes a whale by address.
func (s *WhaleStore) DeleteByAddress(ctx context.Context, address string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM whales WHERE address = ?`, address)
	return err
}

// DeleteLowestVolume deletes the auto-tracked whale with the lowest volume.
func (s *WhaleStore) DeleteLowestVolume(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM whales WHERE id = (
			SELECT id FROM whales WHERE source = 'auto'
			ORDER BY total_volume ASC LIMIT 1
		)`)
	return err
}

// Exists checks if a whale with this address exists.
func (s *WhaleStore) Exists(ctx context.Context, address string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM whales WHERE address = ? LIMIT 1`, address).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func scanWhale(sc interface{ Scan(...any) error }) (WhaleRecord, error) {
	var r WhaleRecord
	var firstSeen, updated int64
	err := sc.Scan(&r.Address, &r.Alias, &r.Source, &firstSeen,
		&r.TotalVolume, &r.LastPollTS, &updated)
	if err != nil {
		return r, err
	}
	r.FirstSeenAt = time.Unix(firstSeen, 0)
	r.UpdatedAt = time.Unix(updated, 0)
	return r, nil
}

func scanWhales(rows *sql.Rows) ([]WhaleRecord, error) {
	defer rows.Close()
	var out []WhaleRecord
	for rows.Next() {
		r, err := scanWhale(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
