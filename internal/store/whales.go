package store

import (
	"context"
	"database/sql"
	"time"
)

// WhaleRecord is a whale user persisted to SQLite (whale_users table).
type WhaleRecord struct {
	Address          string    `json:"address"`
	Alias            string    `json:"alias,omitempty"`
	Name             string    `json:"name,omitempty"`
	Pseudonym        string    `json:"pseudonym,omitempty"`
	Source           string    `json:"source"`
	FirstSeenAt      time.Time `json:"first_seen_at"`
	TotalVolume      float64   `json:"total_volume"`
	LastPollTS       int64     `json:"last_poll_ts"`
	ProfileFetchedAt time.Time `json:"profile_fetched_at,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// DisplayName returns the best display name: alias > name > pseudonym > address.
func (r *WhaleRecord) DisplayName() string {
	if r.Alias != "" {
		return r.Alias
	}
	if r.Name != "" {
		return r.Name
	}
	if r.Pseudonym != "" {
		return r.Pseudonym
	}
	return r.Address
}

// WhaleStore wraps the whale_users table.
type WhaleStore struct {
	rdb *sql.DB // reader pool
	wdb *sql.DB // writer pool
}

const whaleUserCols = `address, alias, name, pseudonym, source, first_seen_at, total_volume, last_poll_ts, profile_fetched_at, updated_at`

// Upsert inserts or updates a whale user record. Returns true if newly inserted.
func (s *WhaleStore) Upsert(ctx context.Context, rec *WhaleRecord) (bool, error) {
	rec.UpdatedAt = time.Now()

	// Check if exists first (for return value)
	var exists bool
	err := s.rdb.QueryRowContext(ctx,
		`SELECT 1 FROM whale_users WHERE address = ?`, rec.Address).Scan(&exists)
	isNew := err == sql.ErrNoRows

	_, err = s.wdb.ExecContext(ctx, `
		INSERT INTO whale_users (address, alias, name, pseudonym, source, first_seen_at, total_volume, last_poll_ts, profile_fetched_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(address) DO UPDATE SET
			alias              = excluded.alias,
			name               = excluded.name,
			pseudonym          = excluded.pseudonym,
			source             = excluded.source,
			total_volume       = excluded.total_volume,
			last_poll_ts       = excluded.last_poll_ts,
			profile_fetched_at = excluded.profile_fetched_at,
			updated_at         = excluded.updated_at`,
		rec.Address, rec.Alias, rec.Name, rec.Pseudonym, rec.Source,
		rec.FirstSeenAt.Unix(), rec.TotalVolume, rec.LastPollTS,
		rec.ProfileFetchedAt.Unix(), rec.UpdatedAt.Unix(),
	)
	if err != nil {
		return false, err
	}
	return isNew, nil
}

// GetAll returns all tracked whale users.
func (s *WhaleStore) GetAll(ctx context.Context) ([]WhaleRecord, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT `+whaleUserCols+` FROM whale_users`)
	if err != nil {
		return nil, err
	}
	return scanWhales(rows)
}

// GetByAddress returns a single whale user record.
func (s *WhaleStore) GetByAddress(ctx context.Context, address string) (*WhaleRecord, error) {
	row := s.rdb.QueryRowContext(ctx,
		`SELECT `+whaleUserCols+` FROM whale_users WHERE address = ?`, address)
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
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE whale_users SET last_poll_ts = ?, updated_at = ? WHERE address = ?`,
		ts, time.Now().Unix(), address)
	return err
}

// IncrVolume atomically increments the total volume.
func (s *WhaleStore) IncrVolume(ctx context.Context, address string, amount float64) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE whale_users SET total_volume = total_volume + ?, updated_at = ? WHERE address = ?`,
		amount, time.Now().Unix(), address)
	return err
}

// UpdateProfile updates the cached profile name/pseudonym for a whale user.
func (s *WhaleStore) UpdateProfile(ctx context.Context, address, name, pseudonym string) error {
	now := time.Now().Unix()
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE whale_users SET name = ?, pseudonym = ?, profile_fetched_at = ?, updated_at = ? WHERE address = ?`,
		name, pseudonym, now, now, address)
	return err
}

// Count returns the total number of tracked whale users.
func (s *WhaleStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM whale_users`).Scan(&n)
	return n, err
}

// DeleteByAddress deletes a whale user by address.
func (s *WhaleStore) DeleteByAddress(ctx context.Context, address string) error {
	_, err := s.wdb.ExecContext(ctx, `DELETE FROM whale_users WHERE address = ?`, address)
	return err
}

// DeleteLowestVolume deletes the auto-tracked whale user with the lowest volume.
func (s *WhaleStore) DeleteLowestVolume(ctx context.Context) error {
	_, err := s.wdb.ExecContext(ctx, `
		DELETE FROM whale_users WHERE id = (
			SELECT id FROM whale_users WHERE source = 'auto'
			ORDER BY total_volume ASC LIMIT 1
		)`)
	return err
}

// Exists checks if a whale user with this address exists.
func (s *WhaleStore) Exists(ctx context.Context, address string) (bool, error) {
	var n int
	err := s.rdb.QueryRowContext(ctx,
		`SELECT 1 FROM whale_users WHERE address = ? LIMIT 1`, address).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func scanWhale(sc interface{ Scan(...any) error }) (WhaleRecord, error) {
	var r WhaleRecord
	var firstSeen, profileFetched, updated int64
	err := sc.Scan(&r.Address, &r.Alias, &r.Name, &r.Pseudonym, &r.Source, &firstSeen,
		&r.TotalVolume, &r.LastPollTS, &profileFetched, &updated)
	if err != nil {
		return r, err
	}
	r.FirstSeenAt = time.Unix(firstSeen, 0)
	r.ProfileFetchedAt = time.Unix(profileFetched, 0)
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
