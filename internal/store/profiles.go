package store

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// ProfileRecord is a cached user profile.
type ProfileRecord struct {
	Address   string    `json:"address"`
	Name      string    `json:"name"`
	Pseudonym string    `json:"pseudonym,omitempty"`
	FetchedAt time.Time `json:"fetched_at"`
}

// DisplayName returns the best display name: user-chosen name first, else pseudonym.
func (p *ProfileRecord) DisplayName() string {
	if p.Name != "" {
		return p.Name
	}
	return p.Pseudonym
}

// ProfileStore wraps the profiles table.
type ProfileStore struct {
	rdb *sql.DB // reader pool
	wdb *sql.DB // writer pool
}

// Upsert inserts or updates a profile record.
func (s *ProfileStore) Upsert(ctx context.Context, rec *ProfileRecord) error {
	if rec.FetchedAt.IsZero() {
		rec.FetchedAt = time.Now()
	}
	_, err := s.wdb.ExecContext(ctx, `
		INSERT INTO profiles (address, name, pseudonym, fetched_at)
		VALUES (?,?,?,?)
		ON CONFLICT(address) DO UPDATE SET
			name       = excluded.name,
			pseudonym  = excluded.pseudonym,
			fetched_at = excluded.fetched_at`,
		rec.Address, rec.Name, rec.Pseudonym, rec.FetchedAt.Unix(),
	)
	return err
}

// Get returns a profile by address, or nil if not found.
func (s *ProfileStore) Get(ctx context.Context, address string) (*ProfileRecord, error) {
	var r ProfileRecord
	var fetchedUnix int64
	err := s.rdb.QueryRowContext(ctx,
		`SELECT address, name, pseudonym, fetched_at FROM profiles WHERE address = ?`,
		address,
	).Scan(&r.Address, &r.Name, &r.Pseudonym, &fetchedUnix)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.FetchedAt = time.Unix(fetchedUnix, 0)
	return &r, nil
}

// GetMulti returns profiles for multiple addresses.
func (s *ProfileStore) GetMulti(ctx context.Context, addresses []string) (map[string]*ProfileRecord, error) {
	if len(addresses) == 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(addresses))
	placeholders = placeholders[:len(placeholders)-1]

	q := `SELECT address, name, pseudonym, fetched_at FROM profiles WHERE address IN (` + placeholders + `)`
	args := make([]any, len(addresses))
	for i, a := range addresses {
		args[i] = a
	}

	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[string]*ProfileRecord, len(addresses))
	for rows.Next() {
		var r ProfileRecord
		var fetchedUnix int64
		if err := rows.Scan(&r.Address, &r.Name, &r.Pseudonym, &fetchedUnix); err != nil {
			return nil, err
		}
		r.FetchedAt = time.Unix(fetchedUnix, 0)
		m[r.Address] = &r
	}
	return m, rows.Err()
}

// Count returns the total number of cached profiles.
func (s *ProfileStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM profiles`).Scan(&n)
	return n, err
}

// DeleteOlderThan removes profiles not refreshed since cutoff.
func (s *ProfileStore) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM profiles WHERE fetched_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
