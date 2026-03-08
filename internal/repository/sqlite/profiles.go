package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// ProfileRepo implements repository.ProfileRepository using SQLite.
type ProfileRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

func (s *ProfileRepo) Upsert(ctx context.Context, rec *repository.ProfileRecord) error {
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

func (s *ProfileRepo) Get(ctx context.Context, address string) (*repository.ProfileRecord, error) {
	var r repository.ProfileRecord
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

func (s *ProfileRepo) GetMulti(ctx context.Context, addresses []string) (map[string]*repository.ProfileRecord, error) {
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

	m := make(map[string]*repository.ProfileRecord, len(addresses))
	for rows.Next() {
		var r repository.ProfileRecord
		var fetchedUnix int64
		if err := rows.Scan(&r.Address, &r.Name, &r.Pseudonym, &fetchedUnix); err != nil {
			return nil, err
		}
		r.FetchedAt = time.Unix(fetchedUnix, 0)
		m[r.Address] = &r
	}
	return m, rows.Err()
}

func (s *ProfileRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM profiles`).Scan(&n)
	return n, err
}

func (s *ProfileRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM profiles WHERE fetched_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
