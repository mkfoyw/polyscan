package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// WhaleRepo implements repository.WhaleRepository using SQLite.
type WhaleRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

const whaleUserCols = `address, alias, name, pseudonym, source, first_seen_at, total_volume, last_poll_ts, profile_fetched_at, updated_at`

func (s *WhaleRepo) Upsert(ctx context.Context, rec *repository.WhaleRecord) (bool, error) {
	rec.UpdatedAt = time.Now()

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

func (s *WhaleRepo) GetAll(ctx context.Context) ([]repository.WhaleRecord, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT `+whaleUserCols+` FROM whale_users`)
	if err != nil {
		return nil, err
	}
	return scanWhales(rows)
}

func (s *WhaleRepo) GetByAddress(ctx context.Context, address string) (*repository.WhaleRecord, error) {
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

func (s *WhaleRepo) UpdateLastPollTS(ctx context.Context, address string, ts int64) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE whale_users SET last_poll_ts = ?, updated_at = ? WHERE address = ?`,
		ts, time.Now().Unix(), address)
	return err
}

func (s *WhaleRepo) IncrVolume(ctx context.Context, address string, amount float64) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE whale_users SET total_volume = total_volume + ?, updated_at = ? WHERE address = ?`,
		amount, time.Now().Unix(), address)
	return err
}

func (s *WhaleRepo) UpdateProfile(ctx context.Context, address, name, pseudonym string) error {
	now := time.Now().Unix()
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE whale_users SET name = ?, pseudonym = ?, profile_fetched_at = ?, updated_at = ? WHERE address = ?`,
		name, pseudonym, now, now, address)
	return err
}

func (s *WhaleRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM whale_users`).Scan(&n)
	return n, err
}

func (s *WhaleRepo) DeleteByAddress(ctx context.Context, address string) error {
	_, err := s.wdb.ExecContext(ctx, `DELETE FROM whale_users WHERE address = ?`, address)
	return err
}

func (s *WhaleRepo) DeleteLowestVolume(ctx context.Context) error {
	_, err := s.wdb.ExecContext(ctx, `
		DELETE FROM whale_users WHERE id = (
			SELECT id FROM whale_users WHERE source = 'auto'
			ORDER BY total_volume ASC LIMIT 1
		)`)
	return err
}

func (s *WhaleRepo) Exists(ctx context.Context, address string) (bool, error) {
	var n int
	err := s.rdb.QueryRowContext(ctx,
		`SELECT 1 FROM whale_users WHERE address = ? LIMIT 1`, address).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func scanWhale(sc interface{ Scan(...any) error }) (repository.WhaleRecord, error) {
	var r repository.WhaleRecord
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

func scanWhales(rows *sql.Rows) ([]repository.WhaleRecord, error) {
	defer rows.Close()
	var out []repository.WhaleRecord
	for rows.Next() {
		r, err := scanWhale(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
