package store

import (
	"context"
	"database/sql"
	"time"
)

// Smart money user status constants.
const (
	SMStatusCandidate = "candidate"
	SMStatusConfirmed = "confirmed"
	SMStatusRejected  = "rejected"
)

// SmartMoneyUser is a smart money address persisted in the smart money database.
type SmartMoneyUser struct {
	Address          string    `json:"address"`
	Alias            string    `json:"alias,omitempty"`
	Name             string    `json:"name,omitempty"`
	Pseudonym        string    `json:"pseudonym,omitempty"`
	Source           string    `json:"source"`            // "auto" or "manual"
	Status           string    `json:"status"`            // "candidate", "confirmed", "rejected"
	WinRate          float64   `json:"win_rate"`
	ROI              float64   `json:"roi"`
	TotalTrades      int       `json:"total_trades"`
	WinningTrades    int       `json:"winning_trades"`
	TotalVolume      float64   `json:"total_volume"`
	LastPollTS       int64     `json:"last_poll_ts"`
	ProfileFetchedAt time.Time `json:"profile_fetched_at,omitempty"`
	AnalyzedAt       time.Time `json:"analyzed_at,omitempty"`
	FirstSeenAt      time.Time `json:"first_seen_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// DisplayName returns the best display name for this smart money user.
func (u *SmartMoneyUser) DisplayName() string {
	if u.Alias != "" {
		return u.Alias
	}
	if u.Name != "" {
		return u.Name
	}
	if u.Pseudonym != "" {
		return u.Pseudonym
	}
	return u.Address
}

// SmartMoneyUserStore wraps the smart_money_users table.
type SmartMoneyUserStore struct {
	rdb *sql.DB
	wdb *sql.DB
}

const smUserCols = `address, alias, name, pseudonym, source, status,
	win_rate, roi, total_trades, winning_trades, total_volume,
	last_poll_ts, profile_fetched_at, analyzed_at, first_seen_at, updated_at`

// Upsert inserts or updates a smart money user record.
func (s *SmartMoneyUserStore) Upsert(ctx context.Context, rec *SmartMoneyUser) error {
	rec.UpdatedAt = time.Now()
	if rec.FirstSeenAt.IsZero() {
		rec.FirstSeenAt = rec.UpdatedAt
	}
	_, err := s.wdb.ExecContext(ctx, `
		INSERT INTO smart_money_users (address, alias, name, pseudonym, source, status,
			win_rate, roi, total_trades, winning_trades, total_volume,
			last_poll_ts, profile_fetched_at, analyzed_at, first_seen_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(address) DO UPDATE SET
			alias              = excluded.alias,
			name               = excluded.name,
			pseudonym          = excluded.pseudonym,
			source             = excluded.source,
			status             = excluded.status,
			win_rate           = excluded.win_rate,
			roi                = excluded.roi,
			total_trades       = excluded.total_trades,
			winning_trades     = excluded.winning_trades,
			total_volume       = excluded.total_volume,
			last_poll_ts       = excluded.last_poll_ts,
			profile_fetched_at = excluded.profile_fetched_at,
			analyzed_at        = excluded.analyzed_at,
			updated_at         = excluded.updated_at`,
		rec.Address, rec.Alias, rec.Name, rec.Pseudonym, rec.Source, rec.Status,
		rec.WinRate, rec.ROI, rec.TotalTrades, rec.WinningTrades, rec.TotalVolume,
		rec.LastPollTS, rec.ProfileFetchedAt.Unix(), rec.AnalyzedAt.Unix(),
		rec.FirstSeenAt.Unix(), rec.UpdatedAt.Unix(),
	)
	return err
}

// GetAll returns all smart money users regardless of status.
func (s *SmartMoneyUserStore) GetAll(ctx context.Context) ([]SmartMoneyUser, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT `+smUserCols+` FROM smart_money_users ORDER BY updated_at DESC`)
	if err != nil {
		return nil, err
	}
	return scanSMUsers(rows)
}

// GetByStatus returns smart money users with the given status.
func (s *SmartMoneyUserStore) GetByStatus(ctx context.Context, status string) ([]SmartMoneyUser, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT `+smUserCols+` FROM smart_money_users WHERE status = ? ORDER BY updated_at DESC`, status)
	if err != nil {
		return nil, err
	}
	return scanSMUsers(rows)
}

// GetByAddress returns a single smart money user by address.
func (s *SmartMoneyUserStore) GetByAddress(ctx context.Context, address string) (*SmartMoneyUser, error) {
	row := s.rdb.QueryRowContext(ctx,
		`SELECT `+smUserCols+` FROM smart_money_users WHERE address = ?`, address)
	u, err := scanSMUser(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// UpdateAlias updates the alias of a smart money user.
func (s *SmartMoneyUserStore) UpdateAlias(ctx context.Context, address, alias string) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET alias = ?, updated_at = ? WHERE address = ?`,
		alias, time.Now().Unix(), address)
	return err
}

// UpdateStatus updates the status of a smart money user.
func (s *SmartMoneyUserStore) UpdateStatus(ctx context.Context, address, status string) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET status = ?, updated_at = ? WHERE address = ?`,
		status, time.Now().Unix(), address)
	return err
}

// UpdateStats updates the analysis stats for a smart money user.
func (s *SmartMoneyUserStore) UpdateStats(ctx context.Context, address string, winRate, roi float64, totalTrades, winningTrades int, totalVolume float64) error {
	now := time.Now().Unix()
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET win_rate = ?, roi = ?, total_trades = ?,
			winning_trades = ?, total_volume = ?, analyzed_at = ?, updated_at = ?
		WHERE address = ?`,
		winRate, roi, totalTrades, winningTrades, totalVolume, now, now, address)
	return err
}

// UpdateProfile updates the cached profile name/pseudonym.
func (s *SmartMoneyUserStore) UpdateProfile(ctx context.Context, address, name, pseudonym string) error {
	now := time.Now().Unix()
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET name = ?, pseudonym = ?, profile_fetched_at = ?, updated_at = ?
		WHERE address = ?`,
		name, pseudonym, now, now, address)
	return err
}

// UpdateLastPollTS updates the last poll timestamp.
func (s *SmartMoneyUserStore) UpdateLastPollTS(ctx context.Context, address string, ts int64) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET last_poll_ts = ?, updated_at = ? WHERE address = ?`,
		ts, time.Now().Unix(), address)
	return err
}

// IncrVolume atomically increments the total volume.
func (s *SmartMoneyUserStore) IncrVolume(ctx context.Context, address string, amount float64) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET total_volume = total_volume + ?, updated_at = ? WHERE address = ?`,
		amount, time.Now().Unix(), address)
	return err
}

// Delete removes a smart money user by address.
func (s *SmartMoneyUserStore) Delete(ctx context.Context, address string) error {
	_, err := s.wdb.ExecContext(ctx, `DELETE FROM smart_money_users WHERE address = ?`, address)
	return err
}

// Exists checks if a smart money user with this address exists.
func (s *SmartMoneyUserStore) Exists(ctx context.Context, address string) (bool, error) {
	var n int
	err := s.rdb.QueryRowContext(ctx,
		`SELECT 1 FROM smart_money_users WHERE address = ? LIMIT 1`, address).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// CountByStatus returns the count of users with a given status.
func (s *SmartMoneyUserStore) CountByStatus(ctx context.Context, status string) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM smart_money_users WHERE status = ?`, status).Scan(&n)
	return n, err
}

// GetCandidates returns all candidate users for analysis.
func (s *SmartMoneyUserStore) GetCandidates(ctx context.Context) ([]SmartMoneyUser, error) {
	return s.GetByStatus(ctx, SMStatusCandidate)
}

// GetConfirmed returns all confirmed smart money users.
func (s *SmartMoneyUserStore) GetConfirmed(ctx context.Context) ([]SmartMoneyUser, error) {
	return s.GetByStatus(ctx, SMStatusConfirmed)
}

func scanSMUser(sc interface{ Scan(...any) error }) (SmartMoneyUser, error) {
	var u SmartMoneyUser
	var profileFetched, analyzedAt, firstSeen, updated int64
	err := sc.Scan(
		&u.Address, &u.Alias, &u.Name, &u.Pseudonym, &u.Source, &u.Status,
		&u.WinRate, &u.ROI, &u.TotalTrades, &u.WinningTrades, &u.TotalVolume,
		&u.LastPollTS, &profileFetched, &analyzedAt, &firstSeen, &updated,
	)
	if err != nil {
		return u, err
	}
	u.ProfileFetchedAt = time.Unix(profileFetched, 0)
	u.AnalyzedAt = time.Unix(analyzedAt, 0)
	u.FirstSeenAt = time.Unix(firstSeen, 0)
	u.UpdatedAt = time.Unix(updated, 0)
	return u, nil
}

func scanSMUsers(rows *sql.Rows) ([]SmartMoneyUser, error) {
	defer rows.Close()
	var out []SmartMoneyUser
	for rows.Next() {
		u, err := scanSMUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
