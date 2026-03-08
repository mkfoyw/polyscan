package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// SmartMoneyUserRepo implements repository.SmartMoneyUserRepository using SQLite.
type SmartMoneyUserRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

const smUserCols = `address, alias, name, pseudonym, source, status,
	win_rate, roi, total_trades, winning_trades, total_volume,
	last_poll_ts, profile_fetched_at, analyzed_at, first_seen_at, updated_at`

func (s *SmartMoneyUserRepo) Upsert(ctx context.Context, rec *repository.SmartMoneyUser) error {
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

func (s *SmartMoneyUserRepo) GetAll(ctx context.Context) ([]repository.SmartMoneyUser, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT `+smUserCols+` FROM smart_money_users ORDER BY updated_at DESC`)
	if err != nil {
		return nil, err
	}
	return scanSMUsers(rows)
}

func (s *SmartMoneyUserRepo) GetByStatus(ctx context.Context, status string) ([]repository.SmartMoneyUser, error) {
	rows, err := s.rdb.QueryContext(ctx,
		`SELECT `+smUserCols+` FROM smart_money_users WHERE status = ? ORDER BY updated_at DESC`, status)
	if err != nil {
		return nil, err
	}
	return scanSMUsers(rows)
}

func (s *SmartMoneyUserRepo) GetByAddress(ctx context.Context, address string) (*repository.SmartMoneyUser, error) {
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

func (s *SmartMoneyUserRepo) UpdateAlias(ctx context.Context, address, alias string) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET alias = ?, updated_at = ? WHERE address = ?`,
		alias, time.Now().Unix(), address)
	return err
}

func (s *SmartMoneyUserRepo) UpdateStatus(ctx context.Context, address, status string) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET status = ?, updated_at = ? WHERE address = ?`,
		status, time.Now().Unix(), address)
	return err
}

func (s *SmartMoneyUserRepo) UpdateStats(ctx context.Context, address string, winRate, roi float64, totalTrades, winningTrades int, totalVolume float64) error {
	now := time.Now().Unix()
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET win_rate = ?, roi = ?, total_trades = ?,
			winning_trades = ?, total_volume = ?, analyzed_at = ?, updated_at = ?
		WHERE address = ?`,
		winRate, roi, totalTrades, winningTrades, totalVolume, now, now, address)
	return err
}

func (s *SmartMoneyUserRepo) UpdateProfile(ctx context.Context, address, name, pseudonym string) error {
	now := time.Now().Unix()
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET name = ?, pseudonym = ?, profile_fetched_at = ?, updated_at = ?
		WHERE address = ?`,
		name, pseudonym, now, now, address)
	return err
}

func (s *SmartMoneyUserRepo) UpdateLastPollTS(ctx context.Context, address string, ts int64) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET last_poll_ts = ?, updated_at = ? WHERE address = ?`,
		ts, time.Now().Unix(), address)
	return err
}

func (s *SmartMoneyUserRepo) IncrVolume(ctx context.Context, address string, amount float64) error {
	_, err := s.wdb.ExecContext(ctx,
		`UPDATE smart_money_users SET total_volume = total_volume + ?, updated_at = ? WHERE address = ?`,
		amount, time.Now().Unix(), address)
	return err
}

func (s *SmartMoneyUserRepo) Delete(ctx context.Context, address string) error {
	_, err := s.wdb.ExecContext(ctx, `DELETE FROM smart_money_users WHERE address = ?`, address)
	return err
}

func (s *SmartMoneyUserRepo) Exists(ctx context.Context, address string) (bool, error) {
	var n int
	err := s.rdb.QueryRowContext(ctx,
		`SELECT 1 FROM smart_money_users WHERE address = ? LIMIT 1`, address).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (s *SmartMoneyUserRepo) CountByStatus(ctx context.Context, status string) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM smart_money_users WHERE status = ?`, status).Scan(&n)
	return n, err
}

func (s *SmartMoneyUserRepo) GetCandidates(ctx context.Context) ([]repository.SmartMoneyUser, error) {
	return s.GetByStatus(ctx, repository.SMStatusCandidate)
}

func (s *SmartMoneyUserRepo) GetConfirmed(ctx context.Context) ([]repository.SmartMoneyUser, error) {
	return s.GetByStatus(ctx, repository.SMStatusConfirmed)
}

func scanSMUser(sc interface{ Scan(...any) error }) (repository.SmartMoneyUser, error) {
	var u repository.SmartMoneyUser
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

func scanSMUsers(rows *sql.Rows) ([]repository.SmartMoneyUser, error) {
	defer rows.Close()
	var out []repository.SmartMoneyUser
	for rows.Next() {
		u, err := scanSMUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// SmartMoneyTradeRepo implements repository.SmartMoneyTradeRepository using SQLite.
type SmartMoneyTradeRepo struct {
	rdb *sql.DB
	wdb *sql.DB
}

const smTradeDBCols = `proxy_wallet, side, asset, condition_id,
	size, price, usd_value, timestamp, title, slug, event_slug, outcome,
	transaction_hash, created_at`

func scanSMTrade(sc interface{ Scan(...any) error }) (repository.SmartMoneyTrade, error) {
	var r repository.SmartMoneyTrade
	var createdUnix int64
	err := sc.Scan(
		&r.ProxyWallet, &r.Side, &r.Asset, &r.ConditionID,
		&r.Size, &r.Price, &r.USDValue, &r.Timestamp,
		&r.Title, &r.Slug, &r.EventSlug, &r.Outcome,
		&r.TransactionHash, &createdUnix,
	)
	if err != nil {
		return r, err
	}
	r.CreatedAt = time.Unix(createdUnix, 0)
	r.Source = "smartmoney"
	return r, nil
}

func scanSMTrades(rows *sql.Rows) ([]repository.SmartMoneyTrade, error) {
	defer rows.Close()
	var out []repository.SmartMoneyTrade
	for rows.Next() {
		r, err := scanSMTrade(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SmartMoneyTradeRepo) Insert(ctx context.Context, rec *repository.SmartMoneyTrade) error {
	_, err := s.UpsertMerge(ctx, rec)
	return err
}

func (s *SmartMoneyTradeRepo) UpsertMerge(ctx context.Context, rec *repository.SmartMoneyTrade) (*repository.SmartMoneyTrade, error) {
	rec.CreatedAt = time.Now()

	if rec.TransactionHash != "" {
		var exists int
		err := s.rdb.QueryRowContext(ctx, `SELECT 1 FROM smart_money_trades WHERE transaction_hash = ? LIMIT 1`, rec.TransactionHash).Scan(&exists)
		if err == nil {
			return rec, nil
		}
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var oldSize, oldUSD, oldPrice float64
	var oldTS int64
	var oldTxHash string
	err = tx.QueryRowContext(ctx, `
		SELECT size, usd_value, price, timestamp, transaction_hash
		FROM smart_money_trades
		WHERE proxy_wallet = ? AND condition_id = ? AND side = ? AND outcome = ?
		LIMIT 1`,
		rec.ProxyWallet, rec.ConditionID, rec.Side, rec.Outcome,
	).Scan(&oldSize, &oldUSD, &oldPrice, &oldTS, &oldTxHash)

	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO smart_money_trades (`+smTradeDBCols+`)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			rec.ProxyWallet, rec.Side, rec.Asset, rec.ConditionID,
			rec.Size, rec.Price, rec.USDValue, rec.Timestamp,
			rec.Title, rec.Slug, rec.EventSlug, rec.Outcome,
			rec.TransactionHash, rec.CreatedAt.Unix(),
		)
		if err != nil && isUniqueViolation(err) {
			return rec, nil
		}
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return rec, nil
	}

	newSize := oldSize + rec.Size
	newUSD := oldUSD + rec.USDValue
	newPrice := oldPrice
	if newSize > 0 {
		newPrice = (oldPrice*oldSize + rec.Price*rec.Size) / newSize
	} else if rec.Price > 0 {
		newPrice = rec.Price
	}
	newTS := oldTS
	if rec.Timestamp > oldTS {
		newTS = rec.Timestamp
	}
	newTxHash := oldTxHash
	if rec.TransactionHash != "" {
		newTxHash = rec.TransactionHash
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE smart_money_trades
		SET size = ?, usd_value = ?, price = ?, timestamp = ?,
			asset = ?, title = ?, slug = ?, event_slug = ?,
			outcome = ?, transaction_hash = ?, created_at = ?
		WHERE proxy_wallet = ? AND condition_id = ? AND side = ? AND outcome = ?`,
		newSize, newUSD, newPrice, newTS,
		rec.Asset, rec.Title, rec.Slug, rec.EventSlug,
		rec.Outcome, newTxHash, rec.CreatedAt.Unix(),
		rec.ProxyWallet, rec.ConditionID, rec.Side, rec.Outcome,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	merged := *rec
	merged.Size = newSize
	merged.USDValue = newUSD
	merged.Price = newPrice
	merged.Timestamp = newTS
	merged.TransactionHash = newTxHash
	merged.CreatedAt = time.Unix(rec.CreatedAt.Unix(), 0)
	return &merged, nil
}

func (s *SmartMoneyTradeRepo) Recent(ctx context.Context, wallets []string, limit int64, beforeTS int64, minUSD, maxPrice float64) ([]repository.SmartMoneyTrade, error) {
	q := `SELECT ` + smTradeDBCols + ` FROM smart_money_trades WHERE 1=1`
	var args []any

	if len(wallets) > 0 {
		placeholders := strings.Repeat("?,", len(wallets))
		placeholders = placeholders[:len(placeholders)-1]
		q += ` AND proxy_wallet IN (` + placeholders + `)`
		for _, w := range wallets {
			args = append(args, w)
		}
	}
	if minUSD > 0 {
		q += ` AND usd_value >= ?`
		args = append(args, minUSD)
	}
	if maxPrice > 0 {
		q += ` AND price <= ?`
		args = append(args, maxPrice)
	}
	if beforeTS > 0 {
		q += ` AND timestamp < ?`
		args = append(args, beforeTS)
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanSMTrades(rows)
}

func (s *SmartMoneyTradeRepo) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]repository.SmartMoneyTrade, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT `+smTradeDBCols+` FROM smart_money_trades
		WHERE proxy_wallet = ?
		ORDER BY timestamp DESC LIMIT ?`, wallet, limit)
	if err != nil {
		return nil, err
	}
	return scanSMTrades(rows)
}

func (s *SmartMoneyTradeRepo) RecentSince(ctx context.Context, wallets []string, sinceTS int64) ([]repository.SmartMoneyTrade, error) {
	q := `SELECT ` + smTradeDBCols + ` FROM smart_money_trades WHERE timestamp >= ?`
	args := []any{sinceTS}

	if len(wallets) > 0 {
		placeholders := strings.Repeat("?,", len(wallets))
		placeholders = placeholders[:len(placeholders)-1]
		q += ` AND proxy_wallet IN (` + placeholders + `)`
		for _, w := range wallets {
			args = append(args, w)
		}
	}
	q += ` ORDER BY timestamp DESC`

	rows, err := s.rdb.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanSMTrades(rows)
}

func (s *SmartMoneyTradeRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM smart_money_trades`).Scan(&n)
	return n, err
}

func (s *SmartMoneyTradeRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.wdb.ExecContext(ctx,
		`DELETE FROM smart_money_trades WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
