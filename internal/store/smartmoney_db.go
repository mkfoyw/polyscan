package store

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// SmartMoneyDB wraps a separate SQLite database for smart money tracking.
// It is independent from the main polyscan.db so they can be managed separately.
type SmartMoneyDB struct {
	db     *DB // reuses the same reader/writer split pattern
	logger *slog.Logger
}

// NewSmartMoneyDB opens an independent SQLite database for smart money data.
func NewSmartMoneyDB(_ context.Context, path string, logger *slog.Logger) (*SmartMoneyDB, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create smartmoney data dir %s: %w", dir, err)
		}
	}

	writerDB, err := openPool(path, 1)
	if err != nil {
		return nil, fmt.Errorf("smartmoney sqlite writer open: %w", err)
	}

	readerDB, err := openPool(path, 4)
	if err != nil {
		writerDB.Close()
		return nil, fmt.Errorf("smartmoney sqlite reader open: %w", err)
	}

	logger.Info("SmartMoney SQLite opened", "path", path)
	db := &DB{writerDB: writerDB, readerDB: readerDB, logger: logger}
	return &SmartMoneyDB{db: db, logger: logger}, nil
}

// EnsureSchema creates the smart money tables if they don't exist.
func (s *SmartMoneyDB) EnsureSchema() error {
	schema := `
CREATE TABLE IF NOT EXISTS smart_money_users (
	id                 INTEGER PRIMARY KEY AUTOINCREMENT,
	address            TEXT    NOT NULL UNIQUE,
	alias              TEXT    NOT NULL DEFAULT '',
	name               TEXT    NOT NULL DEFAULT '',
	pseudonym          TEXT    NOT NULL DEFAULT '',
	source             TEXT    NOT NULL DEFAULT 'auto',
	status             TEXT    NOT NULL DEFAULT 'candidate',
	win_rate           REAL    NOT NULL DEFAULT 0,
	roi                REAL    NOT NULL DEFAULT 0,
	total_trades       INTEGER NOT NULL DEFAULT 0,
	winning_trades     INTEGER NOT NULL DEFAULT 0,
	total_volume       REAL    NOT NULL DEFAULT 0,
	last_poll_ts       INTEGER NOT NULL DEFAULT 0,
	profile_fetched_at INTEGER NOT NULL DEFAULT 0,
	analyzed_at        INTEGER NOT NULL DEFAULT 0,
	first_seen_at      INTEGER NOT NULL DEFAULT 0,
	updated_at         INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sm_users_status ON smart_money_users(status);

CREATE TABLE IF NOT EXISTS smart_money_trades (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	proxy_wallet     TEXT    NOT NULL DEFAULT '',
	side             TEXT    NOT NULL DEFAULT '',
	asset            TEXT    NOT NULL DEFAULT '',
	condition_id     TEXT    NOT NULL DEFAULT '',
	size             REAL    NOT NULL DEFAULT 0,
	price            REAL    NOT NULL DEFAULT 0,
	usd_value        REAL    NOT NULL DEFAULT 0,
	timestamp        INTEGER NOT NULL DEFAULT 0,
	title            TEXT    NOT NULL DEFAULT '',
	slug             TEXT    NOT NULL DEFAULT '',
	event_slug       TEXT    NOT NULL DEFAULT '',
	outcome          TEXT    NOT NULL DEFAULT '',
	transaction_hash TEXT    NOT NULL DEFAULT '',
	created_at       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sm_trades_timestamp    ON smart_money_trades(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sm_trades_wallet_ts    ON smart_money_trades(proxy_wallet, timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sm_trades_txhash ON smart_money_trades(transaction_hash) WHERE transaction_hash != '';
`
	if _, err := s.db.writerDB.Exec(schema); err != nil {
		return fmt.Errorf("ensure smartmoney schema: %w", err)
	}
	s.logger.Info("SmartMoney SQLite schema ensured")
	return nil
}

// Close closes the smart money database connections.
func (s *SmartMoneyDB) Close() error {
	return s.db.Close()
}

// Users returns a SmartMoneyUserStore backed by this database.
func (s *SmartMoneyDB) Users() *SmartMoneyUserStore {
	return &SmartMoneyUserStore{rdb: s.db.readerDB, wdb: s.db.writerDB}
}

// Trades returns a SmartMoneyTradeStore backed by this database.
func (s *SmartMoneyDB) Trades() *SmartMoneyTradeStore {
	return &SmartMoneyTradeStore{rdb: s.db.readerDB, wdb: s.db.writerDB}
}
