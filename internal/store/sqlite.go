 package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

// DB wraps separate reader/writer sql.DB pools and provides access to typed stores.
// WAL mode allows concurrent readers with a single writer, so we open two pools:
//   - writerDB: MaxOpenConns(1) — serialises all INSERT/UPDATE/DELETE
//   - readerDB: MaxOpenConns(4) — allows parallel SELECT from API handlers
type DB struct {
	writerDB *sql.DB
	readerDB *sql.DB
	logger   *slog.Logger
}

// openPool opens one sql.DB pool with common pragmas.
func openPool(dsn string, maxConn int) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConn)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	return db, nil
}

// NewDB opens a SQLite database with separate reader/writer pools and returns a store handle.
// It automatically creates the parent directory if it does not exist.
func NewDB(_ context.Context, path string, logger *slog.Logger) (*DB, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create data dir %s: %w", dir, err)
		}
	}

	writerDB, err := openPool(path, 1)
	if err != nil {
		return nil, fmt.Errorf("sqlite writer open: %w", err)
	}

	readerDB, err := openPool(path, 4)
	if err != nil {
		writerDB.Close()
		return nil, fmt.Errorf("sqlite reader open: %w", err)
	}

	logger.Info("SQLite opened (read/write split)", "path", path, "reader_conns", 4, "writer_conns", 1)
	return &DB{writerDB: writerDB, readerDB: readerDB, logger: logger}, nil
}

// Close closes both SQLite database connections.
func (d *DB) Close() error {
	rErr := d.readerDB.Close()
	wErr := d.writerDB.Close()
	if wErr != nil {
		return wErr
	}
	return rErr
}

// EnsureSchema creates all tables & indexes if they do not exist.
// Uses the writer connection for DDL statements.
func (d *DB) EnsureSchema() error {
	schema := `
CREATE TABLE IF NOT EXISTS trades (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	proxy_wallet     TEXT    NOT NULL DEFAULT '',
	pseudonym        TEXT    NOT NULL DEFAULT '',
	profile_name     TEXT    NOT NULL DEFAULT '',
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
	source           TEXT    NOT NULL DEFAULT '',
	created_at       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp      ON trades(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts      ON trades(proxy_wallet, timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_txhash  ON trades(transaction_hash) WHERE transaction_hash != '';
CREATE INDEX IF NOT EXISTS idx_trades_condition_id   ON trades(condition_id);

CREATE TABLE IF NOT EXISTS alerts (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	type       TEXT    NOT NULL DEFAULT '',
	message    TEXT    NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_alerts_created      ON alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_type_created ON alerts(type, created_at DESC);

CREATE TABLE IF NOT EXISTS whale_users (
	id                 INTEGER PRIMARY KEY AUTOINCREMENT,
	address            TEXT    NOT NULL UNIQUE,
	alias              TEXT    NOT NULL DEFAULT '',
	name               TEXT    NOT NULL DEFAULT '',
	pseudonym          TEXT    NOT NULL DEFAULT '',
	source             TEXT    NOT NULL DEFAULT '',
	first_seen_at      INTEGER NOT NULL DEFAULT 0,
	total_volume       REAL    NOT NULL DEFAULT 0,
	last_poll_ts       INTEGER NOT NULL DEFAULT 0,
	profile_fetched_at INTEGER NOT NULL DEFAULT 0,
	updated_at         INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS price_events (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	asset_id    TEXT    NOT NULL DEFAULT '',
	market      TEXT    NOT NULL DEFAULT '',
	question    TEXT    NOT NULL DEFAULT '',
	outcome     TEXT    NOT NULL DEFAULT '',
	window      TEXT    NOT NULL DEFAULT '',
	pct_change  REAL    NOT NULL DEFAULT 0,
	old_price   REAL    NOT NULL DEFAULT 0,
	new_price   REAL    NOT NULL DEFAULT 0,
	detected_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_price_events_asset_detected ON price_events(asset_id, detected_at DESC);

CREATE TABLE IF NOT EXISTS settlements (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	condition_id TEXT    NOT NULL UNIQUE,
	question     TEXT    NOT NULL DEFAULT '',
	slug         TEXT    NOT NULL DEFAULT '',
	event_slug   TEXT    NOT NULL DEFAULT '',
	outcome      TEXT    NOT NULL DEFAULT '',
	image_url    TEXT    NOT NULL DEFAULT '',
	resolved_at  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_settlements_resolved ON settlements(resolved_at DESC);

CREATE TABLE IF NOT EXISTS whale_trades (
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
CREATE INDEX IF NOT EXISTS idx_whale_trades_timestamp    ON whale_trades(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_whale_trades_wallet_ts    ON whale_trades(proxy_wallet, timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_whale_trades_txhash ON whale_trades(transaction_hash) WHERE transaction_hash != '';

CREATE TABLE IF NOT EXISTS profiles (
	address    TEXT PRIMARY KEY,
	name       TEXT NOT NULL DEFAULT '',
	pseudonym  TEXT NOT NULL DEFAULT '',
	fetched_at INTEGER NOT NULL DEFAULT 0
);
`
	if _, err := d.writerDB.Exec(schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	// Migrate data from old 'whales' table to 'whale_users' if the old table exists.
	var hasOldTable int
	_ = d.writerDB.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='whales'`).Scan(&hasOldTable)
	if hasOldTable == 1 {
		_, _ = d.writerDB.Exec(`
			INSERT OR IGNORE INTO whale_users (address, alias, source, first_seen_at, total_volume, last_poll_ts, updated_at)
			SELECT address, alias, source, first_seen_at, total_volume, last_poll_ts, updated_at FROM whales`)
		_, _ = d.writerDB.Exec(`DROP TABLE whales`)
		d.logger.Info("migrated whales -> whale_users")
	}

	d.logger.Info("SQLite schema ensured")
	return nil
}

// Trades returns a TradeStore backed by this database.
func (d *DB) Trades() *TradeStore { return &TradeStore{rdb: d.readerDB, wdb: d.writerDB} }

// Alerts returns an AlertStore backed by this database.
func (d *DB) Alerts() *AlertStore { return &AlertStore{rdb: d.readerDB, wdb: d.writerDB} }

// Whales returns a WhaleStore backed by this database.
func (d *DB) Whales() *WhaleStore { return &WhaleStore{rdb: d.readerDB, wdb: d.writerDB} }

// PriceEvents returns a PriceEventStore backed by this database.
func (d *DB) PriceEvents() *PriceEventStore { return &PriceEventStore{rdb: d.readerDB, wdb: d.writerDB} }

// Settlements returns a SettlementStore backed by this database.
func (d *DB) Settlements() *SettlementStore { return &SettlementStore{rdb: d.readerDB, wdb: d.writerDB} }

// WhaleTrades returns a WhaleTradStore backed by this database.
func (d *DB) WhaleTrades() *WhaleTradStore { return &WhaleTradStore{rdb: d.readerDB, wdb: d.writerDB} }

// Profiles returns a ProfileStore backed by this database.
func (d *DB) Profiles() *ProfileStore { return &ProfileStore{rdb: d.readerDB, wdb: d.writerDB} }

// isUniqueViolation checks whether the error is a SQLite UNIQUE constraint failure.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}
