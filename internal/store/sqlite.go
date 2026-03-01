 package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// DB wraps a sql.DB and provides access to typed stores.
type DB struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewDB opens a SQLite database and returns a store handle.
// It automatically creates the parent directory if it does not exist.
func NewDB(_ context.Context, path string, logger *slog.Logger) (*DB, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create data dir %s: %w", dir, err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}

	// Optimise for concurrent use.
	db.SetMaxOpenConns(1) // SQLite serialises writes; one conn avoids BUSY.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("sqlite WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return nil, fmt.Errorf("sqlite busy_timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return nil, fmt.Errorf("sqlite foreign_keys: %w", err)
	}

	logger.Info("SQLite opened", "path", path)
	return &DB{db: db, logger: logger}, nil
}

// Close closes the SQLite database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

// EnsureSchema creates all tables & indexes if they do not exist.
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

CREATE TABLE IF NOT EXISTS whales (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	address       TEXT    NOT NULL UNIQUE,
	alias         TEXT    NOT NULL DEFAULT '',
	source        TEXT    NOT NULL DEFAULT '',
	first_seen_at INTEGER NOT NULL DEFAULT 0,
	total_volume  REAL    NOT NULL DEFAULT 0,
	last_poll_ts  INTEGER NOT NULL DEFAULT 0,
	updated_at    INTEGER NOT NULL DEFAULT 0
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
`
	if _, err := d.db.Exec(schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	d.logger.Info("SQLite schema ensured")
	return nil
}

// Trades returns a TradeStore backed by this database.
func (d *DB) Trades() *TradeStore { return &TradeStore{db: d.db} }

// Alerts returns an AlertStore backed by this database.
func (d *DB) Alerts() *AlertStore { return &AlertStore{db: d.db} }

// Whales returns a WhaleStore backed by this database.
func (d *DB) Whales() *WhaleStore { return &WhaleStore{db: d.db} }

// PriceEvents returns a PriceEventStore backed by this database.
func (d *DB) PriceEvents() *PriceEventStore { return &PriceEventStore{db: d.db} }

// Settlements returns a SettlementStore backed by this database.
func (d *DB) Settlements() *SettlementStore { return &SettlementStore{db: d.db} }
