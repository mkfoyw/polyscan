// migrate-mongo2sqlite reads all data from MongoDB (polyscan database)
// and writes it into a SQLite database file.
//
// Usage:
//
//	go run ./cmd/migrate-mongo2sqlite [--mongo URI] [--db NAME] [--sqlite PATH]
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	_ "modernc.org/sqlite"
)

func main() {
	mongoURI := flag.String("mongo", "mongodb://localhost:27017/", "MongoDB connection URI")
	mongoDB := flag.String("db", "polyscan", "MongoDB database name")
	sqlitePath := flag.String("sqlite", "polyscan.db", "SQLite output file path")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// ---- Connect to MongoDB ----
	client, err := mongo.Connect(options.Client().ApplyURI(*mongoURI))
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Disconnect(ctx)

	mdb := client.Database(*mongoDB)
	logger.Info("connected to MongoDB", "uri", *mongoURI, "db", *mongoDB)

	// ---- Open SQLite ----
	db, err := sql.Open("sqlite", *sqlitePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sqlite open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	mustExec(db, "PRAGMA journal_mode=WAL")
	mustExec(db, "PRAGMA busy_timeout=5000")
	mustExec(db, "PRAGMA synchronous=NORMAL")

	// Ensure schema
	mustExec(db, schema)
	logger.Info("SQLite schema created", "path", *sqlitePath)

	// ---- Migrate each collection ----
	migrateTrades(ctx, logger, mdb, db)
	migrateAlerts(ctx, logger, mdb, db)
	migrateWhales(ctx, logger, mdb, db)
	migratePriceEvents(ctx, logger, mdb, db)
	migrateSettlements(ctx, logger, mdb, db)

	logger.Info("migration complete ✓")
}

// ---------------------------------------------------------------------------
// trades
// ---------------------------------------------------------------------------

func migrateTrades(ctx context.Context, logger *slog.Logger, mdb *mongo.Database, db *sql.DB) {
	col := mdb.Collection("trades")
	cursor, err := col.Find(ctx, bson.D{})
	if err != nil {
		logger.Error("trades find", "error", err)
		return
	}
	defer cursor.Close(ctx)

	tx, _ := db.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO trades
		(proxy_wallet, pseudonym, profile_name, side, asset, condition_id,
		 size, price, usd_value, timestamp, title, slug, event_slug, outcome,
		 transaction_hash, source, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)

	var n int
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			logger.Error("decode trade", "error", err)
			continue
		}
		stmt.ExecContext(ctx,
			str(doc, "proxy_wallet"),
			str(doc, "pseudonym"),
			str(doc, "profile_name"),
			str(doc, "side"),
			str(doc, "asset"),
			str(doc, "condition_id"),
			num(doc, "size"),
			num(doc, "price"),
			num(doc, "usd_value"),
			i64(doc, "timestamp"),
			str(doc, "title"),
			str(doc, "slug"),
			str(doc, "event_slug"),
			str(doc, "outcome"),
			str(doc, "transaction_hash"),
			str(doc, "source"),
			ts(doc, "created_at"),
		)
		n++
		if n%5000 == 0 {
			logger.Info("trades progress", "rows", n)
		}
	}
	stmt.Close()
	tx.Commit()
	logger.Info("trades migrated", "total", n)
}

// ---------------------------------------------------------------------------
// alerts
// ---------------------------------------------------------------------------

func migrateAlerts(ctx context.Context, logger *slog.Logger, mdb *mongo.Database, db *sql.DB) {
	col := mdb.Collection("alerts")
	cursor, err := col.Find(ctx, bson.D{})
	if err != nil {
		logger.Error("alerts find", "error", err)
		return
	}
	defer cursor.Close(ctx)

	tx, _ := db.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, `
		INSERT INTO alerts (type, message, created_at) VALUES (?,?,?)`)

	var n int
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		stmt.ExecContext(ctx,
			str(doc, "type"),
			str(doc, "message"),
			ts(doc, "created_at"),
		)
		n++
	}
	stmt.Close()
	tx.Commit()
	logger.Info("alerts migrated", "total", n)
}

// ---------------------------------------------------------------------------
// whales
// ---------------------------------------------------------------------------

func migrateWhales(ctx context.Context, logger *slog.Logger, mdb *mongo.Database, db *sql.DB) {
	col := mdb.Collection("whales")
	cursor, err := col.Find(ctx, bson.D{})
	if err != nil {
		logger.Error("whales find", "error", err)
		return
	}
	defer cursor.Close(ctx)

	tx, _ := db.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO whales
		(address, alias, source, first_seen_at, total_volume, last_poll_ts, updated_at)
		VALUES (?,?,?,?,?,?,?)`)

	var n int
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		stmt.ExecContext(ctx,
			str(doc, "address"),
			str(doc, "alias"),
			str(doc, "source"),
			ts(doc, "first_seen_at"),
			num(doc, "total_volume"),
			i64(doc, "last_poll_ts"),
			ts(doc, "updated_at"),
		)
		n++
	}
	stmt.Close()
	tx.Commit()
	logger.Info("whales migrated", "total", n)
}

// ---------------------------------------------------------------------------
// price_events
// ---------------------------------------------------------------------------

func migratePriceEvents(ctx context.Context, logger *slog.Logger, mdb *mongo.Database, db *sql.DB) {
	col := mdb.Collection("price_events")
	cursor, err := col.Find(ctx, bson.D{})
	if err != nil {
		logger.Error("price_events find", "error", err)
		return
	}
	defer cursor.Close(ctx)

	tx, _ := db.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, `
		INSERT INTO price_events
		(asset_id, market, question, outcome, window, pct_change, old_price, new_price, detected_at)
		VALUES (?,?,?,?,?,?,?,?,?)`)

	var n int
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		stmt.ExecContext(ctx,
			str(doc, "asset_id"),
			str(doc, "market"),
			str(doc, "question"),
			str(doc, "outcome"),
			str(doc, "window"),
			num(doc, "pct_change"),
			num(doc, "old_price"),
			num(doc, "new_price"),
			ts(doc, "detected_at"),
		)
		n++
	}
	stmt.Close()
	tx.Commit()
	logger.Info("price_events migrated", "total", n)
}

// ---------------------------------------------------------------------------
// settlements
// ---------------------------------------------------------------------------

func migrateSettlements(ctx context.Context, logger *slog.Logger, mdb *mongo.Database, db *sql.DB) {
	col := mdb.Collection("settlements")
	cursor, err := col.Find(ctx, bson.D{})
	if err != nil {
		logger.Error("settlements find", "error", err)
		return
	}
	defer cursor.Close(ctx)

	tx, _ := db.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO settlements
		(condition_id, question, slug, event_slug, outcome, image_url, resolved_at)
		VALUES (?,?,?,?,?,?,?)`)

	var n int
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		stmt.ExecContext(ctx,
			str(doc, "condition_id"),
			str(doc, "question"),
			str(doc, "slug"),
			str(doc, "event_slug"),
			str(doc, "outcome"),
			str(doc, "image_url"),
			ts(doc, "resolved_at"),
		)
		n++
	}
	stmt.Close()
	tx.Commit()
	logger.Info("settlements migrated", "total", n)
}

// ---------------------------------------------------------------------------
// helpers — extract typed values from bson.M safely
// ---------------------------------------------------------------------------

func str(doc bson.M, key string) string {
	v, ok := doc[key]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func num(doc bson.M, key string) float64 {
	v, ok := doc[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}

func i64(doc bson.M, key string) int64 {
	v, ok := doc[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// ts extracts a time field from a bson.M and returns Unix seconds.
// MongoDB driver v2 decodes Date fields as bson.DateTime (milliseconds since epoch).
func ts(doc bson.M, key string) int64 {
	v, ok := doc[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case bson.DateTime:
		// bson.DateTime is milliseconds since epoch
		return time.UnixMilli(int64(t)).Unix()
	case time.Time:
		return t.Unix()
	case int64:
		return t
	case int32:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func mustExec(db *sql.DB, q string) {
	if _, err := db.Exec(q); err != nil {
		fmt.Fprintf(os.Stderr, "exec: %v\nSQL: %s\n", err, q)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// SQLite schema (duplicated from store/sqlite.go for standalone use)
// ---------------------------------------------------------------------------

const schema = `
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
