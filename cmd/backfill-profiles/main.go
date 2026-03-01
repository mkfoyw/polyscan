// backfill-profiles is a one-time migration tool that populates
// the profile_name field for existing trades in SQLite.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mkfoyw/polyscan/internal/profile"
	_ "modernc.org/sqlite"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := "polyscan.db"
	proxy := "http://127.0.0.1:7890"
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}
	if len(os.Args) > 2 {
		proxy = os.Args[2]
	}

	// Set proxy env vars so http.DefaultTransport picks them up
	if proxy != "" {
		for _, env := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} {
			os.Setenv(env, proxy)
		}
		logger.Info("proxy configured", "url", proxy)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Open SQLite database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sqlite open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// Find distinct proxy_wallet values where profile_name is missing
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT proxy_wallet FROM trades
		WHERE proxy_wallet != '' AND (profile_name IS NULL OR profile_name = '')`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query wallets: %v\n", err)
		os.Exit(1)
	}

	var wallets []string
	for rows.Next() {
		var w string
		if err := rows.Scan(&w); err != nil {
			fmt.Fprintf(os.Stderr, "scan wallet: %v\n", err)
			os.Exit(1)
		}
		wallets = append(wallets, w)
	}
	rows.Close()

	logger.Info("found wallets to backfill", "count", len(wallets))

	profileClient := profile.NewClient(logger)
	updated := 0
	skipped := 0
	failed := 0

	for i, wallet := range wallets {
		if wallet == "" {
			continue
		}

		name := profileClient.Lookup(ctx, wallet)
		if name == "" {
			skipped++
			continue
		}

		// Update all trades for this wallet that are missing profile_name
		result, err := db.ExecContext(ctx, `
			UPDATE trades SET profile_name = ?
			WHERE proxy_wallet = ? AND (profile_name IS NULL OR profile_name = '')`,
			name, wallet)
		if err != nil {
			logger.Error("update failed", "wallet", wallet, "error", err)
			failed++
			continue
		}
		n, _ := result.RowsAffected()
		updated += int(n)

		if (i+1)%10 == 0 || i == len(wallets)-1 {
			logger.Info("progress", "processed", i+1, "total", len(wallets), "updated_trades", updated, "skipped", skipped)
		}

		// Rate limit: ~2 requests per second to be polite
		time.Sleep(500 * time.Millisecond)
	}

	logger.Info("backfill complete",
		"wallets", len(wallets),
		"updated_trades", updated,
		"skipped_wallets", skipped,
		"failed_wallets", failed,
	)
}
