package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	LogLevel string `yaml:"log_level"` // "debug", "info", "warn", "error"

	// HTTP/HTTPS/SOCKS5 proxy URL (e.g. http://127.0.0.1:7897 or socks5://127.0.0.1:7897)
	Proxy string `yaml:"proxy"`

	// SQLite database file path
	SQLitePath string `yaml:"sqlite_path"`

	// Large trade detection
	LargeTradeThreshold float64 `yaml:"large_trade_threshold"` // USD threshold
	LargeTradeMaxPrice  float64 `yaml:"large_trade_max_price"`  // Only alert when price < this (0 = no filter)

	// Price spike rules
	PriceSpikeRules []PriceSpikeRule `yaml:"price_spike_rules"`

	// Polling intervals
	MarketSyncInterval Duration `yaml:"market_sync_interval"`
	TradePollInterval  Duration `yaml:"trade_poll_interval"`

	// Telegram
	Telegram TelegramConfig `yaml:"telegram"`

	// Whale tracking
	Whale WhaleConfig `yaml:"whale"`

	// HTTP API server
	API APIConfig `yaml:"api"`

	// Data retention — per-collection pruning. 0 means no pruning.
	Retention RetentionConfig `yaml:"retention"`
}

// RetentionConfig holds per-collection retention settings (in days).
type RetentionConfig struct {
	TradesDays      int `yaml:"trades_days"`       // trades & alerts
	MarketsDays     int `yaml:"markets_days"`      // new markets (in-memory) & settlements
	PriceEventsDays int `yaml:"price_events_days"` // price spike events
}

// PriceSpikeRule defines a single price spike detection rule.
type PriceSpikeRule struct {
	Window  Duration `yaml:"window"`  // time window, e.g. "5m"
	Percent float64  `yaml:"percent"` // percentage threshold, e.g. 5.0
}

// TelegramConfig holds Telegram bot settings.
type TelegramConfig struct {
	BotToken string `yaml:"bot_token"`
	ChatID   string `yaml:"chat_id"`
}

// WhaleConfig holds whale tracking settings.
type WhaleConfig struct {
	MinTradeAmount float64  `yaml:"min_trade_amount"` // min single-trade USD to trigger auto-track (0 = use large_trade_threshold)
	MaxBuyPrice    float64  `yaml:"max_buy_price"`    // only auto-track buys with price <= this (0 = no filter)
	MaxTracked     int      `yaml:"max_tracked"`      // max tracked wallets
	PollInterval   Duration `yaml:"poll_interval"`    // per-whale poll interval
	Cooldown       Duration `yaml:"cooldown"`         // cooldown between alerts for same whale
}

// APIConfig holds HTTP API server settings.
type APIConfig struct {
	Enabled     bool     `yaml:"enabled"`
	Addr        string   `yaml:"addr"`         // e.g. ":8080"
	AdminTokens []string `yaml:"admin_tokens"` // tokens for write operations (empty = disabled)
}

// Duration is a time.Duration that can be unmarshaled from a YAML string like "5m".
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// DefaultConfig returns a config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		LogLevel:   "info",
		SQLitePath: "data/polyscan.db",
		LargeTradeThreshold: 5000,
		PriceSpikeRules: []PriceSpikeRule{
			{Window: Duration{5 * time.Minute}, Percent: 5},
			{Window: Duration{10 * time.Minute}, Percent: 10},
			{Window: Duration{1 * time.Hour}, Percent: 15},
		},
		MarketSyncInterval: Duration{5 * time.Minute},
		TradePollInterval:  Duration{30 * time.Second},
		Telegram: TelegramConfig{},
		Whale: WhaleConfig{
			MaxTracked:   200,
			PollInterval: Duration{30 * time.Second},
			Cooldown:     Duration{5 * time.Minute},
		},
		API: APIConfig{
			Enabled: true,
			Addr:    ":8080",
		},
	}
}

// Load reads a YAML config file and returns the parsed Config.
// Missing fields use default values.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return cfg, nil
}
