package notify

import (
	"fmt"
	"log/slog"

	"github.com/gregdel/pushover"
	"github.com/mkfoyw/polyscan/internal/store"
)

// Pushover sends smart money trade notifications via Pushover.
type Pushover struct {
	app    *pushover.Pushover
	user   *pushover.Recipient
	logger *slog.Logger
}

// NewPushover creates a new Pushover notifier.
func NewPushover(appKey, userKey string, logger *slog.Logger) *Pushover {
	return &Pushover{
		app:    pushover.New(appKey),
		user:   pushover.NewRecipient(userKey),
		logger: logger,
	}
}

// SendSmartMoneyTrade sends a notification for a smart money trade.
func (p *Pushover) SendSmartMoneyTrade(t store.SmartMoneyTrade) {
	side := "买入"
	if t.Side == "SELL" {
		side = "卖出"
	}

	name := t.ProfileName
	if name == "" {
		if len(t.ProxyWallet) > 10 {
			name = t.ProxyWallet[:6] + "…" + t.ProxyWallet[len(t.ProxyWallet)-4:]
		} else {
			name = t.ProxyWallet
		}
	}

	title := fmt.Sprintf("🧠 %s %s $%.0f", name, side, t.USDValue)

	priceCents := t.Price * 100
	body := fmt.Sprintf(
		"%s %s\n%s @%.1f¢\n$%.0f (%.1f shares)",
		side, t.Outcome,
		t.Title,
		priceCents,
		t.USDValue, t.Size,
	)

	url := ""
	if t.EventSlug != "" {
		url = "https://polymarket.com/event/" + t.EventSlug
	} else if t.Slug != "" {
		url = "https://polymarket.com/market/" + t.Slug
	}

	msg := pushover.NewMessageWithTitle(body, title)
	msg.Sound = "cashregister"
	if url != "" {
		msg.URL = url
		msg.URLTitle = "查看市场"
	}

	_, err := p.app.SendMessage(msg, p.user)
	if err != nil {
		p.logger.Error("pushover send failed", "error", err, "wallet", t.ProxyWallet)
		return
	}
	p.logger.Debug("pushover notification sent", "wallet", t.ProxyWallet, "usd", t.USDValue)
}
