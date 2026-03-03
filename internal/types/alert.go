package types

import "fmt"

// AlertType identifies the kind of alert.
type AlertType int

const (
	AlertLargeTrade AlertType = iota
	AlertPriceSpike
	AlertWhaleActivity
	AlertNewWhaleTracked
	AlertSmartMoney
)

func (a AlertType) String() string {
	switch a {
	case AlertLargeTrade:
		return "LargeTrade"
	case AlertPriceSpike:
		return "PriceSpike"
	case AlertWhaleActivity:
		return "WhaleActivity"
	case AlertNewWhaleTracked:
		return "NewWhaleTracked"
	case AlertSmartMoney:
		return "SmartMoney"
	default:
		return "Unknown"
	}
}

// Alert is a notification event to be sent via Telegram.
type Alert struct {
	Type    AlertType
	Message string // Pre-formatted message for Telegram (Markdown)
}

// FormatLargeTradeAlert formats a large trade alert message.
func FormatLargeTradeAlert(question, side, outcome string, usdValue, size, price float64, wallet, url string) Alert {
	walletDisplay := formatWalletLink(wallet)
	msg := fmt.Sprintf(
		"🐳 *大额交易*\n"+
			"📋 %s\n"+
			"💰 *%s %s* — $%.0f (%.1f @ %.4f)\n"+
			"👤 %s\n"+
			"🔗 [查看市场](%s)",
		escapeMarkdown(question),
		side, outcome,
		usdValue, size, price,
		walletDisplay,
		url,
	)
	return Alert{Type: AlertLargeTrade, Message: msg}
}

// FormatPriceSpikeAlert formats a price spike alert message.
func FormatPriceSpikeAlert(question, outcome, window string, pctChange, oldPrice, newPrice float64, url string) Alert {
	msg := fmt.Sprintf(
		"📈 *价格飙升*\n"+
			"📋 %s\n"+
			"📊 *%s* 在 %s 内上涨 *%.1f%%* (%.4f → %.4f)\n"+
			"🔗 [查看市场](%s)",
		escapeMarkdown(question),
		outcome, window,
		pctChange, oldPrice, newPrice,
		url,
	)
	return Alert{Type: AlertPriceSpike, Message: msg}
}

// FormatWhaleActivityAlert formats a whale activity alert message.
func FormatWhaleActivityAlert(alias, side, outcome string, usdcSize, size, price float64, question, url string) Alert {
	// alias may be a 0x address or a human-readable name
	aliasDisplay := formatWalletLink(alias)
	msg := fmt.Sprintf(
		"🐋 *鲸鱼动态*\n"+
			"👤 %s\n"+
			"💰 *%s %s* — $%.0f (%.1f @ %.4f)\n"+
			"📋 %s\n"+
			"🔗 [查看市场](%s)",
		aliasDisplay,
		side, outcome,
		usdcSize, size, price,
		escapeMarkdown(question),
		url,
	)
	return Alert{Type: AlertWhaleActivity, Message: msg}
}

// FormatNewWhaleAlert formats a new whale tracked alert.
func FormatNewWhaleAlert(wallet, reason string) Alert {
	msg := fmt.Sprintf(
		"🆕 *新增鲸鱼追踪*\n"+
			"👤 %s\n"+
			"📝 %s",
		formatWalletLink(wallet),
		escapeMarkdown(reason),
	)
	return Alert{Type: AlertNewWhaleTracked, Message: msg}
}

// formatWalletLink returns a clickable Polymarket profile Markdown link for
// a wallet address, or the original string if it's not an address.
func formatWalletLink(wallet string) string {
	if len(wallet) >= 10 && wallet[:2] == "0x" {
		return fmt.Sprintf("[%s](https://polymarket.com/profile/%s)", wallet, wallet)
	}
	return wallet
}

// escapeMarkdown escapes special characters for Telegram MarkdownV2.
// We use Markdown (v1) mode, so only need to escape limited chars.
func escapeMarkdown(s string) string {
	// For Markdown v1, escape _ * [ ] ( ) ~ ` > # + - = | { } . !
	// We keep it simple: just escape _ and * which are most common in market questions.
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '_', '*', '[', ']', '(', ')', '~', '`':
			result = append(result, '\\')
		}
		result = append(result, s[i])
	}
	return string(result)
}

// FormatSmartMoneyAlert formats a smart money trade alert message.
func FormatSmartMoneyAlert(alias, side, outcome string, usdcSize, size, price float64, question, url string) Alert {
	aliasDisplay := formatWalletLink(alias)
	msg := fmt.Sprintf(
		"🧠 *聪明钱动态*\n"+
			"👤 %s\n"+
			"💰 *%s %s* — $%.0f (%.1f @ %.4f)\n"+
			"📋 %s\n"+
			"🔗 [查看市场](%s)",
		aliasDisplay,
		side, outcome,
		usdcSize, size, price,
		escapeMarkdown(question),
		url,
	)
	return Alert{Type: AlertSmartMoney, Message: msg}
}
