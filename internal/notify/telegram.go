package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/mkfoyw/polyscan/internal/types"
	"golang.org/x/time/rate"
)

const maxRetries = 3

const telegramAPI = "https://api.telegram.org"

// Telegram sends alert messages via the Telegram Bot API.
type Telegram struct {
	botToken string
	chatID   string
	client   *http.Client
	limiter  *rate.Limiter
	logger   *slog.Logger
}

// NewTelegram creates a new Telegram notifier.
func NewTelegram(botToken, chatID string, logger *slog.Logger) *Telegram {
	return &Telegram{
		botToken: botToken,
		chatID:   chatID,
		client:   &http.Client{Timeout: 10 * time.Second},
		limiter:  rate.NewLimiter(rate.Every(time.Second), 2), // 1/s with burst 2
		logger:   logger,
	}
}

type telegramSendRequest struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

type telegramResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description,omitempty"`
	Parameters  *struct {
		RetryAfter int `json:"retry_after,omitempty"`
	} `json:"parameters,omitempty"`
}

// Send sends an alert via Telegram. It blocks until the rate limiter allows.
// On 429 (rate limited), it waits for retry_after seconds and retries.
func (t *Telegram) Send(ctx context.Context, alert types.Alert) error {
	if t.botToken == "" || t.chatID == "" {
		t.logger.Warn("telegram not configured, skipping alert", "type", alert.Type)
		return nil
	}

	reqBody := telegramSendRequest{
		ChatID:    t.chatID,
		Text:      alert.Message,
		ParseMode: "Markdown",
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := t.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limiter: %w", err)
		}

		url := fmt.Sprintf("%s/bot%s/sendMessage", telegramAPI, t.botToken)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := t.client.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}

		var tgResp telegramResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&tgResp)
		resp.Body.Close()
		if decodeErr != nil {
			return fmt.Errorf("decode response: %w", decodeErr)
		}

		if tgResp.OK {
			t.logger.Debug("telegram message sent", "type", alert.Type.String())
			return nil
		}

		// Handle 429 rate limit
		if resp.StatusCode == http.StatusTooManyRequests && tgResp.Parameters != nil && tgResp.Parameters.RetryAfter > 0 {
			wait := time.Duration(tgResp.Parameters.RetryAfter) * time.Second
			t.logger.Warn("telegram rate limited, waiting", "retry_after", tgResp.Parameters.RetryAfter, "attempt", attempt+1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
				continue
			}
		}

		return fmt.Errorf("telegram API error: %s (status %d)", tgResp.Description, resp.StatusCode)
	}

	return fmt.Errorf("telegram API: max retries (%d) exhausted due to rate limiting", maxRetries)
}

// Notifier consumes alerts from a channel and sends them via Telegram.
// It runs until the context is cancelled.
func (t *Telegram) Notifier(ctx context.Context, alerts <-chan types.Alert) {
	for {
		select {
		case <-ctx.Done():
			t.logger.Info("notifier shutting down")
			return
		case alert, ok := <-alerts:
			if !ok {
				return
			}
			if err := t.Send(ctx, alert); err != nil {
				t.logger.Error("failed to send telegram alert",
					"type", alert.Type.String(),
					"error", err,
				)
			}
		}
	}
}
