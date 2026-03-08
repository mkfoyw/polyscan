package services

import (
	"context"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// TradeService provides trade business logic.
type TradeService struct {
	repo repository.TradeRepository
}

func NewTradeService(repo repository.TradeRepository) *TradeService {
	return &TradeService{repo: repo}
}

func (s *TradeService) Insert(ctx context.Context, rec *repository.TradeRecord) error {
	return s.repo.Insert(ctx, rec)
}

func (s *TradeService) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]repository.TradeRecord, error) {
	return s.repo.RecentByWallet(ctx, wallet, limit)
}

func (s *TradeService) RecentLarge(ctx context.Context, minUSD float64, limit int64, beforeTS int64, maxPrice float64, minPrice float64) ([]repository.TradeRecord, error) {
	return s.repo.RecentLarge(ctx, minUSD, limit, beforeTS, maxPrice, minPrice)
}

func (s *TradeService) ExistsByTxHash(ctx context.Context, txHash string) (bool, error) {
	return s.repo.ExistsByTxHash(ctx, txHash)
}

func (s *TradeService) Recent(ctx context.Context, limit int64, beforeTS int64) ([]repository.TradeRecord, error) {
	return s.repo.Recent(ctx, limit, beforeTS)
}

func (s *TradeService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *TradeService) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.repo.DeleteOlderThan(ctx, cutoff)
}

func (s *TradeService) EnrichByAssetTimestamp(ctx context.Context, asset string, timestamp int64, proxyWallet, pseudonym, profileName, txHash string) error {
	return s.repo.EnrichByAssetTimestamp(ctx, asset, timestamp, proxyWallet, pseudonym, profileName, txHash)
}

func (s *TradeService) UpsertRESTTrade(ctx context.Context, rec *repository.TradeRecord) error {
	return s.repo.UpsertRESTTrade(ctx, rec)
}
