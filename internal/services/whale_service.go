package services

import (
	"context"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// WhaleService provides whale user business logic.
type WhaleService struct {
	repo repository.WhaleRepository
}

func NewWhaleService(repo repository.WhaleRepository) *WhaleService {
	return &WhaleService{repo: repo}
}

func (s *WhaleService) Upsert(ctx context.Context, rec *repository.WhaleRecord) (bool, error) {
	return s.repo.Upsert(ctx, rec)
}

func (s *WhaleService) GetAll(ctx context.Context) ([]repository.WhaleRecord, error) {
	return s.repo.GetAll(ctx)
}

func (s *WhaleService) GetByAddress(ctx context.Context, address string) (*repository.WhaleRecord, error) {
	return s.repo.GetByAddress(ctx, address)
}

func (s *WhaleService) UpdateLastPollTS(ctx context.Context, address string, ts int64) error {
	return s.repo.UpdateLastPollTS(ctx, address, ts)
}

func (s *WhaleService) IncrVolume(ctx context.Context, address string, amount float64) error {
	return s.repo.IncrVolume(ctx, address, amount)
}

func (s *WhaleService) UpdateProfile(ctx context.Context, address, name, pseudonym string) error {
	return s.repo.UpdateProfile(ctx, address, name, pseudonym)
}

func (s *WhaleService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *WhaleService) DeleteByAddress(ctx context.Context, address string) error {
	return s.repo.DeleteByAddress(ctx, address)
}

func (s *WhaleService) DeleteLowestVolume(ctx context.Context) error {
	return s.repo.DeleteLowestVolume(ctx)
}

func (s *WhaleService) Exists(ctx context.Context, address string) (bool, error) {
	return s.repo.Exists(ctx, address)
}

// WhaleTradeService provides whale trade business logic.
type WhaleTradeService struct {
	repo repository.WhaleTradeRepository
}

func NewWhaleTradeService(repo repository.WhaleTradeRepository) *WhaleTradeService {
	return &WhaleTradeService{repo: repo}
}

func (s *WhaleTradeService) Insert(ctx context.Context, rec *repository.WhaleTrade) error {
	return s.repo.Insert(ctx, rec)
}

func (s *WhaleTradeService) Recent(ctx context.Context, wallets []string, limit int64, beforeTS, afterTS int64, minUSD float64, maxPrice float64) ([]repository.WhaleTrade, error) {
	return s.repo.Recent(ctx, wallets, limit, beforeTS, afterTS, minUSD, maxPrice)
}

func (s *WhaleTradeService) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]repository.WhaleTrade, error) {
	return s.repo.RecentByWallet(ctx, wallet, limit)
}

func (s *WhaleTradeService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *WhaleTradeService) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.repo.DeleteOlderThan(ctx, cutoff)
}
