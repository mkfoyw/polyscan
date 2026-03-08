package services

import (
	"context"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// SmartMoneyUserService provides smart money user business logic.
type SmartMoneyUserService struct {
	repo repository.SmartMoneyUserRepository
}

func NewSmartMoneyUserService(repo repository.SmartMoneyUserRepository) *SmartMoneyUserService {
	return &SmartMoneyUserService{repo: repo}
}

func (s *SmartMoneyUserService) Upsert(ctx context.Context, rec *repository.SmartMoneyUser) error {
	return s.repo.Upsert(ctx, rec)
}

func (s *SmartMoneyUserService) GetAll(ctx context.Context) ([]repository.SmartMoneyUser, error) {
	return s.repo.GetAll(ctx)
}

func (s *SmartMoneyUserService) GetByStatus(ctx context.Context, status string) ([]repository.SmartMoneyUser, error) {
	return s.repo.GetByStatus(ctx, status)
}

func (s *SmartMoneyUserService) GetByAddress(ctx context.Context, address string) (*repository.SmartMoneyUser, error) {
	return s.repo.GetByAddress(ctx, address)
}

func (s *SmartMoneyUserService) UpdateAlias(ctx context.Context, address, alias string) error {
	return s.repo.UpdateAlias(ctx, address, alias)
}

func (s *SmartMoneyUserService) UpdateStatus(ctx context.Context, address, status string) error {
	return s.repo.UpdateStatus(ctx, address, status)
}

func (s *SmartMoneyUserService) UpdateStats(ctx context.Context, address string, winRate, roi float64, totalTrades, winningTrades int, totalVolume float64) error {
	return s.repo.UpdateStats(ctx, address, winRate, roi, totalTrades, winningTrades, totalVolume)
}

func (s *SmartMoneyUserService) UpdateProfile(ctx context.Context, address, name, pseudonym string) error {
	return s.repo.UpdateProfile(ctx, address, name, pseudonym)
}

func (s *SmartMoneyUserService) UpdateLastPollTS(ctx context.Context, address string, ts int64) error {
	return s.repo.UpdateLastPollTS(ctx, address, ts)
}

func (s *SmartMoneyUserService) IncrVolume(ctx context.Context, address string, amount float64) error {
	return s.repo.IncrVolume(ctx, address, amount)
}

func (s *SmartMoneyUserService) Delete(ctx context.Context, address string) error {
	return s.repo.Delete(ctx, address)
}

func (s *SmartMoneyUserService) Exists(ctx context.Context, address string) (bool, error) {
	return s.repo.Exists(ctx, address)
}

func (s *SmartMoneyUserService) CountByStatus(ctx context.Context, status string) (int64, error) {
	return s.repo.CountByStatus(ctx, status)
}

func (s *SmartMoneyUserService) GetCandidates(ctx context.Context) ([]repository.SmartMoneyUser, error) {
	return s.repo.GetCandidates(ctx)
}

func (s *SmartMoneyUserService) GetConfirmed(ctx context.Context) ([]repository.SmartMoneyUser, error) {
	return s.repo.GetConfirmed(ctx)
}

// SmartMoneyTradeService provides smart money trade business logic.
type SmartMoneyTradeService struct {
	repo repository.SmartMoneyTradeRepository
}

func NewSmartMoneyTradeService(repo repository.SmartMoneyTradeRepository) *SmartMoneyTradeService {
	return &SmartMoneyTradeService{repo: repo}
}

func (s *SmartMoneyTradeService) Insert(ctx context.Context, rec *repository.SmartMoneyTrade) error {
	return s.repo.Insert(ctx, rec)
}

func (s *SmartMoneyTradeService) UpsertMerge(ctx context.Context, rec *repository.SmartMoneyTrade) (*repository.SmartMoneyTrade, error) {
	return s.repo.UpsertMerge(ctx, rec)
}

func (s *SmartMoneyTradeService) Recent(ctx context.Context, wallets []string, limit int64, beforeTS int64, minUSD, maxPrice float64) ([]repository.SmartMoneyTrade, error) {
	return s.repo.Recent(ctx, wallets, limit, beforeTS, minUSD, maxPrice)
}

func (s *SmartMoneyTradeService) RecentByWallet(ctx context.Context, wallet string, limit int64) ([]repository.SmartMoneyTrade, error) {
	return s.repo.RecentByWallet(ctx, wallet, limit)
}

func (s *SmartMoneyTradeService) RecentSince(ctx context.Context, wallets []string, sinceTS int64) ([]repository.SmartMoneyTrade, error) {
	return s.repo.RecentSince(ctx, wallets, sinceTS)
}

func (s *SmartMoneyTradeService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *SmartMoneyTradeService) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.repo.DeleteOlderThan(ctx, cutoff)
}
