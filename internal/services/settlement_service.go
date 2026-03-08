package services

import (
	"context"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// SettlementService provides settlement business logic.
type SettlementService struct {
	repo repository.SettlementRepository
}

func NewSettlementService(repo repository.SettlementRepository) *SettlementService {
	return &SettlementService{repo: repo}
}

func (s *SettlementService) Upsert(ctx context.Context, rec *repository.SettlementRecord) error {
	return s.repo.Upsert(ctx, rec)
}

func (s *SettlementService) Recent(ctx context.Context, limit int64, before time.Time) ([]repository.SettlementRecord, error) {
	return s.repo.Recent(ctx, limit, before)
}

func (s *SettlementService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *SettlementService) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.repo.DeleteOlderThan(ctx, cutoff)
}
