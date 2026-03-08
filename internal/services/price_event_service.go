package services

import (
	"context"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// PriceEventService provides price event business logic.
type PriceEventService struct {
	repo repository.PriceEventRepository
}

func NewPriceEventService(repo repository.PriceEventRepository) *PriceEventService {
	return &PriceEventService{repo: repo}
}

func (s *PriceEventService) Insert(ctx context.Context, rec *repository.PriceEventRecord) error {
	return s.repo.Insert(ctx, rec)
}

func (s *PriceEventService) RecentByAsset(ctx context.Context, assetID string, limit int64) ([]repository.PriceEventRecord, error) {
	return s.repo.RecentByAsset(ctx, assetID, limit)
}

func (s *PriceEventService) Recent(ctx context.Context, limit int64) ([]repository.PriceEventRecord, error) {
	return s.repo.Recent(ctx, limit)
}

func (s *PriceEventService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *PriceEventService) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.repo.DeleteOlderThan(ctx, cutoff)
}
