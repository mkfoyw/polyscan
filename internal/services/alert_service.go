// Package services provides business logic services that orchestrate
// repository operations. Each service depends on repository interfaces,
// making it decoupled from the storage implementation.
package services

import (
	"context"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// AlertService provides alert business logic.
type AlertService struct {
	repo repository.AlertRepository
}

func NewAlertService(repo repository.AlertRepository) *AlertService {
	return &AlertService{repo: repo}
}

func (s *AlertService) Insert(ctx context.Context, rec *repository.AlertRecord) error {
	return s.repo.Insert(ctx, rec)
}

func (s *AlertService) Recent(ctx context.Context, limit int64) ([]repository.AlertRecord, error) {
	return s.repo.Recent(ctx, limit)
}

func (s *AlertService) RecentByType(ctx context.Context, alertType string, limit int64) ([]repository.AlertRecord, error) {
	return s.repo.RecentByType(ctx, alertType, limit)
}

func (s *AlertService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *AlertService) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.repo.DeleteOlderThan(ctx, cutoff)
}
