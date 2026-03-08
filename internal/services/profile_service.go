package services

import (
	"context"
	"time"

	"github.com/mkfoyw/polyscan/internal/repository"
)

// ProfileService provides profile caching business logic.
type ProfileService struct {
	repo repository.ProfileRepository
}

func NewProfileService(repo repository.ProfileRepository) *ProfileService {
	return &ProfileService{repo: repo}
}

func (s *ProfileService) Upsert(ctx context.Context, rec *repository.ProfileRecord) error {
	return s.repo.Upsert(ctx, rec)
}

func (s *ProfileService) Get(ctx context.Context, address string) (*repository.ProfileRecord, error) {
	return s.repo.Get(ctx, address)
}

func (s *ProfileService) GetMulti(ctx context.Context, addresses []string) (map[string]*repository.ProfileRecord, error) {
	return s.repo.GetMulti(ctx, addresses)
}

func (s *ProfileService) Count(ctx context.Context) (int64, error) {
	return s.repo.Count(ctx)
}

func (s *ProfileService) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.repo.DeleteOlderThan(ctx, cutoff)
}
