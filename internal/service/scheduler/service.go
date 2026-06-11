package scheduler

import (
	"context"
)

const (
	CompareAndRemoveExpiredFilter = "compare_and_remove_expired_filter"
)

type Service struct {
}

func NewService() Contract {
	return &Service{}
}

func (s *Service) Route(ctx context.Context, key string) Call {
	switch key {
	case CompareAndRemoveExpiredFilter:
		return s.compareAndRemoveExpiredFilter(ctx)
	}
	return nil
}

func (s *Service) compareAndRemoveExpiredFilter(ctx context.Context) Call {
	return func(ctx context.Context) error {
		return nil
	}
}
