package readiness

import (
	"context"
	"fmt"
	"github.com/PavelAgarkov/template/internal/repository/clickhouse"
	"github.com/PavelAgarkov/template/internal/repository/postgres"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	redisClient          *redis.Client
	postgresRepository   *postgres.Repository
	clickHouseRepository *clickhouse.Repository
}

func NewService(
	redisClient *redis.Client,
	postgresRepository *postgres.Repository,
	clickHouseRepository *clickhouse.Repository,
) *Service {
	return &Service{
		redisClient:          redisClient,
		postgresRepository:   postgresRepository,
		clickHouseRepository: clickHouseRepository,
	}
}

func (s *Service) CheckReadiness(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	g.Go(func() error {
		cctx, ccancel := context.WithTimeout(ctx, 800*time.Millisecond)
		defer ccancel()
		if err := s.redisClient.Ping(cctx).Err(); err != nil {
			return fmt.Errorf("ping redis: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		cctx, ccancel := context.WithTimeout(ctx, 800*time.Millisecond)
		defer ccancel()
		if err := s.postgresRepository.PoolMaster.Ping(cctx); err != nil {
			return fmt.Errorf("ping postgres master: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		cctx, ccancel := context.WithTimeout(ctx, 800*time.Millisecond)
		defer ccancel()
		if err := s.postgresRepository.PoolAsyncReplicas.Ping(cctx); err != nil {
			return fmt.Errorf("ping postgres async replicas: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		cctx, ccancel := context.WithTimeout(ctx, 800*time.Millisecond)
		defer ccancel()
		if err := s.postgresRepository.PoolSyncReplicas.Ping(cctx); err != nil {
			return fmt.Errorf("ping postgres sync replicas: %w", err)
		}
		return nil
	})

	//g.Go(func() error {
	//	cctx, ccancel := context.WithTimeout(ctx, 800*time.Millisecond)
	//	defer ccancel()
	//	if err := s.clickHouseRepository.StatisticOrderClickHouse.PingContext(cctx); err != nil {
	//		return fmt.Errorf("ping clickhouse replica: %w", err)
	//	}
	//	return nil
	//})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("readiness failed: %w", err)
	}

	logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
		Msg:       "Readiness check passed",
		Component: "readiness",
		Method:    "CheckReadiness",
	})

	return nil
}
