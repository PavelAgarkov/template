package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionManagerInterface interface {
	CallWithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
}

type TransactionManager struct {
	master *pgxpool.Pool
}

func NewTransactionManager(repository *Repository) *TransactionManager {
	return &TransactionManager{
		master: repository.PoolMaster,
	}
}

func (tm *TransactionManager) CallWithTx(
	ctx context.Context,
	fn func(ctx context.Context, tx pgx.Tx) error,
) (err error) {
	tx, err := tm.master.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "Failed to begin transaction",
			Error:     err,
			Component: "TransactionManager",
			Method:    "CallWithTx",
		})

		host := tm.master.Config().ConnConfig.Host
		port := tm.master.Config().ConnConfig.Port
		s := tm.master.Stat()
		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "debugging transaction manager stats",
			Error:     err,
			Component: "TransactionManager", Method: "CallWithTx",
			Args: map[string]any{
				"host":                            host,
				"port":                            port,
				"pool_acquired":                   s.AcquiredConns(),
				"pool_total":                      s.TotalConns(),
				"pool_idle":                       s.IdleConns(),
				"pool_max":                        s.MaxConns(),
				"acquire_duration":                s.AcquireDuration(),
				"acquire_count":                   s.AcquireCount(),
				"canceled_acquire_count":          s.CanceledAcquireCount(),
				"constructing_conns":              s.ConstructingConns(),
				"empty_acquire_count":             s.EmptyAcquireCount(),
				"max_lifetime_destroy_count":      s.MaxLifetimeDestroyCount(),
				"max_idle_destroy_count":          s.MaxIdleDestroyCount(),
				"new_conns_count":                 s.NewConnsCount(),
				"pool_constructing":               s.ConstructingConns(),
				"pool_acquire_count":              s.AcquireCount(),
				"pool_acquire_duration":           s.AcquireDuration(),
				"pool_canceled_acquire_count":     s.CanceledAcquireCount(),
				"pool_empty_acquire_count":        s.EmptyAcquireCount(),
				"pool_idle_conns":                 s.IdleConns(),
				"pool_max_conns":                  s.MaxConns(),
				"pool_total_conns":                s.TotalConns(),
				"pool_new_conns_count":            s.NewConnsCount(),
				"pool_max_lifetime_destroy_count": s.MaxLifetimeDestroyCount(),
				"pool_max_idle_destroy_count":     s.MaxIdleDestroyCount(),
				"pool_constructing_conns":         s.ConstructingConns(),
				"pool_acquired_conns":             s.AcquiredConns(),
			},
		})
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			panicRollbackError := tx.Rollback(ctx)
			if panicRollbackError != nil {
				logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
					Msg:       "Failed to rollback transaction after panic",
					Error:     panicRollbackError,
					Component: "TransactionManager",
					Method:    "CallWithTx",
				})
			}
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "Transaction rolled back due to panic",
				Error:     fmt.Errorf("%v", p),
				Component: "TransactionManager",
				Method:    "CallWithTx",
			})
			return
		}

		if err != nil {
			rollbackContext, rollbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
			rollbackErr := tx.Rollback(rollbackContext)
			if rollbackErr != nil {
				logger.WriteErrorLog(rollbackContext, &logger_wrapper.LogEntry{
					Msg:       "Failed to rollback transaction",
					Error:     rollbackErr,
					Component: "TransactionManager",
					Method:    "CallWithTx",
				})
			}
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "Transaction rolled back due to error",
				Error:     err,
				Component: "TransactionManager",
				Method:    "CallWithTx",
			})
			rollbackCancel()
			return
		}

		commitContext, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		commitErr := tx.Commit(commitContext)
		if commitErr != nil {
			logger.WriteErrorLog(commitContext, &logger_wrapper.LogEntry{
				Msg:       "Failed to commit transaction",
				Error:     commitErr,
				Component: "TransactionManager",
				Method:    "CallWithTx",
			})
			rollbackAfterCommitError := tx.Rollback(commitContext)
			if rollbackAfterCommitError != nil {
				logger.WriteErrorLog(commitContext, &logger_wrapper.LogEntry{
					Msg:       "Failed to commit transaction",
					Error:     rollbackAfterCommitError,
					Component: "TransactionManager",
					Method:    "CallWithTx",
				})
			}

			commitCancel()
			return
		}
		commitCancel()
	}()

	err = fn(ctx, tx)
	return
}
