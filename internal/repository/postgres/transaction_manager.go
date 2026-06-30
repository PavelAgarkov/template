package postgres

import (
	"context"
	"fmt"
	"log"
	"time"

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
		log.Printf("Failed to begin transaction: %v", err)

		host := tm.master.Config().ConnConfig.Host
		port := tm.master.Config().ConnConfig.Port
		s := tm.master.Stat()
		log.Printf("Transaction manager stats: host=%s, port=%d, pool_acquired=%d, pool_total=%d, pool_idle=%d, pool_max=%d, acquire_duration=%v, acquire_count=%d, canceled_acquire_count=%d, constructing_conns=%d, empty_acquire_count=%d, max_lifetime_destroy_count=%d, max_idle_destroy_count=%d, new_conns_count=%d",
			host, port, s.AcquiredConns(), s.TotalConns(), s.IdleConns(), s.MaxConns(),
			s.AcquireDuration(), s.AcquireCount(), s.CanceledAcquireCount(),
			s.ConstructingConns(), s.EmptyAcquireCount(), s.MaxLifetimeDestroyCount(),
			s.MaxIdleDestroyCount(), s.NewConnsCount(),
		)

		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			panicRollbackError := tx.Rollback(ctx)
			if panicRollbackError != nil {
				log.Printf("Failed to rollback transaction after panic: %v", panicRollbackError)
			}
			log.Printf("Transaction rolled back due to panic: %v", p)
			return
		}

		if err != nil {
			rollbackContext, rollbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
			rollbackErr := tx.Rollback(rollbackContext)
			if rollbackErr != nil {
				log.Printf("Failed to rollback transaction: %v", rollbackErr)
			}
			log.Printf("Transaction rolled back due to error: %v", err)
			rollbackCancel()
			return
		}

		commitContext, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		commitErr := tx.Commit(commitContext)
		if commitErr != nil {
			log.Printf("Failed to commit transaction: %v", commitErr)
			rollbackAfterCommitError := tx.Rollback(commitContext)
			if rollbackAfterCommitError != nil {
				log.Printf("Failed to rollback transaction after commit error: %v", rollbackAfterCommitError)
			}

			commitCancel()
			return
		}
		commitCancel()
	}()

	err = fn(ctx, tx)
	return
}
