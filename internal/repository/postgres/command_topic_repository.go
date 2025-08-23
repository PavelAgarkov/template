package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/PavelAgarkov/template/internal/models/pg_model"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/jackc/pgx/v5"
)

type CommandTopicRepository struct {
	Repository *Repository
}

func NewCommandRepository(repository *Repository) *CommandTopicRepository {
	return &CommandTopicRepository{
		Repository: repository,
	}
}

func (c *CommandTopicRepository) Dequeue(ctx context.Context, tx pgx.Tx, officeID pg_model.OfficeID, fn pg_model.Call) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	if _, err := tx.Exec(timeoutCtx,
		`SET LOCAL lock_timeout               = '1s';
         SET LOCAL statement_timeout          = '3min';
         SET LOCAL idle_in_transaction_session_timeout = '3min';`,
	); err != nil {
		return fmt.Errorf("[Dequeue] set local timeouts: %w", err)
	}

	dequeueQuery := `SELECT
    id,
    office_id,
    type,
    queue_number,
    priority,
    payload,
    created_at
FROM cloud_template.command_topic
WHERE queue_number = $1
ORDER BY
    priority   ASC,  -- самые маленькие значения priority первыми
    created_at ASC   -- старейшие из равных priority
LIMIT 1
FOR UPDATE SKIP LOCKED;`

	row := tx.QueryRow(timeoutCtx, dequeueQuery, officeID)
	cmd := &pg_model.Command{}
	if err := row.Scan(
		&cmd.ID,
		&cmd.OfficeID,
		&cmd.Type,
		&cmd.QueueNumber,
		&cmd.Priority,
		&cmd.Payload,
		&cmd.CreatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("[Dequeue] Scan command fn dequeue filed: %w", err)
	}

	logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
		Msg:       fmt.Sprintf("[Dequeue] Command fn dequeue filed: %v officeId %v", cmd.ID, cmd.OfficeID),
		Component: "CommandTopicRepository",
		Method:    "Dequeue",
	})

	err := fn(timeoutCtx, tx, cmd)
	if err != nil {
		return fmt.Errorf("[Dequeue] command fn dequeue filed: %w", err)
	}

	query := `DELETE FROM cloud_template.command_topic WHERE id = $1`
	tag, err := tx.Exec(timeoutCtx, query, cmd.ID)
	if err != nil {
		return fmt.Errorf("[Dequeue] DELETE failed: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("delete cmd skipped: row %d not found / already locked", cmd.ID)
	}

	logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
		Msg:       fmt.Sprintf("[Dequeue] Successfully processed and deleted command ID %d for office %d", cmd.ID, cmd.OfficeID),
		Component: "CommandTopicRepository",
		Method:    "Dequeue",
	})

	return nil
}

func (c *CommandTopicRepository) Enqueue(ctx context.Context, tx pgx.Tx, cmd *pg_model.Command) error {
	insertQuery := `insert into cloud_template.command_topic (type, queue_number, priority, payload, office_id, created_at)
values ($1, $2, $3, $4, $5, now()) on conflict do nothing;`

	if _, err := tx.Exec(ctx, insertQuery,
		cmd.Type,
		cmd.QueueNumber,
		cmd.Priority,
		cmd.Payload,
		cmd.OfficeID,
	); err != nil {
		return fmt.Errorf("[Enqueue] Exec filed: %w", err)
	}

	return nil
}

func (c *CommandTopicRepository) GetCommand(ctx context.Context, queueNumber int64, cmdType string) (*pg_model.Command, error) {
	getCommandQuery := `SELECT
	id,
	office_id,
	type,
	queue_number,
	priority,
	payload,
	created_at from cloud_template.command_topic where queue_number = $1 and type = $2 limit 1;`

	row := c.Repository.PoolMaster.QueryRow(ctx, getCommandQuery, queueNumber, cmdType)
	cmd := &pg_model.Command{}
	if err := row.Scan(
		&cmd.ID,
		&cmd.OfficeID,
		&cmd.Type,
		&cmd.QueueNumber,
		&cmd.Priority,
		&cmd.Payload,
		&cmd.CreatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("[GetCommand] Scan command filed: %w", err)
	}

	return cmd, nil
}
