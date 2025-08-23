package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PavelAgarkov/template/internal/models/pg_model"

	sq "github.com/Masterminds/squirrel"
	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/jackc/pgx/v5"
)

type NomenclatureTopicRepository struct {
	Repository *Repository
}

func NewNomenclatureTopicRepository(repository *Repository) *NomenclatureTopicRepository {
	return &NomenclatureTopicRepository{
		Repository: repository,
	}
}

func (repo *NomenclatureTopicRepository) CreateNomenclatureGroupTask(ctx context.Context, tx pgx.Tx, bucket *pg_model.Bucket) error {
	if bucket == nil || len(bucket.Nms) == 0 {
		return nil
	}

	NTqb := sq.Insert("cloud_template.nomenclature_topic").
		Columns("office_id", "task_type", "queue_number", "priority", "created_at", "last_update").
		Values(bucket.OfficeID, bucket.Push.TaskType, bucket.Push.QueueNumber, bucket.Push.Priority, sq.Expr("NOW()"), sq.Expr("NOW()"))

	NTqb = NTqb.PlaceholderFormat(sq.Dollar).Suffix("ON CONFLICT DO NOTHING RETURNING id, office_id")
	query, args, err := NTqb.ToSql()
	if err != nil {
		return fmt.Errorf("[CreateNomenclatureGroupTask] ToSql failed: %w", err)
	}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("[CreateNomenclatureGroupTask] Query failed: %w", err)
	}
	defer rows.Close()

	association := make(map[int64]int64)
	for rows.Next() {
		var id, officeID int64
		if err := rows.Scan(&id, &officeID); err != nil {
			return fmt.Errorf("[CreateNomenclatureGroupTask] Scan failed: %w", err)
		}
		association[officeID] = id
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("[CreateNomenclatureGroupTask] Err error: %w", err)
	}

	// и тут можно темп тейбл с копированием в него через, если хочется оптимизировать
	// if _, err := tx.CopyFrom(
	//        ctx,
	//        pgx.Identifier{"tmp_topic_item"},
	//        []string{"task_id", "office_id", "nm_id"},
	//        pgx.CopyFromRows(rows),
	//    );
	NTIqb := sq.Insert("cloud_template.nomenclature_topic_item").
		Columns("task_id", "office_id", "nm_id")
	taskID := association[int64(bucket.OfficeID)]
	for _, nm := range bucket.Nms {
		NTIqb = NTIqb.Values(taskID, bucket.OfficeID, nm)
	}
	NTIqb = NTIqb.PlaceholderFormat(sq.Dollar).Suffix("ON CONFLICT (task_id, nm_id, office_id) DO NOTHING")

	NTIquery, NTIargs, NTIerr := NTIqb.ToSql()
	if NTIerr != nil {
		return fmt.Errorf("[CreateNomenclatureGroupTask] ToSql ToSql failed: %w", NTIerr)
	}

	_, err = tx.Exec(ctx, NTIquery, NTIargs...)
	if err != nil {
		return fmt.Errorf("[CreateNomenclatureGroupTask] Exec failed: %w", err)
	}

	return nil
}

func (repo *NomenclatureTopicRepository) ScheduleQueueElementWithBlock(
	ctx context.Context,
	tx pgx.Tx,
	fn func(ctx context.Context, tx pgx.Tx, task *pg_model.Task) error,
	officeID pg_model.OfficeID,
	taskType []string,
) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	if _, err := tx.Exec(timeoutCtx,
		`SET LOCAL lock_timeout               = '1s';
         SET LOCAL statement_timeout          = '1min';
         SET LOCAL idle_in_transaction_session_timeout = '1min';`,
	); err != nil {
		return fmt.Errorf("[Dequeue] set local timeouts: %w", err)
	}

	taskQuery := `SELECT
    id,
    office_id,
    task_type,
    queue_number,
    priority,
    created_at,
    last_update
FROM cloud_template.nomenclature_topic
WHERE task_type = ANY($1) and queue_number = $2
ORDER BY
    priority   ASC,  -- самые маленькие значения priority первыми
    created_at ASC   -- старейшие из равных priority
LIMIT 1
FOR UPDATE SKIP LOCKED;`

	row := tx.QueryRow(timeoutCtx, taskQuery, taskType, officeID)
	task := &pg_model.Task{}
	if err := row.Scan(
		&task.ID,
		&task.OfficeID,
		&task.TaskType,
		&task.QueueNumber,
		&task.Priority,
		&task.CreatedAt,
		&task.LastUpdate); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("[ScheduleQueueElementWithBlock] Scan failed: %w", err)
	}

	logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
		Msg:       fmt.Sprintf("Processing task ID %d for office %d", task.ID, task.OfficeID),
		Component: "NomenclatureTopicRepository",
		Method:    "ScheduleQueueElementWithBlock",
	})

	err := fn(timeoutCtx, tx, task)
	if err != nil {
		return fmt.Errorf("[ScheduleQueueElementWithBlock] Function execution failed: %w", err)
	}

	query := `DELETE FROM cloud_template.nomenclature_topic WHERE id = $1`
	tag, err := tx.Exec(timeoutCtx, query, task.ID)
	if err != nil {
		return fmt.Errorf("[ScheduleQueueElementWithBlock] DELETE failed: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("delete task skipped: row %d not found / already locked", task.ID)
	}

	logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
		Msg:       fmt.Sprintf("Successfully processed and deleted task ID %d for office %d", task.ID, task.OfficeID),
		Component: "NomenclatureTopicRepository",
		Method:    "ScheduleQueueElementWithBlock",
	})

	return nil
}

func (repo *NomenclatureTopicRepository) GetRelationForStreamNomenclatureTask(ctx context.Context, tx pgx.Tx, task *pg_model.Task) ([]int64, error) {
	if task == nil {
		return nil, nil
	}
	query := `SELECT nm_id FROM cloud_template.nomenclature_topic_item WHERE task_id = $1`
	rows, err := tx.Query(ctx, query, task.ID)
	if err != nil {
		return nil, fmt.Errorf("[GetRelationForStreamNomenclatureTask] Query failed: %w", err)
	}
	defer rows.Close()

	var nmIDs []int64
	for rows.Next() {
		var nmID int64
		if err := rows.Scan(&nmID); err != nil {
			return nil, fmt.Errorf("[GetRelationForStreamNomenclatureTask] Scan failed: %w", err)
		}
		nmIDs = append(nmIDs, nmID)
	}
	return nmIDs, nil
}
