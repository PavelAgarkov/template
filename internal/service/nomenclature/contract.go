package nomenclature

import (
	"context"
	"github.com/PavelAgarkov/template/internal/models/pg_model"

	"github.com/jackc/pgx/v5"
)

type (
	Call           func(ctx context.Context, tx pgx.Tx, task *pg_model.Task) error
	TopicInterface interface {
		PushNomenclatureGroupTask(ctx context.Context, tx pgx.Tx, bucket *pg_model.Bucket, push *pg_model.PushParams) error
		GetRelationForStreamNomenclatureTask(ctx context.Context, tx pgx.Tx, task *pg_model.Task) ([]int64, error)
		ScheduleQueueElementWithBlock(ctx context.Context, fn Call, officeID pg_model.OfficeID, taskType []string) error
	}
)
