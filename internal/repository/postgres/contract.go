package postgres

import (
	"context"
	"github.com/PavelAgarkov/template/internal/models/pg_model"

	"github.com/jackc/pgx/v5"
)

type (
	NomenclatureTopicRepositoryInterface interface {
		CreateNomenclatureGroupTask(ctx context.Context, tx pgx.Tx, bucket *pg_model.Bucket) error
		ScheduleQueueElementWithBlock(ctx context.Context, tx pgx.Tx, fn func(ctx context.Context, tx pgx.Tx, task *pg_model.Task) error, officeID pg_model.OfficeID, taskType []string) error
		GetRelationForStreamNomenclatureTask(ctx context.Context, tx pgx.Tx, task *pg_model.Task) ([]int64, error)
	}

	CommandTopicRepositoryInterface interface {
		Dequeue(ctx context.Context, tx pgx.Tx, officeID pg_model.OfficeID, fn pg_model.Call) error
		Enqueue(ctx context.Context, tx pgx.Tx, cmd *pg_model.Command) error
		GetCommand(ctx context.Context, queueNumber int64, cmdType string) (*pg_model.Command, error)
	}

	AuthorizationRepositoryInterface interface {
		Generate(ctx context.Context, name string, token string) (*pg_model.Authorized, error)
		GetAllAuthorizedUsers(ctx context.Context) ([]*pg_model.Authorized, error)
	}
)
