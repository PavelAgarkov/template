package command_bus

import (
	"context"
	"github.com/PavelAgarkov/template/internal/models/pg_model"

	"github.com/jackc/pgx/v5"
)

type (
	ServiceInterface interface {
		CreateCommand(ctx context.Context, cmd *pg_model.Command) error
		CreateCommandWithTx(ctx context.Context, tx pgx.Tx, cmd *pg_model.Command) error
		ScheduleCommandFromQueue(ctx context.Context, officeID pg_model.OfficeID, invoke pg_model.Call) error
		InvokeRouter(cmd *pg_model.Command) pg_model.Call
		Invoke(fnCtx context.Context, tx pgx.Tx, cmd *pg_model.Command) error
		GetCommand(ctx context.Context, queueNumber int64, cmdType string) (*pg_model.Command, error)
	}
)
