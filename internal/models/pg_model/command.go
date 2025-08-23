package pg_model

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	RecalculateCommandDyOfficeID            = "recalculate_command_by_office_id"
	RecalculateCommandByTtl                 = "recalculate_command_by_ttl"
	DeleteNmFromNomenclatureFilterScheduler = "delete_nm_from_nomenclature_filter_scheduler"
	DeleteFromNomenclatureCacheTurnover     = "delete_from_nomenclature_cache_turnover"
)

const (
	CommandPriorityTooLow      = 500
	CommandPriorityLow         = 400
	CommandPriorityMedium      = 300
	CommandPriorityNormal      = 200
	CommandPriorityHigh        = 100
	CommandPriorityCritical    = 1
	CommandDefaultPushPriority = CommandPriorityNormal
)

type Call func(ctx context.Context, tx pgx.Tx, cmd *Command) error

type Command struct {
	ID          int64
	Type        string
	Status      string
	QueueNumber int32
	Priority    int32
	Payload     []byte
	OfficeID    OfficeID
	CreatedAt   *time.Time
	LastUpdate  *time.Time
}
