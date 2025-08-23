package clickhouse

import (
	"golang.org/x/net/context"
)

type TurnoverRepositoryInterface interface {
	GetTurnoverNew(ctx context.Context) error
}
