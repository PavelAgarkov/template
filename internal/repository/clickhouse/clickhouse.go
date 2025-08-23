package clickhouse

import (
	"context"
	"database/sql"

	"github.com/PavelAgarkov/service-pkg/database/clickhouse"
)

type Repository struct {
	StatisticOrderClickHouse *sql.DB
	connection               *clickhouse.Connection
}

func NewClickhouseRepository(master *sql.DB, connection *clickhouse.Connection) *Repository {
	return &Repository{
		StatisticOrderClickHouse: master,
		connection:               connection,
	}
}

func (r *Repository) Reconnect(ctx context.Context) error {
	err := r.connection.Reconnect(ctx, r.connection.GetClickHouseConfig())
	if err != nil {
		return err
	}
	r.StatisticOrderClickHouse = r.connection.GetDB()
	return nil
}
