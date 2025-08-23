package postgres

import (
	"github.com/PavelAgarkov/service-pkg/database/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	PoolMaster, PoolAsyncReplicas, PoolSyncReplicas                   *pgxpool.Pool
	masterConnection, asyncReplicasConnection, syncReplicasConnection *postgres.Connection
}

func NewPostgresRepository(masterConnection, asyncReplicasConnection, syncReplicasConnection *postgres.Connection) *Repository {
	return &Repository{
		masterConnection:        masterConnection,
		PoolMaster:              masterConnection.GetPool(),
		asyncReplicasConnection: asyncReplicasConnection,
		PoolAsyncReplicas:       asyncReplicasConnection.GetPool(),
		syncReplicasConnection:  syncReplicasConnection,
		PoolSyncReplicas:        syncReplicasConnection.GetPool(),
	}
}
