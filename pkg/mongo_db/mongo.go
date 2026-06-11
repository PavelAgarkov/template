package mongo_db

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	URI               string        // mongodb://user:pass@host:port/db?replicaSet=...
	DB                string        // имя базы по умолчанию
	AppName           string        // для мониторинга
	MaxPoolSize       uint64        // максимум соединений в пуле драйвера (на хост)
	MinPoolSize       uint64        // минимум соединений в пуле
	MaxConnIdleTime   time.Duration // TTL простоя соединения
	ServerSelectionTO time.Duration // таймаут выбора сервера
	ConnectTimeout    time.Duration // таймаут установления соединения
}

type Pool struct {
	client *mongo.Client
	db     *mongo.Database
}

func New(ctx context.Context, cfg Config) (*Pool, error) {
	if cfg.URI == "" {
		return nil, errors.New("empty Mongo URI")
	}
	cliOpts := options.Client().ApplyURI(cfg.URI)
	if cfg.AppName != "" {
		cliOpts.SetAppName(cfg.AppName)
	}
	if cfg.MaxPoolSize > 0 {
		cliOpts.SetMaxPoolSize(cfg.MaxPoolSize)
	}
	if cfg.MinPoolSize > 0 {
		cliOpts.SetMinPoolSize(cfg.MinPoolSize)
	}
	if cfg.MaxConnIdleTime > 0 {
		cliOpts.SetMaxConnIdleTime(cfg.MaxConnIdleTime)
	}
	if cfg.ServerSelectionTO > 0 {
		cliOpts.SetServerSelectionTimeout(cfg.ServerSelectionTO)
	}
	if cfg.ConnectTimeout > 0 {
		cliOpts.SetConnectTimeout(cfg.ConnectTimeout)
	}

	client, err := mongo.Connect(ctx, cliOpts)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}

	p := &Pool{
		client: client,
		db:     client.Database(cfg.DB),
		//sem:    sem,
	}
	return p, nil
}

func (p *Pool) DB() *mongo.Database {
	return p.db
}

func (p *Pool) Client() *mongo.Client {
	return p.client
}

func (p *Pool) Close(ctx context.Context) error {
	return p.client.Disconnect(ctx)
}
