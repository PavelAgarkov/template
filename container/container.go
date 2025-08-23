package container

import (
	"context"
	"fmt"
	"github.com/PavelAgarkov/template/internal/config"
	routes "github.com/PavelAgarkov/template/internal/open_api"
	"github.com/PavelAgarkov/template/internal/repository/clickhouse"
	"github.com/PavelAgarkov/template/internal/repository/postgres"
	"github.com/PavelAgarkov/template/internal/service/readiness"
	_ "github.com/PavelAgarkov/template/swagger_docs" // Сгенерированный пакет с описанием OpenAPI
	"log"
	"net/http"
	"time"

	simpleServer "github.com/PavelAgarkov/template/pkg/server"

	"github.com/PavelAgarkov/service-pkg/application"
	clickhouse2 "github.com/PavelAgarkov/service-pkg/database/clickhouse"
	postgres2 "github.com/PavelAgarkov/service-pkg/database/postgres"
	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	scheduler2 "github.com/PavelAgarkov/service-pkg/scheduler"
	"github.com/PavelAgarkov/service-pkg/server"
	"github.com/PavelAgarkov/service-pkg/watchdog"
	"github.com/go-redis/redis/v8"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/mem"
)

func InitClickhouseBusher(
	ctx context.Context,
	app *application.App,
	wdog watchdog.LeaderElectingWatchdog,
	nomenclatureBlockScheduler scheduler2.JobSchedulerInterface,
) {
	watcher := wdog.Elect(watchdog.Config{
		ElectionName: watchdog.ClickhouseBusher,
		Expiration:   watchdog.DefaultLeaderExpiration,
	})
	app.RegisterWatchdogsLeadership(&application.LeaderSupervisor{
		Stop:           nomenclatureBlockScheduler.Stop(),
		Start:          nomenclatureBlockScheduler.Start(ctx),
		Watchdog:       wdog,
		SupervisorName: watchdog.ClickhouseBusher,
		Watcher:        watcher,
	})
}

func InitCron(app *application.App, wdog watchdog.LeaderElectingWatchdog, cron *scheduler2.Cron) {
	watcher := wdog.Elect(watchdog.Config{
		ElectionName: watchdog.Cron,
		Expiration:   watchdog.DefaultLeaderExpiration,
	})
	app.RegisterWatchdogsLeadership(&application.LeaderSupervisor{
		Stop:           cron.Stop,
		Start:          cron.Start,
		Watchdog:       wdog,
		SupervisorName: watchdog.Cron,
		Watcher:        watcher,
	})
}

func InitScheduler(ctx context.Context, app *application.App, schedulers ...scheduler2.JobSchedulerInterface) {
	supervisor := scheduler2.NewTaskSupervisor(schedulers)
	supervisor.Start(ctx)
	app.RegisterShutdown("scheduler_tasker", supervisor.Stop, application.ImmediatePriority)
}

func InitLogger() {
	if err := logger.InitLoggerForStdout(
		zapcore.InfoLevel, false, nil,
		zap.AddCallerSkip(2),
		zap.AddStacktrace(zapcore.DPanicLevel),
		zap.AddStacktrace(zapcore.PanicLevel),
		zap.AddStacktrace(zapcore.FatalLevel),
	); err != nil {
		panic(fmt.Sprintf("failed to init logger: %v", err))
	}
}

func InitRedisClient(ctx context.Context, app *application.App, redisCfg config.RedisConfig) *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisCfg.Address,
		Username: redisCfg.Username,
		Password: redisCfg.Password,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.WritePanicLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "Failed to connect to Redis",
			Component: "container",
			Method:    "InitRedisClient",
			Error:     err,
		})
	}

	app.RegisterShutdown("redis_client", func() {
		if err := redisClient.Close(); err != nil {
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "Failed to close Redis client",
				Component: "container",
				Method:    "InitRedisClient",
				Error:     err,
			})
		}
	}, application.MediumPriority)

	return redisClient
}

func InitPostgres(ctx context.Context, app *application.App, pgMasterCfg, pgAsyncReplicasCfg, pgSyncReplicasCfg config.Postgres) *postgres.Repository {
	postgresMaster := postgres2.NewPostgresConnection(
		ctx,
		postgres2.Configs{
			Host:                  pgMasterCfg.Host,
			Port:                  pgMasterCfg.Port,
			Username:              pgMasterCfg.Username,
			Password:              pgMasterCfg.Password,
			Database:              pgMasterCfg.Database,
			SSLMode:               pgMasterCfg.SSLMode,
			MaxOpenedConnections:  pgMasterCfg.MaxOpenedConnections,
			ConnectionMaxIdleTime: pgMasterCfg.ConnectionMaxIdleTime,
			ConnectionMaxLifeTime: pgMasterCfg.ConnectionMaxLifeTime,
			ApplicationName:       pgMasterCfg.ApplicationName,
			HealthCheckPeriod:     pgMasterCfg.HealthCheckPeriod,
			ConnectTimeout:        pgMasterCfg.ConnectTimeout,
			MaxConnLifeTimeJitter: pgMasterCfg.MaxConnLifeTimeJitter,
		},
	)
	app.RegisterShutdown("k8s_haproxy_pgsql_master", postgresMaster.Stop, application.HighPriority)

	postgresAsyncReplicas := postgres2.NewPostgresConnection(
		ctx,
		postgres2.Configs{
			Host:                  pgAsyncReplicasCfg.Host,
			Port:                  pgAsyncReplicasCfg.Port,
			Username:              pgAsyncReplicasCfg.Username,
			Password:              pgAsyncReplicasCfg.Password,
			Database:              pgAsyncReplicasCfg.Database,
			SSLMode:               pgAsyncReplicasCfg.SSLMode,
			MaxOpenedConnections:  pgAsyncReplicasCfg.MaxOpenedConnections,
			ConnectionMaxIdleTime: pgAsyncReplicasCfg.ConnectionMaxIdleTime,
			ConnectionMaxLifeTime: pgAsyncReplicasCfg.ConnectionMaxLifeTime,
			ApplicationName:       pgAsyncReplicasCfg.ApplicationName,
			HealthCheckPeriod:     pgAsyncReplicasCfg.HealthCheckPeriod,
			ConnectTimeout:        pgAsyncReplicasCfg.ConnectTimeout,
			MaxConnLifeTimeJitter: pgAsyncReplicasCfg.MaxConnLifeTimeJitter,
		},
	)
	app.RegisterShutdown("k8s_haproxy_pgsql_replicaasync", postgresAsyncReplicas.Stop, application.HighPriority)

	postgresSyncReplicas := postgres2.NewPostgresConnection(
		ctx,
		postgres2.Configs{
			Host:                  pgSyncReplicasCfg.Host,
			Port:                  pgSyncReplicasCfg.Port,
			Username:              pgSyncReplicasCfg.Username,
			Password:              pgSyncReplicasCfg.Password,
			Database:              pgSyncReplicasCfg.Database,
			SSLMode:               pgSyncReplicasCfg.SSLMode,
			MaxOpenedConnections:  pgSyncReplicasCfg.MaxOpenedConnections,
			ConnectionMaxIdleTime: pgSyncReplicasCfg.ConnectionMaxIdleTime,
			ConnectionMaxLifeTime: pgSyncReplicasCfg.ConnectionMaxLifeTime,
			ApplicationName:       pgSyncReplicasCfg.ApplicationName,
			HealthCheckPeriod:     pgSyncReplicasCfg.HealthCheckPeriod,
			ConnectTimeout:        pgSyncReplicasCfg.ConnectTimeout,
			MaxConnLifeTimeJitter: pgSyncReplicasCfg.MaxConnLifeTimeJitter,
		},
	)
	app.RegisterShutdown("k8s_haproxy_pgsql_replicasync", postgresSyncReplicas.Stop, application.HighPriority)

	return postgres.NewPostgresRepository(
		postgresMaster,
		postgresAsyncReplicas,
		postgresSyncReplicas,
	)
	//return postgres.NewPostgresRepository(nil, nil)
}

func InitOrderClickhouse(ctx context.Context, app *application.App, cfg clickhouse2.Clickhouse) *clickhouse.Repository {
	statisticOrderClickHouse, err := clickhouse2.NewClickhouseConnection(ctx, cfg)
	if err != nil {
		logger.WritePanicLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "Failed to connect to ClickHouse",
			Component: "container",
			Method:    "InitClickhouse",
			Error:     err,
		})
	}
	if statisticOrderClickHouse == nil {
		return nil
	}
	app.RegisterShutdown("order_clickhouse", statisticOrderClickHouse.Shutdown(ctx), application.HighPriority)

	return clickhouse.NewClickhouseRepository(statisticOrderClickHouse.GetDB(), statisticOrderClickHouse)
}

func InitGrpcServer(
	ctx context.Context,
	app *application.App,
	serverCfg config.Server,
	registerFunc func(s *grpc.Server),
	unaryInterceptors []grpc.UnaryServerInterceptor,
) {
	allUnaryInterceptors := []grpc.UnaryServerInterceptor{
		recovery.UnaryServerInterceptor(
			recovery.WithRecoveryHandlerContext(server.PanicHandler),
		),
	}
	allUnaryInterceptors = append(allUnaryInterceptors, unaryInterceptors...)

	//pool := mem.NewTieredBufferPool(128<<10, 512<<10, 1<<20, 4<<20, 8<<20)
	stop := server.CreateGRPCServer(
		ctx,
		registerFunc,
		server.Configs{
			Port:       serverCfg.Addr,
			Network:    serverCfg.Network,
			Reflection: serverCfg.ReflectionEnabled,
		},
		[]grpc.ServerOption{
			// ------------ если это убрать - сервер течет по памяти
			experimental.BufferPool(mem.NopBufferPool{}), // - сервер берет больше памяти, но его останавливает GC - эффективнее на средних нагрузках
			//experimental.BufferPool(pool), // сервер берет меньше памяти, но его не останавливает GC а пул - эффективнее на экстремальных нагрузках
			grpc.SharedWriteBuffer(true), // лучше использовать без пула
			grpc.WriteBufferSize(0),
			grpc.ReadBufferSize(0),
			// ------------ если это убрать - сервер течет по памяти

			grpc.ChainUnaryInterceptor(allUnaryInterceptors...),
			grpc.ChainStreamInterceptor(
				recovery.StreamServerInterceptor(
					recovery.WithRecoveryHandlerContext(server.PanicHandler),
				),
			),
			grpc.MaxSendMsgSize(serverCfg.OutGRPCBodySize * 1024 * 1024),
			grpc.MaxRecvMsgSize(serverCfg.InGRPCBodySize * 1024 * 1024),
		}...,
	)
	app.RegisterShutdown("grpc_server", stop, application.ImmediatePriority)
}

func InitGorillaHttpServer(ctx context.Context, app *application.App, serverConfig config.SimpleServer, live *readiness.Service) {
	list := func(simple *server.HTTPServer) {
		simple.Router.Handle("/liveness", http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
			}))
		simple.Router.Handle("/readiness", http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				ctx := request.Context()
				if err := live.CheckReadiness(ctx); err != nil {
					logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
						Msg:       "Liveness check failed",
						Error:     err,
						Component: "SimpleServer",
						Method:    "router",
						Args:      request.URL.Path,
					})
					writer.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				writer.WriteHeader(http.StatusOK)
			}))

		simple.Router.Handle("/health", http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusOK)
				log.Println("health check")
				return
			})).Methods("GET")

		simple.Router.Handle("/metrics", promhttp.Handler())
	}

	simpleHttpServerShutdownFunctionHttp := server.CreateHttpServer(
		list,
		serverConfig.Addr,
		server.LoggerContextMiddleware(),
		server.RecoverMiddleware,
		server.LoggingMiddleware,
	)
	app.RegisterShutdown("simple_server", simpleHttpServerShutdownFunctionHttp, application.ImmediatePriority)
}

func InitSimpleServer(ctx context.Context, app *application.App, serverConfig config.SimpleServer, live *readiness.Service) {
	router := func(mux *http.ServeMux, container *simpleServer.Container) {
		mux.HandleFunc("/liveness", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/readiness", func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			if err := container.Liveness.CheckReadiness(ctx); err != nil {
				logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
					Msg:       "Liveness check failed",
					Error:     err,
					Component: "SimpleServer",
					Method:    "router",
					Args:      r.URL.Path,
				})
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		})
	}
	container := &simpleServer.Container{
		Liveness: live,
	}
	simple := simpleServer.NewSimpleServer(ctx, container, router, serverConfig)
	app.RegisterShutdown("simple_server", simple.Stop(10*time.Second), application.ImmediatePriority)
}

// InitChiHTTPServer @title           github.com/PavelAgarkov/template Internal API
// @version         1.0
// @description     Liveness / readiness / metrics endpoints.
// @BasePath        /
// @schemes         http
func InitChiHTTPServer(
	ctx context.Context,
	app *application.App,
	serverCfg config.SimpleServer,
	rediness *readiness.Service,
) {
	// Регистрируем роуты
	router := func(s *server.HTTPServerChi) {
		s.Router.Get("/liveness", routes.LivenessProbe)
		s.Router.Get("/readiness", routes.ReadinessProbe(rediness))
		s.Router.Get("/health", routes.Health)

		s.Router.Handle("/metrics", promhttp.Handler())
		//http://localhost:9000/api/swagger/index.html
		s.Router.Get("/api/swagger/*", httpSwagger.Handler(
			httpSwagger.URL("/api/swagger/doc.json"),
		))
	}

	// Стартуем HTTP-сервер и регистрируем функцию остановки в приложении
	shutdown := server.CreateHTTPChiServer(
		router,
		serverCfg.Addr,
		server.LoggerChiContextMiddleware(),
		server.RecoverChiMiddleware,
		server.LoggingChiMiddleware,
	)
	app.RegisterShutdown("chi_http_server", shutdown, application.ImmediatePriority)
}
