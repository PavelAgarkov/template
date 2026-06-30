package container

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/PavelAgarkov/template/internal/config"
	routes "github.com/PavelAgarkov/template/internal/open_api"
	"github.com/PavelAgarkov/template/internal/repository/clickhouse"
	"github.com/PavelAgarkov/template/internal/repository/postgres"
	"github.com/PavelAgarkov/template/internal/service/kafka/api"
	"github.com/PavelAgarkov/template/internal/service/readiness"
	"github.com/PavelAgarkov/template/internal/service/scheduler"
	chi2 "github.com/PavelAgarkov/template/pkg/chi"
	kafka2 "github.com/PavelAgarkov/template/pkg/kafka"
	"github.com/PavelAgarkov/template/pkg/metrics"
	_ "github.com/PavelAgarkov/template/swagger_docs" // Сгенерированный пакет с описанием OpenAPI

	simpleServer "github.com/PavelAgarkov/template/pkg/server"

	"github.com/bytedance/sonic"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	rateenvelopequeue "github.com/simplegear/rate-envelope-queue"

	"github.com/PavelAgarkov/service-pkg/application"
	clickhouse2 "github.com/PavelAgarkov/service-pkg/database/clickhouse"
	postgres2 "github.com/PavelAgarkov/service-pkg/database/postgres"
	scheduler2 "github.com/PavelAgarkov/service-pkg/scheduler"
	"github.com/PavelAgarkov/service-pkg/server"
	"github.com/PavelAgarkov/service-pkg/watchdog"

	"github.com/go-redis/redis/v8"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger/v2"
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

func InitOrchestrator(app *application.App, wdog watchdog.LeaderElectingWatchdog, orchestrator rateenvelopequeue.SingleQueuePool) {
	watcher := wdog.Elect(watchdog.Config{
		ElectionName: watchdog.Cron,
		Expiration:   watchdog.DefaultLeaderExpiration,
	})

	start := func() {
		orchestrator.Start()
		envelope, err := rateenvelopequeue.NewEnvelope(
			rateenvelopequeue.WithScheduleModeInterval(5*time.Second),
			rateenvelopequeue.WithInvoke(func(ctx context.Context, envelope *rateenvelopequeue.Envelope) error {
				log.Printf("Orchestrator is running\n")
				return nil
			}),
		)
		if err != nil {
			panic(err)
		}
		err = orchestrator.Send(envelope)
		if err != nil {
			panic(err)
		}
	}

	stop := func() {
		orchestrator.Stop()
	}

	app.RegisterWatchdogsLeadership(&application.LeaderSupervisor{
		Stop:           stop,
		Start:          start,
		Watchdog:       wdog,
		SupervisorName: watchdog.Cron,
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

func InitRedisClient(ctx context.Context, app *application.App, redisCfg config.RedisConfig) *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisCfg.Address,
		Username: redisCfg.Username,
		Password: redisCfg.Password,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	app.RegisterShutdown("redis_client", func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("Failed to close Redis client: %v", err)
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
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
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
					log.Printf("Readiness check failed: %v\n", err)
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

func InitSimpleServer(ctx context.Context, app *application.App, serverConfig config.SimpleServer, live *readiness.Service, consumerController api.ConsumerControllerInterface) {
	router := func(mux *http.ServeMux, container *simpleServer.Container) {
		if serverConfig.NeedProfiler {
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		}

		mux.Handle("/manage/consumer/", consumerController.Controller())

		mux.HandleFunc("/liveness", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/readiness", func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			if err := container.Liveness.CheckReadiness(ctx); err != nil {
				log.Printf("Readiness check failed: %v\n", err)
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
	serverCfg config.ServerHttp,
	rediness *readiness.Service,
	proxy api.Proxy,
) {

	storage := server.NewPreShutdownState(
		serverCfg.PreShutdownState.Need,
		serverCfg.PreShutdownState.TimeForDraining,
		serverCfg.PreShutdownState.TimeForShutdown,
	)
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

		s.Router.Route("/v1", func(r chi.Router) {
			r.Route("/stream", func(r chi.Router) {
				r.Route("/receive", func(r chi.Router) {
					r.Use(chi2.MetricsMiddleware)
					r.Use(server.DrainMiddleware(storage, func(w http.ResponseWriter) {
						func(w http.ResponseWriter, msg string, status int, code int) {
							type RequestError struct {
								Message string `json:"message"`
								Code    int    `json:"code"`
							}
							w.Header().Set("Content-Type", "application/json")
							w.WriteHeader(status)
							_, internalErr := sonic.Marshal(&RequestError{
								Message: msg,
								Code:    code,
							})

							if internalErr != nil {
								_, _ = sonic.Marshal(&RequestError{
									Message: msg,
									Code:    code,
								})
								log.Printf("failed to write json error response: %v", internalErr)
							}
						}(w, "server in draining state try again", http.StatusServiceUnavailable, 5555)
					}))
					// /v1/stream/receive/shk-on-place
					r.Post("/shk-on-place", proxy.ReceiveShkOnPlaceBytesBufferStreamV1)
					// /v1/stream/receive/tare-move
					r.Post("/tare-move", proxy.ReceiveTareMoveBytesBufferStreamV1)
				})
			})
		})
	}

	// Стартуем HTTP-сервер и регистрируем функцию остановки в приложении
	shutdown := server.CreateHTTPChiServer(
		router,
		serverCfg.Addr,
		storage,
		server.RecoverChiMiddleware,
		server.LoggerChiContextMiddleware(),
		server.LoggingChiMiddleware, // убрать после отладки
	)
	app.RegisterShutdown("chi_http_server", shutdown, application.ImmediatePriority)
}

func InitRoboScheduler(parent context.Context, app *application.App, SchedulerCfg config.Scheduler, router scheduler.Contract) rateenvelopequeue.SingleQueuePool {
	orchestrator := rateenvelopequeue.NewRateEnvelopeQueue(
		parent,
		SchedulerCfg.Name,
		rateenvelopequeue.WithLimitOption(SchedulerCfg.WorkerPool),
		rateenvelopequeue.WithStopModeOption(rateenvelopequeue.Drain),
	)

	for _, job := range SchedulerCfg.Schedule {
		envelope, err := rateenvelopequeue.NewEnvelope(
			rateenvelopequeue.WithType(job.Name),
			rateenvelopequeue.WithScheduleModeInterval(time.Duration(job.Time)),
			rateenvelopequeue.WithInvoke(
				func(ctx context.Context, envelope *rateenvelopequeue.Envelope) error {
					callCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
					defer cancel()

					call := router.Route(callCtx, job.Name)
					if call == nil {
						return nil
					}
					err := call(callCtx)
					if err != nil {
						return fmt.Errorf("failed to execute scheduled job %q: %w", job.Name, err)
					}
					return nil
				},
			),
		)
		if err != nil {
			panic(err)
		}
		err = orchestrator.Send(envelope)
		if err != nil {
			panic(err)
		}
	}

	return orchestrator
}

func InitTopicConsumerPool(
	parent context.Context,
	app *application.App,
	consumerConfig config.Consumer,
	handler func(context.Context, []kafka.Message) error,
) (rateenvelopequeue.SingleQueuePool, func()) {
	orchestrator := rateenvelopequeue.NewRateEnvelopeQueue(
		parent,
		fmt.Sprintf("orchestrator.%s", consumerConfig.Name),
		rateenvelopequeue.WithLimitOption(consumerConfig.WorkerPool),
		rateenvelopequeue.WithStopModeOption(rateenvelopequeue.Stop),
	)

	restart := func() {
		consumerInternalConfigs := kafka2.Configs{
			Name:              consumerConfig.Name,
			WorkersInPool:     consumerConfig.WorkerPool,
			Brokers:           consumerConfig.Brokers,
			Topic:             consumerConfig.Topic[0].Topic,
			GroupID:           consumerConfig.ReadConfigs.GroupID,
			BatchSize:         consumerConfig.ReadConfigs.BatchSize,
			BatchDeadline:     time.Duration(consumerConfig.ReadConfigs.BatchDeadline),
			MinBytes:          consumerConfig.ReadConfigs.MinBytes,
			MaxBytes:          consumerConfig.ReadConfigs.MaxBytes,
			CommitInterval:    0, // отключаем авто-коммит - ручной коммит после успешной обработки пачки
			SessionTimeout:    time.Duration(consumerConfig.ReadConfigs.SessionTimeout),
			HeartbeatInterval: time.Duration(consumerConfig.ReadConfigs.HeartbeatInterval),
			RebalanceTimeout:  time.Duration(consumerConfig.ReadConfigs.RebalanceTimeout),
			MaxWait:           time.Duration(consumerConfig.ReadConfigs.MaxWait),
			QueueCapacity:     consumerConfig.ReadConfigs.QueueCapacity,
			ReaderDownTimeout: time.Duration(consumerConfig.ReadConfigs.ReaderDownTimeout),
		}
		switch consumerConfig.ReadConfigs.Auth.Mechanism {
		case "SASL_PLAINTEXT_SHA256":
			mechanism, err := scram.Mechanism(scram.SHA256, consumerConfig.ReadConfigs.Auth.Login, consumerConfig.ReadConfigs.Auth.Password)
			if err != nil {
				panic(fmt.Sprintf("failed to create SASL mechanism: %v", err))
			}

			dialer := &kafka.Dialer{
				Timeout:       10 * time.Second,
				KeepAlive:     30 * time.Second,
				DualStack:     true,
				SASLMechanism: mechanism,
				ClientID:      consumerConfig.Name,
			}
			consumerInternalConfigs.Dialer = dialer

		default: // "PLAINTEXT"
			consumerInternalConfigs.Dialer = &kafka.Dialer{}
		}

		for i := 0; i < consumerInternalConfigs.WorkersInPool; i++ {
			consumerEnvelope, err := rateenvelopequeue.NewEnvelope(
				// интервал не важен, т.к. задача будет выполняться сразу после взятия из пула. Важно что так включается
				//режим периодического выполнения, который управляет перезапуском воркера после ребаланса вызванного изнутри
				rateenvelopequeue.WithScheduleModeInterval(time.Duration(consumerConfig.RebalanceInterval)), // время не важно, т.к это интервал для первого запуска и перезапуска при ребалансе
				rateenvelopequeue.WithInvoke(
					func(ctx context.Context, envelope *rateenvelopequeue.Envelope) error {
						consumer := kafka2.NewKafkaConsumer(
							consumerInternalConfigs,
							func(ctx context.Context, messages []kafka.Message) error {
								log.Printf("Processing %d messages", len(messages))

								err := handler(ctx, messages)
								if err != nil {
									log.Printf("Handler returned error: %v", err)
									return err
								}

								return nil
							},
						)
						// для проверки ребаланса можно включить таймаут. И после него оркестратор перезапустит воркер
						//ctx, cancel := context.WithTimeout(parent, 5*time.Second)
						//defer cancel()
						consumer.Run(ctx)

						return nil
					},
				),
			)

			if err != nil {
				panic(err)
			}
			err = orchestrator.Send(consumerEnvelope)
			if err != nil {
				panic(err)
			}
		}
	}

	return orchestrator, restart
}

func InitKafkaProducerPlaintext(ctx context.Context, app *application.App, producerCfg config.Producer, pool *sync.Pool) kafka2.Producer {
	transport := &kafka.Transport{}
	w := &kafka.Writer{
		Transport:              transport, // Plaintext
		Addr:                   kafka.TCP(producerCfg.Brokers...),
		MaxAttempts:            producerCfg.WriteConfigs.Attempts,                                                           // ретрай на случай REBALANCE/NOT_LEADER
		Balancer:               &kafka.Hash{},                                                                               // <-- round-robin по партициям
		Async:                  producerCfg.WriteConfigs.Async,                                                              // синхронная запись
		RequiredAcks:           kafka.RequireOne,                                                                            // дождаться коммита у лидера, фалловеров игнориуем
		BatchBytes:             producerCfg.WriteConfigs.BatchBytes,                                                         // 10 MiB
		BatchTimeout:           producerCfg.WriteConfigs.BatchTimeout,                                                       // макс задержка перед отправкой пачки
		BatchSize:              producerCfg.WriteConfigs.BatchSize,                                                          // макс кол-во сообщений в пачке
		AllowAutoTopicCreation: producerCfg.WriteConfigs.AllowAutoTopicCreation,                                             // полезно в prod
		Compression:            kafka.Zstd,                                                                                  // сжатие
		WriteTimeout:           producerCfg.WriteConfigs.WriteTimeout,                                                       // таймаут записи
		ReadTimeout:            producerCfg.WriteConfigs.ReadTimeout,                                                        // таймаут чтения
		ErrorLogger:            log.New(os.Stderr, producerCfg.WriteConfigs.ErrorLoggerLabel, log.LstdFlags|log.Lshortfile), // логгер ошибок
		Logger:                 log.New(io.Discard, "", 0),                                                                  // логгер событий (обычно не нужен)
	}
	app.RegisterShutdown(producerCfg.WriteConfigs.ErrorLoggerLabel, func() {
		err := w.Close()
		if err != nil {
			log.Printf("failed to close kafka writer: %v", err)
		}
		return
	}, application.HighPriority)

	mappers := make([]kafka2.TopicMapper, 0, len(producerCfg.Topic))
	for _, topic := range producerCfg.Topic {
		mappers = append(mappers, kafka2.TopicMapper{
			OfficeID: topic.OfficeID,
			Topic:    topic.Topic,
		})
	}

	wrappedWriter := kafka2.NewWriterWrapper(w, mappers, producerCfg.Type, producerCfg.Name, producerCfg.Entity, pool, producerCfg.Brokers, nil)
	err := wrappedWriter.Ping(ctx)
	if err != nil {
		log.Printf("failed to ping kafka writer: %v", err)
	}

	return wrappedWriter
}

func InitKafkaProducerSasl(ctx context.Context, app *application.App, producerCfg config.Producer, pool *sync.Pool) kafka2.Producer {
	var transport *kafka.Transport
	switch producerCfg.WriteConfigs.Auth.Mechanism {
	case "SASL_PLAINTEXT_SHA256":
		mech, err := scram.Mechanism(scram.SHA256, producerCfg.WriteConfigs.Auth.Login, producerCfg.WriteConfigs.Auth.Password)
		if err != nil {
			log.Fatalf("scram mechanism: %v", err)
		}
		transport = &kafka.Transport{
			SASL: mech,
		}
	default:
		log.Printf("unsupported kafka sasl mechanism: %s", producerCfg.WriteConfigs.Auth.Mechanism)
		panic("unsupported kafka sasl mechanism")
	}

	w := &kafka.Writer{
		Transport:   transport,
		Addr:        kafka.TCP(producerCfg.Brokers...),
		MaxAttempts: producerCfg.WriteConfigs.Attempts, // ретрай на случай REBALANCE/NOT_LEADER
		//Balancer:    &kafka.RoundRobin{}, // round-robin по партициям, плохо балансирует
		//Balancer:               &kafka.LeastBytes{},                                                                         // находит минимально загруженную партицию на клиентной стороне
		Balancer:               &kafka.Hash{},                                                                               // один и тот же ключ у продюсеоа попадает в одну и ту же партицию
		Async:                  producerCfg.WriteConfigs.Async,                                                              // синхронная запись
		RequiredAcks:           kafka.RequireOne,                                                                            // дождаться коммита у лидера, фалловеров игнориуем
		BatchBytes:             producerCfg.WriteConfigs.BatchBytes,                                                         // 10 MiB
		BatchTimeout:           producerCfg.WriteConfigs.BatchTimeout,                                                       // макс задержка перед отправкой пачки
		BatchSize:              producerCfg.WriteConfigs.BatchSize,                                                          // макс кол-во сообщений в пачке
		AllowAutoTopicCreation: producerCfg.WriteConfigs.AllowAutoTopicCreation,                                             // полезно в prod
		Compression:            kafka.Zstd,                                                                                  // сжатие
		WriteTimeout:           producerCfg.WriteConfigs.WriteTimeout,                                                       // таймаут записи
		ReadTimeout:            producerCfg.WriteConfigs.ReadTimeout,                                                        // таймаут чтения
		ErrorLogger:            log.New(os.Stderr, producerCfg.WriteConfigs.ErrorLoggerLabel, log.LstdFlags|log.Lshortfile), // логгер ошибок
		Logger:                 log.New(io.Discard, "", 0),                                                                  // логгер событий (обычно не нужен)
	}

	app.RegisterShutdown(producerCfg.WriteConfigs.ErrorLoggerLabel, func() {
		err := w.Close()
		if err != nil {
			log.Printf("failed to close kafka writer: %v", err)
		}
		return
	}, application.HighPriority)

	mappers := make([]kafka2.TopicMapper, 0, len(producerCfg.Topic))
	for _, topic := range producerCfg.Topic {
		mappers = append(mappers, kafka2.TopicMapper{
			OfficeID: topic.OfficeID,
			Topic:    topic.Topic,
		})
	}

	wrappedWriter := kafka2.NewWriterWrapper(w, mappers, producerCfg.Type, producerCfg.Name, producerCfg.Entity, pool, producerCfg.Brokers, transport)
	err := wrappedWriter.Ping(ctx)
	if err != nil {
		log.Printf("failed to ping kafka writer: %v", err)
	}

	return wrappedWriter
}

func InitPartitionedWriters(parent context.Context, custom *metrics.Metrics, app *application.App, cfg *config.Config) ([]kafka2.Producer, []kafka2.Producer) {
	var (
		shardWriters []kafka2.Producer
		mainWriters  []kafka2.Producer
	)

	switch {
	case config.IsProdEnv(cfg.Application.TestEnv) || config.IsStageEnv(cfg.Application.TestEnv):
		for _, producerConfig := range cfg.Kafka.Producers {
			writer := InitKafkaProducerSasl(
				parent, app, producerConfig,
				&sync.Pool{
					New: func() any {
						return make([]kafka.Message, 0, kafka2.DoubleMultiplePool(producerConfig.WriteConfigs.BatchSize))
					},
				},
			)
			switch producerConfig.Type {
			case kafka2.ShardProducer:
				shardWriters = append(shardWriters, writer)
			case kafka2.MainProducer:
				mainWriters = append(mainWriters, writer)
			}
		}
	case config.IsLocalEnv(cfg.Application.TestEnv):
		for _, producerConfig := range cfg.Kafka.Producers {
			writer := InitKafkaProducerPlaintext(
				parent, app, producerConfig,
				&sync.Pool{
					New: func() any {
						return make([]kafka.Message, 0, kafka2.DoubleMultiplePool(producerConfig.WriteConfigs.BatchSize))
					},
				},
			)
			switch producerConfig.Type {
			case kafka2.ShardProducer:
				shardWriters = append(shardWriters, writer)
			case kafka2.MainProducer:
				mainWriters = append(mainWriters, writer)
			}
		}
	}

	return mainWriters, shardWriters
}

func InitMetrics() (*prometheus.Registry, *metrics.Metrics) {
	registry := prometheus.NewRegistry()
	custom := metrics.NewMetrics()

	registry.MustRegister(
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(
				collectors.GoRuntimeMetricsRule{
					Matcher: regexp.MustCompile(".*"),
				},
			),
		),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			Namespace: metrics.Namespace,
		}),
		custom.RequestsTotal,
		metrics.HttpRequests,
		metrics.HttpDuration,
	)

	return registry, custom
}
