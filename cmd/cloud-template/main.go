package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/PavelAgarkov/template/container"
	"github.com/PavelAgarkov/template/internal/api"
	v1 "github.com/PavelAgarkov/template/internal/api/cloud-template/v1"
	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/repository/postgres"
	"github.com/PavelAgarkov/template/internal/service"
	"github.com/PavelAgarkov/template/internal/service/autorization"
	"github.com/PavelAgarkov/template/internal/service/command_bus"
	api2 "github.com/PavelAgarkov/template/internal/service/kafka/api"
	"github.com/PavelAgarkov/template/internal/service/kafka/handler"
	"github.com/PavelAgarkov/template/internal/service/nomenclature"
	"github.com/PavelAgarkov/template/internal/service/readiness"
	"github.com/PavelAgarkov/template/internal/service/scheduler"
	"github.com/PavelAgarkov/template/pkg/kafka/proxy_loader"
	"github.com/PavelAgarkov/template/pkg/mongo_db"
	cloudtemplatepbv1 "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"

	"github.com/PavelAgarkov/service-pkg/kernel"
	locker2 "github.com/PavelAgarkov/service-pkg/locker"
	"github.com/PavelAgarkov/service-pkg/readiness_barrier"
	scheduler2 "github.com/PavelAgarkov/service-pkg/scheduler"
	grpcserver "github.com/PavelAgarkov/service-pkg/server"
	"github.com/PavelAgarkov/service-pkg/utils"
	"github.com/PavelAgarkov/service-pkg/watchdog"

	"google.golang.org/grpc"
)

func main() {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		panic("Failed to load configuration: " + err.Error())
	}

	app := kernel.NewDefaultKernel(baseCtx, cfg.Application.Cores, cfg.Application.HeapOverflow)
	app.Start(cancel)

	defer app.Stop()
	defer app.RegisterRecovers()()

	mongoPool, err := mongo_db.New(baseCtx, mongo_db.Config{
		URI:               cfg.MongoDBPool.URI,
		DB:                cfg.MongoDBPool.DB,
		AppName:           cfg.MongoDBPool.AppName,
		MaxPoolSize:       cfg.MongoDBPool.MaxPoolSize,
		MinPoolSize:       cfg.MongoDBPool.MinPoolSize,
		MaxConnIdleTime:   cfg.MongoDBPool.MaxConnIdleTime,
		ServerSelectionTO: cfg.MongoDBPool.ServerSelectionTO,
		ConnectTimeout:    cfg.MongoDBPool.ConnectTimeout,
	})
	if err != nil {
		panic(err)
	}
	app.RegisterShutdown("mongodb.pool", func() {
		err := mongoPool.Close(baseCtx)
		if err != nil {
			log.Printf("Failed to close mongo pool: %v\n", err)
		}
	}, kernel.LowestPriority)

	postgresRepository := container.InitPostgres(baseCtx, app, cfg.PostgresMaster, cfg.PostgresAsyncReplicas, cfg.PostgresSyncReplicas)
	redisClient := container.InitRedisClient(baseCtx, app, cfg.Redis)
	transactionManager := postgres.NewTransactionManager(postgresRepository)

	authorizingRepository := postgres.NewAuthorizationRepository(postgresRepository)
	authorizationService := autorization.NewService(baseCtx, authorizingRepository)
	app.RegisterShutdown("authorization-service", authorizationService.Stop, kernel.HighestPriority)

	nomenclatureTopicRepository := postgres.NewNomenclatureTopicRepository(postgresRepository)
	nomenclatureTopicService := nomenclature.NewNomenclatureTopicService(transactionManager, nomenclatureTopicRepository)

	commandTopicRepository := postgres.NewCommandRepository(postgresRepository)
	commandService := command_bus.NewCommandService(
		*cfg,
		commandTopicRepository,
		transactionManager,
	)

	nomenclatureConsumer := scheduler2.NewJobScheduler(int64(cfg.Application.ClickhouseRate))
	commandBusConsumer := scheduler2.NewJobScheduler(int64(cfg.Application.CommandBusRate))
	cron := scheduler2.NewCron()
	_ = service.NewCore(
		baseCtx, *cfg, commandService,
		nomenclatureTopicService, nomenclatureConsumer,
		commandBusConsumer, cron,
	)

	barrierShepherdGoodsApiImplementationV1 := readiness_barrier.NewReadinessBarrier(baseCtx, readiness_barrier.ReadinessBarrierConfig{
		Name: "shepherd-cache-goods-v1",
	})
	barrierShepherdGoodsApiImplementationV1.Start()
	app.RegisterShutdown("readiness_barrier-shepherd-cache-goods-v1", barrierShepherdGoodsApiImplementationV1.Stop, kernel.ImmediatePriority)

	barrierShepherdGoodsApiImplementationV2 := readiness_barrier.NewReadinessBarrier(baseCtx, readiness_barrier.ReadinessBarrierConfig{
		Name: "shepherd-cache-goods-v2",
	})
	barrierShepherdGoodsApiImplementationV2.Start()
	app.RegisterShutdown("readiness_barrier-shepherd-cache-goods-v2", barrierShepherdGoodsApiImplementationV2.Stop, kernel.ImmediatePriority)

	num := 1
	pullConsumer := scheduler2.NewJobScheduler(1)
	err = pullConsumer.Add(scheduler2.JobConfiguration{
		Name: "pullConsumer",
		Func: func(ctx context.Context) error {
			if ctx.Err() != nil {
				fmt.Println("Pull Consumer cancelled")
				return ctx.Err()
			}

			if num%10 == 0 {
				fmt.Println("First interval finished, sending ready signal")
				err := barrierShepherdGoodsApiImplementationV1.SendSignalCtx(ctx, readiness_barrier.ReadySignalToggle)
				if err != nil {
					fmt.Println("Failed to send ready signal for v1:", err)
				}
				err = barrierShepherdGoodsApiImplementationV2.SendSignalCtx(ctx, readiness_barrier.ReadySignalToggle)
				if err != nil {
					fmt.Println("Failed to send ready signal for v2:", err)
				}

				//time.Sleep(10 * time.Second)
				fmt.Println("Second interval started, sending NotReadySignalToggle signal")
				err = barrierShepherdGoodsApiImplementationV1.SendSignalCtx(ctx, readiness_barrier.NotReadySignalToggle)
				if err != nil {
					fmt.Println("Failed to send NotReadySignalToggle signal for v1:", err)
				}
				err = barrierShepherdGoodsApiImplementationV2.SendSignalCtx(ctx, readiness_barrier.NotReadySignalToggle)
				if err != nil {
					fmt.Println("Failed to send NotReadySignalToggle signal for v2:", err)
				}

				//time.Sleep(10 * time.Second)
				fmt.Println("Third interval started, sending ready signal")
				err = barrierShepherdGoodsApiImplementationV1.SendSignalCtx(ctx, readiness_barrier.ReadySignalToggle)
				if err != nil {
					fmt.Println("Failed to send ready signal for v1:", err)
				}
				err = barrierShepherdGoodsApiImplementationV2.SendSignalCtx(ctx, readiness_barrier.ReadySignalToggle)
				if err != nil {
					fmt.Println("Failed to send ready signal for v2:", err)
				}

				fmt.Println("before stopping pullConsumer")

				go func() {
					defer utils.Recover(ctx)
					pullConsumer.Stop()()
				}()
				return nil
			}
			num++
			return nil
		},
		Tick:     1 * time.Second,
		Deadline: 60 * time.Second,
		StopMode: scheduler2.StopGraceful,
	},
	)
	if err != nil {
		panic("Failed to add pullConsumer job: " + err.Error())
	}

	container.InitGrpcServer(
		baseCtx, app, cfg.Server,
		func(s *grpc.Server) {
			cloudtemplatepbv1.RegisterGoodsTurnoverServiceServer(s, v1.NewTurnoverApiImplementation())
		},
		[]grpc.UnaryServerInterceptor{
			grpcserver.EnforceMaxSendSize((cfg.Server.OutGRPCBodySize * 9 / 10) * 1024 * 1024),
			api.UnaryLoggerInterceptor(),
			grpcserver.TimeoutUnaryInterceptor(cfg.Server.TimeOut),
			api.UnaryAuthInterceptor(authorizationService),
		},
	)
	clickHouseRepository := container.InitOrderClickhouse(baseCtx, app, cfg.Clickhouse)

	scheduleService := scheduler.NewService()

	shkOnPlaceConsumerConfig, ok := cfg.Kafka.Consumers["shk_on_place_consumer"]
	if !ok {
		panic("Failed to find kafka consumers config")
	}
	tareMoveConsumerConfig, ok := cfg.Kafka.Consumers["tare_move_consumer"]
	if !ok {
		panic("Failed to find kafka consumers config")
	}

	shkOnPlaceHandler := handler.NewShkOnPlaceHandler().Handle

	tareHandler := handler.NewTareMoveHandler().Handle

	goodsConsumerPool, restartGoodsConsumerPool := container.InitTopicConsumerPool(
		baseCtx, app, shkOnPlaceConsumerConfig, shkOnPlaceHandler)

	tareConsumerPool, restartTareConsumerPool := container.InitTopicConsumerPool(baseCtx, app, tareMoveConsumerConfig,
		tareHandler)

	consumerController := api2.NewConsumerController(
		goodsConsumerPool, tareConsumerPool,
		restartGoodsConsumerPool, restartTareConsumerPool,
	)

	schedule := container.InitRoboScheduler(baseCtx, app, cfg.Scheduler, scheduleService)

	locker := locker2.NewLocker(redisClient)
	wdog := watchdog.NewRedisWatchdogLeader(baseCtx, locker)
	app.RegisterShutdown("watchdog", wdog.Stop, kernel.ImmediatePriority)
	container.InitOrchestrator(app, wdog, schedule)
	app.StartWatchdogsLeadership()

	orchestratorPool := service.NewOrchestratorPool([]func(){restartGoodsConsumerPool, restartTareConsumerPool})
	orchestratorPool.Start()
	app.RegisterShutdown("orchestrator.pool", func() { orchestratorPool.Stop() }, kernel.ImmediatePriority)

	_, metrics := container.InitMetrics()
	mainProducers, shardProducers := container.InitPartitionedWriters(baseCtx, metrics, app, cfg)
	proxyLoader := proxy_loader.NewProxyLoader(metrics, mainProducers, shardProducers) // делайте свои способы записи в продюсеры
	proxy := api2.NewProxyAPI(metrics, proxyLoader)

	readinessService := readiness.NewService(redisClient, postgresRepository, clickHouseRepository)
	container.InitSimpleServer(baseCtx, app, cfg.SimpleServer, readinessService, consumerController)
	container.InitChiHTTPServer(baseCtx, app, cfg.ServerHttp, readinessService, proxy)

	app.Run()
}
