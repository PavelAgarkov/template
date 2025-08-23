package main

import (
	"context"
	"fmt"
	"time"

	"github.com/PavelAgarkov/template/container"
	"github.com/PavelAgarkov/template/internal/api"
	v1 "github.com/PavelAgarkov/template/internal/api/cloud-template/v1"
	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/repository/postgres"
	"github.com/PavelAgarkov/template/internal/service"
	"github.com/PavelAgarkov/template/internal/service/autorization"
	"github.com/PavelAgarkov/template/internal/service/command_bus"
	"github.com/PavelAgarkov/template/internal/service/nomenclature"
	"github.com/PavelAgarkov/template/internal/service/readiness"
	cloudtemplatepbv1 "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"

	"github.com/PavelAgarkov/service-pkg/application"
	lock "github.com/PavelAgarkov/service-pkg/locker"
	"github.com/PavelAgarkov/service-pkg/readiness_barrier"
	scheduler2 "github.com/PavelAgarkov/service-pkg/scheduler"
	grpcserver "github.com/PavelAgarkov/service-pkg/server"
	"github.com/PavelAgarkov/service-pkg/utils"
	"github.com/PavelAgarkov/service-pkg/watchdog"
	"google.golang.org/grpc"
)

//func init() {
//	go func() {
//		mux := http.NewServeMux()
//		mux.HandleFunc("/debug/pprof/", pprof.Index)
//		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
//		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
//		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
//		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
//		_ = http.ListenAndServe("127.0.0.1:6060", mux)
//	}()
//}

func main() {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		panic("Failed to load configuration: " + err.Error())
	}

	container.InitLogger()

	app := application.NewApp(baseCtx, cfg.Application.Cores, cfg.Application.HeapOverflow)
	app.Start(cancel)

	defer app.FlushLogger()

	defer app.Stop()
	defer app.RegisterRecovers()()

	postgresRepository := container.InitPostgres(baseCtx, app, cfg.PostgresMaster, cfg.PostgresAsyncReplicas, cfg.PostgresSyncReplicas)
	redisClient := container.InitRedisClient(baseCtx, app, cfg.Redis)
	transactionManager := postgres.NewTransactionManager(postgresRepository)

	authorizingRepository := postgres.NewAuthorizationRepository(postgresRepository)
	authorizationService := autorization.NewService(baseCtx, authorizingRepository)
	app.RegisterShutdown("authorization-service", authorizationService.Stop, application.HighestPriority)

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
	app.RegisterShutdown("readiness_barrier-shepherd-cache-goods-v1", barrierShepherdGoodsApiImplementationV1.Stop, application.ImmediatePriority)

	barrierShepherdGoodsApiImplementationV2 := readiness_barrier.NewReadinessBarrier(baseCtx, readiness_barrier.ReadinessBarrierConfig{
		Name: "shepherd-cache-goods-v2",
	})
	barrierShepherdGoodsApiImplementationV2.Start()
	app.RegisterShutdown("readiness_barrier-shepherd-cache-goods-v2", barrierShepherdGoodsApiImplementationV2.Stop, application.ImmediatePriority)

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

	locker := lock.NewLocker(redisClient)
	wdog := watchdog.NewRedisWatchdogLeader(baseCtx, locker)
	app.RegisterShutdown("watchdog", wdog.Stop, application.ImmediatePriority)
	container.InitCron(app, wdog, cron)
	container.InitClickhouseBusher(baseCtx, app, wdog, nomenclatureConsumer)
	app.StartWatchdogsLeadership()
	container.InitScheduler(baseCtx, app, commandBusConsumer, pullConsumer)

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
	readinessService := readiness.NewService(redisClient, postgresRepository, clickHouseRepository)
	//container.InitGorillaHttpServer(baseCtx, app, cfg.SimpleServer, live)
	//container.InitSimpleServer(baseCtx, app, cfg.SimpleServer, live)
	container.InitChiHTTPServer(baseCtx, app, cfg.SimpleServer, readinessService)

	app.Run()
}
