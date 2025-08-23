package main

import (
	"context"
	"fmt"
	loggerwrapper "github.com/PavelAgarkov/service-pkg/logger"
	"github.com/PavelAgarkov/template/container"
	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/repository/postgres"
	"github.com/PavelAgarkov/template/internal/service/autorization"

	"github.com/PavelAgarkov/service-pkg/application"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"

	"os"
	"time"
)

func main() {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		logger.WritePanicLog(baseCtx, &loggerwrapper.LogEntry{
			Msg:       "Failed to load config",
			Component: "token-generator",
			Method:    "main",
			Error:     err,
		})
		return
	}

	app := application.NewApp(baseCtx, cfg.Application.Cores, cfg.Application.HeapOverflow)
	app.Start(cancel)
	defer app.Stop()
	defer app.RegisterRecovers()()

	container.InitLogger()
	postgresRepository := container.InitPostgres(baseCtx, app, cfg.PostgresMaster, cfg.PostgresAsyncReplicas, cfg.PostgresSyncReplicas)
	authorizingRepository := postgres.NewAuthorizationRepository(postgresRepository)
	authorizationService := autorization.NewService(baseCtx, authorizingRepository)
	app.RegisterShutdown("authorizationService", authorizationService.Stop, application.ImmediatePriority)

	if len(os.Args) < 2 {
		logger.WriteErrorLog(baseCtx, &loggerwrapper.LogEntry{
			Msg:       "Client name is required",
			Component: "token-generator",
			Method:    "main",
			Args:      os.Args,
			Error:     fmt.Errorf("client name is required as an argument"),
		})
		return
	}

	now := time.Now()
	clientName := os.Args[1]
	authorized, _ := authorizationService.Generate(baseCtx, clientName)

	logger.WriteInfoLog(baseCtx, &loggerwrapper.LogEntry{
		Msg:       fmt.Sprintf("Client: %s\nToken: %s\n", authorized.Client, authorized.Token),
		Component: "token-generator",
		Start:     &now,
		Method:    "main",
		Args:      os.Args[1],
		Result:    "Token generated successfully",
	})

	return
}
