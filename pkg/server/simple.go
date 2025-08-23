package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/service/readiness"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/PavelAgarkov/service-pkg/utils"
)

type SimpleServer struct {
	server *http.Server
}

type Container struct {
	Liveness *readiness.Service
}

func NewSimpleServer(ctx context.Context, container *Container, router func(mux *http.ServeMux, container *Container), config config.SimpleServer) *SimpleServer {
	mux := http.NewServeMux()

	router(mux, container)
	server := &http.Server{
		Addr:    config.Addr,
		Handler: mux,
	}

	simpleServer := &SimpleServer{
		server: server,
	}

	utils.GoRecover(ctx, func(ctx context.Context) {
		if err := simpleServer.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       fmt.Sprintf("Failed to start simple server on %s", config.Addr),
				Error:     err,
				Component: "SimpleServer",
				Method:    "CreateSimple",
				Args:      config,
			})
		}
	})

	logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
		Msg:       fmt.Sprintf("simple server is started on %s", config.Addr),
		Component: "SimpleServer",
		Method:    "CreateSimple",
	})

	return simpleServer
}

func (simple *SimpleServer) Stop(shutdownDuration time.Duration) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownDuration)
		defer cancel()
		if err := simple.server.Shutdown(ctx); err != nil {
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "Failed to shutdown simple server",
				Error:     err,
				Component: "SimpleServer",
				Method:    "stop",
				Args:      simple.server.Addr,
			})
		}
	}
}
