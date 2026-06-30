package server

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/service/readiness"

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
			log.Printf("Failed to start simple server on %s: %v", config.Addr, err)
		}
	})

	log.Printf("simple server is started on %s", config.Addr)

	return simpleServer
}

func (simple *SimpleServer) Stop(shutdownDuration time.Duration) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownDuration)
		defer cancel()
		if err := simple.server.Shutdown(ctx); err != nil {
			log.Printf("Failed to shutdown simple server on %s: %v", simple.server.Addr, err)
		}
	}
}
