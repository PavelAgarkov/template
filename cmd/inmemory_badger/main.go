package main

import (
	"context"
	"fmt"
	"time"

	"github.com/PavelAgarkov/template/container"
	"github.com/PavelAgarkov/template/internal/api"
	v1 "github.com/PavelAgarkov/template/internal/api/badger_interface/v1"
	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/repository/badger"
	"github.com/PavelAgarkov/template/internal/service/budger_service"
	badgerpbv1 "github.com/PavelAgarkov/template/protobuf/badger_interface/v1/service"

	"github.com/PavelAgarkov/service-pkg/kernel"

	sdk "github.com/PavelAgarkov/badger-wrapper"

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

	badgerStorageEngine, err := sdk.OpenOnlyInMemoryConnection(
		baseCtx,
		sdk.BadgerDBMaster{
			InMemory:             cfg.BadgerDBMaster.InMemory,
			RamLimitMemory:       cfg.BadgerDBMaster.RamLimitMemory,
			ReadOnly:             cfg.BadgerDBMaster.ReadOnly,
			WithMetrics:          cfg.BadgerDBMaster.WithMetrics,
			GCInterval:           cfg.BadgerDBMaster.GCInterval,
			NumGoroutines:        cfg.BadgerDBMaster.NumGoroutines,
			ValueThreshold:       cfg.BadgerDBMaster.ValueThreshold,
			BaseTableSize:        cfg.BadgerDBMaster.BaseTableSize,
			NumCompactors:        cfg.BadgerDBMaster.NumCompactors,
			ZstdCompressionLevel: cfg.BadgerDBMaster.ZstdCompressionLevel,
			DetectConflicts:      cfg.BadgerDBMaster.DetectConflicts,
			Encoder:              cfg.BadgerDBMaster.Encoder,
		},
		sdk.TxnManagerOptions{
			MaxRetries:  5,
			BaseBackoff: 5 * time.Millisecond,
			MaxBackoff:  150 * time.Millisecond,
		},
		sdk.GetLevelByName(cfg.BadgerDBMaster.LoggingLevel),
		sdk.ReadWriteLoad,
	)
	if err != nil {
		fmt.Println("Failed to open Badger storage:", err)
		return
	}
	app.RegisterShutdown("badger-only-in-memory-storage", func() { _ = badgerStorageEngine.Close() }, kernel.MediumPriority)

	badgerRepository := badger.NewRepository(badgerStorageEngine)
	queryEngineService := budger_service.NewQueryService(badgerRepository)

	container.InitGrpcServer(
		baseCtx, app, cfg.Server,
		func(s *grpc.Server) {
			badgerpbv1.RegisterBadgerServiceServer(s, v1.NewBadgerImplementationV1(queryEngineService))
		},
		[]grpc.UnaryServerInterceptor{
			api.UnaryLoggerInterceptor(),
		},
	)

	//badgersdk.Demonstrate(badgerRepository.GetEngine())

	const (
		DefaultBD        = "badger_in_memory"
		DefaultVersion   = "v1"
		DefaultUserTable = "user"
	)
	db := DefaultBD
	ver := DefaultVersion
	table := DefaultUserTable
	parts := []sdk.IndexPart{{Field: "type", Value: "admin"}, {Field: "office", Value: "507"}}
	//parts := []sdk.IndexPart{{Field: "type", Value: "manager"}}
	//parts := []sdk.IndexPart{{Field: "office", Value: "507"}} // ничего не найдет т.к. office не в начале префикса
	//parts := []sdk.IndexPart{{Field: "type", Value: "admin"}}
	prefix := sdk.BuildCompositeIndexPrefix(db, ver, table, parts)
	fmt.Println(string(prefix) + " <- index prefix") // idx:badger_in_memory:v1:user:type==admin#

	pkprefix := sdk.BuildPKPrefix(db, ver, table)
	fmt.Println(string(pkprefix) + " <- pk prefix")

	sdk.Demonstrate(badgerStorageEngine, db, ver, table, prefix, pkprefix)
	app.Run()
}
