package v1

import (
	"github.com/PavelAgarkov/template/internal/service/budger_service"
	pb "github.com/PavelAgarkov/template/protobuf/badger_interface/v1/service"
)

type BadgerImplementationV1 struct {
	pb.UnimplementedBadgerServiceServer
	queryEngineService budger_service.QueryEngineServiceInterface
}

func NewBadgerImplementationV1(queryEngineService budger_service.QueryEngineServiceInterface) *BadgerImplementationV1 {
	return &BadgerImplementationV1{
		queryEngineService: queryEngineService,
	}
}
