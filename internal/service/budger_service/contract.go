package budger_service

import (
	"context"
	pb "github.com/PavelAgarkov/template/protobuf/badger_interface/v1/service"
)

type (
	QueryEngineServiceInterface interface {
		BuildQuery(ctx context.Context, whereIs []*pb.Where, dbase, version, table string) ([]string, error)
	}
)
