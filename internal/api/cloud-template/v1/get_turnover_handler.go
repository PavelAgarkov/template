package v1

import (
	"context"
	"github.com/PavelAgarkov/template/internal/models/pg_model"
	pb "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (i *TurnoverApiImplementationV1) GetTurnover(ctx context.Context, req *pb.GetTurnoverRequest) (*pb.GetTurnoverResponse, error) {
	if req.OfficeId < 1 {
		return nil, status.Errorf(codes.InvalidArgument, OfficeIDValidationError, req.OfficeId)
	}

	if n := len(req.NmIds); n == 0 || n > pg_model.TaskNMButch {
		return nil, status.Errorf(codes.InvalidArgument, NmIDsContainValidationError, len(req.NmIds))
	}

	return mapGetTurnoverRowToResponseFromPostgres(), nil
}
