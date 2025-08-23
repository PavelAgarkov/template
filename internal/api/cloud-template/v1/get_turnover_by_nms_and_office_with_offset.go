package v1

import (
	"context"

	"github.com/PavelAgarkov/template/internal/models/pg_model"
	pb "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (i *TurnoverApiImplementationV1) GetTurnoverByNmsAndOfficeWithOffset(ctx context.Context, req *pb.GetTurnoverByNmsAndOfficeWithOffsetRequest) (*pb.GetTurnoverByNmsAndOfficeWithOffsetResponse, error) {
	if req.OfficeId < 1 {
		return nil, status.Errorf(codes.InvalidArgument, OfficeIDValidationError, req.OfficeId)
	}

	if req.CurrentNmId < 0 {
		return nil, status.Errorf(codes.InvalidArgument, CurrentNmIDValidationError, req.CurrentNmId)
	}

	if req.Offset < 0 || req.Offset > pg_model.TaskNMButch {
		return nil, status.Errorf(codes.InvalidArgument, OffsetValidationError, req.Offset)
	}

	return mapGetTurnoverByNmsAndOfficeWithOffsetToResponse(), nil
}
