package v1

import (
	"context"
	pb "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (i *TurnoverApiImplementationV1) RecalculateTurnoverForOffice(ctx context.Context, req *pb.RecalculateTurnoverForOfficeRequest) (*pb.RecalculateTurnoverForOfficeResponse, error) {
	if req.OfficeId < 1 {
		return nil, status.Errorf(codes.InvalidArgument, OfficeIDValidationError, req.OfficeId)
	}

	return &pb.RecalculateTurnoverForOfficeResponse{}, nil
}
