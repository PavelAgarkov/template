package v1

import (
	pb "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"
)

func mapGetTurnoverRowToResponseFromPostgres() *pb.GetTurnoverResponse {
	return &pb.GetTurnoverResponse{
		Data: nil,
	}
}

func mapGetTurnoverByNmsAndOfficeWithOffsetToResponse() *pb.GetTurnoverByNmsAndOfficeWithOffsetResponse {
	return &pb.GetTurnoverByNmsAndOfficeWithOffsetResponse{}
}
