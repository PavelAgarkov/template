package v1

import (
	pb "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"
)

type TurnoverApiImplementationV1 struct {
	pb.UnimplementedGoodsTurnoverServiceServer
}

func NewTurnoverApiImplementation() *TurnoverApiImplementationV1 {
	return &TurnoverApiImplementationV1{}
}
