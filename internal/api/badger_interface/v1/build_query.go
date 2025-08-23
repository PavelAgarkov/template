package v1

import (
	"context"
	"strings"

	pb "github.com/PavelAgarkov/template/protobuf/badger_interface/v1/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (i *BadgerImplementationV1) BuildQueryBadger(ctx context.Context, req *pb.BuildQueryBadgerRequest) (*pb.BuildQueryBadgeResponse, error) {
	q := req.GetQuery()
	if q == nil || q.Builder == nil {
		return nil, status.Error(codes.InvalidArgument, "empty query/builder")
	}
	builder := q.GetBuilder()
	whereIs := q.GetWhereIs()

	dbase := strings.TrimSpace(builder.GetDatabase())
	version := strings.TrimSpace(builder.GetVersion())
	table := strings.TrimSpace(builder.GetTable())
	if dbase == "" || version == "" || table == "" {
		return nil, status.Error(codes.InvalidArgument, "builder.database/version/table must be set")
	}

	result, err := i.queryEngineService.BuildQuery(ctx, whereIs, dbase, version, table)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build query failed: %v", err)
	}
	return &pb.BuildQueryBadgeResponse{ResultJson: result}, nil
}
