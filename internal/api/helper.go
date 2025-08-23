package api

import (
	"context"
	"fmt"
	"github.com/PavelAgarkov/template/internal/service/autorization"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func UnaryLoggerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		requestId := uuid.New()
		//clientName, ok := ctx.Value(clientNameKey{}).(string)
		clientName := "unknown"
		//if !ok {
		//	return nil, status.Error(codes.Unauthenticated, "missing client name in context")
		//}

		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("Request from client: %s, method: %s, requestID %s", clientName, info.FullMethod, requestId),
			Component: "grpc",
			Method:    "UnaryLoggerInterceptor",
			Result:    "Request received",
		})

		resp, err := handler(ctx, req)
		if err != nil {
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       fmt.Sprintf("Error handling request from client %s: %v", clientName, err),
				Component: "grpc",
				Method:    "UnaryLoggerInterceptor",
				Result:    "Error occurred",
				Error:     err,
			})

			return nil, err
		}

		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("Response sent to client: %s, requestID: %s", clientName, requestId),
			Component: "grpc",
			Method:    info.FullMethod,
			Result:    "Response sent successfully",
		})
		return resp, nil
	}
}

func UnaryAuthInterceptor(authSvc autorization.ServiceInterface) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		clientName := first(md.Get(autorization.ClientNameMetaHeader))
		token := first(md.Get(autorization.TokenMetaHeader))

		if clientName == "" || token == "" {
			return nil, status.Error(codes.Unauthenticated, "missing client name or token")
		}

		valid, err := authSvc.CheckToken(token, clientName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "authorization process has error")
		}
		if !valid {
			return nil, status.Error(codes.Unauthenticated, "authorization failed, check your token or something else")
		}

		ctx = context.WithValue(ctx, clientNameKey{}, clientName)
		return handler(ctx, req)
	}
}

func first(vals []string) string {
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

type clientNameKey struct{}
