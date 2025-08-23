package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/PavelAgarkov/template/internal/service/autorization"
	pb "github.com/PavelAgarkov/template/protobuf/cloud-template/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

func withConstantHeaders(pairs ...string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoke grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, pairs...)
		return invoke(ctx, method, req, reply, cc, opts...)
	}
}

func main() {
	const scheme = "static"
	builder := manual.NewBuilderWithScheme(scheme)
	builder.InitialState(resolver.State{
		Addresses: []resolver.Address{
			//http://github.com/PavelAgarkov/template.whs-ai.k8s.prod-xc
			{Addr: "github.com/PavelAgarkov/template.whs-ai.k8s.prod-xc:82"},
			//http://github.com/PavelAgarkov/template.whs-ai.k8s.prod-el
			{Addr: "github.com/PavelAgarkov/template.whs-ai.k8s.prod-el:82"},
		},
	})

	// Example headers, replace with actual values
	hdr := []string{
		autorization.TokenMetaHeader, "43ed87a019b3515a6f2848eae4cdd44751777176d||||||||||||||||||||||||7e39e58aad9899061f4c781af00164482f66e9d9396324e4e1351d394c936724798ccd29638714fcfd227098ffaace6992a486ebe2e237800d95e46e1080af8833742f12968e372be96b717b184d7cdc03dc477d2a2b55e2b5eef811f3944b84c4fa7b54f5f215e150d032547de3b121c1dea3aa74439b75e4fbdc0d4427e1a21681f1268731e1373de60f449742d38e588955d4cfbd75cb544cb79f641550dd6ccaa16d615d58e4457379b0eb232f98a60bbf2e6a8e1c7325c27d92e40c2655ce9627368d6c8a5f27c452c317b36294f411ece128874c6138b2b82a270a9",
		autorization.ClientNameMetaHeader, "Obi Wan Kenobi",
	}

	pool := mem.NewTieredBufferPool(128<<10, 512<<10, 1<<20, 4<<20, 8<<20)
	conn, err := grpc.NewClient(
		fmt.Sprintf("%s:///github.com/PavelAgarkov/template", scheme),
		grpc.WithResolvers(builder),
		grpc.WithDefaultServiceConfig(
			`{
			"loadBalancingConfig": [{"round_robin": {}}],
			"methodConfig": [
				{
					"name": [
						{"service": "goods_turnover.v1.GoodsTurnoverService"}
					],
					"timeout": "2s",
					"retryPolicy": {
						"maxAttempts": 4,
						"initialBackoff": "0.2s",
						"maxBackoff": "2s",
						"backoffMultiplier": 1.5,
						"retryableStatusCodes": ["UNAVAILABLE", "RESOURCE_EXHAUSTED"]
					},
					"waitForReady": true
				}
			]
		}`,
		),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(withConstantHeaders(hdr...)),

		//experimental.WithBufferPool(mem.NopBufferPool{}), // можно не указывать: по умолчанию стоит mem.DefaultBufferPool
		experimental.WithBufferPool(pool),
		// отключаем лишние транспортные буферы
		grpc.WithReadBufferSize(0),
		grpc.WithWriteBufferSize(0),
		//grpc.WithSharedWriteBuffer(true), // если есть пул то не нужно
	)
	if err != nil {
		log.Fatalf("grpc connect: %v", err)
	}
	defer conn.Close()

	conn.Connect()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	client := pb.NewGoodsTurnoverServiceClient(conn)
	resp, err := client.GetTurnover(ctx, &pb.GetTurnoverRequest{
		OfficeId: 507,
		NmIds:    []int64{261537588, 261537712, 33},
	})
	if err != nil {
		log.Fatalf("GetTurnover: %v", err)
	}
	log.Printf("GetTurnover response: %+v", resp)
}
