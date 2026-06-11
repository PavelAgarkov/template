package scheduler

import "context"

type Call func(ctx context.Context) error

type Contract interface {
	Route(ctx context.Context, key string) Call
}
