package cacher

import "context"

type ConcurrentWarmer interface {
	Name() string
	NeedWarm() bool
	Warm(ctx context.Context) error
}
