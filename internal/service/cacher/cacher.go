package cacher

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

func Warm(ctx context.Context, limit int, warmers ...ConcurrentWarmer) error {
	if len(warmers) == 0 {
		return nil
	}

	g, ctx := errgroup.WithContext(ctx)
	if limit > 0 {
		g.SetLimit(limit)
	} else {
		g.SetLimit(1)
	}

	for _, w := range warmers {
		if !w.NeedWarm() {
			continue
		}
		w := w
		g.Go(
			func() error {
				if err := w.Warm(ctx); err != nil {
					return fmt.Errorf("failed the warm operation %s: %w", w.Name(), err)
				}
				return nil
			},
		)
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to warm caches: %w", err)
	}

	return nil
}
