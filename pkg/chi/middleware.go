package chi

import (
	"net/http"
	"time"

	"github.com/PavelAgarkov/template/pkg/metrics"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		duration := time.Since(start).Seconds()

		metrics.HttpRequests.WithLabelValues(r.Method, route, http.StatusText(ww.Status())).Inc()
		metrics.HttpDuration.WithLabelValues(r.Method, route, http.StatusText(ww.Status())).Observe(duration)
	})
}
