package routes

import (
	"github.com/PavelAgarkov/template/internal/service/readiness"
	"net/http"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
)

// LivenessProbe godoc
// @Summary      Liveness probe
// @Description  Returns 200 if the service is running.
// @Tags         health
// @Success      200  {string}  string  "OK"
// @Router       /liveness [get]
func LivenessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// ReadinessProbe godoc
// @Summary      Readiness probe
// @Description  200 – ready; 503 – dependency problem.
// @Tags         health
// @Success      200  {string}  string  "OK"
// @Failure      503  {string}  string  "Service Unavailable"
// @Router       /readiness [get]
func ReadinessProbe(ready *readiness.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := ready.CheckReadiness(ctx); err != nil {
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "Liveness check failed",
				Error:     err,
				Component: "HTTPServer",
				Method:    "ReadinessProbe",
				Args:      r.URL.Path,
			})
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Health godoc
// @Summary      Health ping
// @Tags         health
// @Success      200  {string}  string  "OK"
// @Router       /health [get]
func Health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
