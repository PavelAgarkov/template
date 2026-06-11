package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const Namespace = "robo_warehouse_gateway"

var (
	HttpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "robo_warehouse_gateway",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Количество HTTP запросов",
		},
		[]string{"method", "route", "status"},
	)
	HttpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "robo_warehouse_gateway",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Длительность HTTP запросов",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "route", "status"},
	)
)

type Metrics struct {
	RequestsTotal prometheus.Counter
}

func NewMetrics() *Metrics {
	m := &Metrics{
		RequestsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: Namespace,
				Subsystem: "http",
				Name:      "requests_total-1",
				Help:      "Общее количество запросов",
			},
		),
	}

	return m
}
