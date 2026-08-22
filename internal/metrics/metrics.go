package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storm_requests_total",
			Help: "Total HTTP requests processed.",
		},
		[]string{"method", "status"},
	)

	inflight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "storm_inflight_requests",
			Help: "Number of requests currently in flight.",
		},
	)

	duration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "storm_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal, inflight, duration)
}

// RequestStart marks one request as in-flight.
func RequestStart() {
	inflight.Inc()
}

// Record reports one finished request to the collectors.
func Record(result storm.Result) {
	status := "error"
	if result.StatusCode > 0 {
		status = fmt.Sprintf("%d", result.StatusCode)
	}
	inflight.Dec()
	requestsTotal.WithLabelValues(result.Method, status).Inc()
	duration.WithLabelValues(result.Method).Observe(result.Duration.Seconds())
}

// Handler exposes the Prometheus scrape endpoint.
func Handler() http.Handler {
	return promhttp.Handler()
}
