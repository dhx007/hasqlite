package hasqlite

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	syncDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "hasqlite_sync_duration_seconds",
			Help: "Duration of sync operations",
		},
		[]string{"node_id", "database"},
	)
	
	eventCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hasqlite_events_total",
			Help: "Total number of events processed",
		},
		[]string{"node_id", "event_type"},
	)
	
	connectionCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hasqlite_connections",
			Help: "Number of active database connections",
		},
		[]string{"node_id", "database"},
	)
	
	errorCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hasqlite_errors_total",
			Help: "Total number of errors",
		},
		[]string{"node_id", "error_type"},
	)
)

func RecordSyncDuration(nodeID, database string, duration float64) {
	syncDuration.WithLabelValues(nodeID, database).Observe(duration)
}

func IncrementEventCount(nodeID, eventType string) {
	eventCount.WithLabelValues(nodeID, eventType).Inc()
}

func SetConnectionCount(nodeID, database string, count float64) {
	connectionCount.WithLabelValues(nodeID, database).Set(count)
}

func IncrementErrorCount(nodeID, errorType string) {
	errorCount.WithLabelValues(nodeID, errorType).Inc()
}