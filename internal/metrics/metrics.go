package metrics

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Registry struct {
	mu sync.Mutex

	requestsTotal       map[string]uint64
	requestDuration     map[string]*histogram
	rateLimiterTotal    map[string]uint64
	requestCacheTotal   map[string]uint64
	carrierPollTotal    map[string]uint64
	notificationsTotal  uint64
	lastCarrierBatch    int
	lastCarrierDuration time.Duration
}

type histogram struct {
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

func NewRegistry() *Registry {
	return &Registry{
		requestsTotal:     make(map[string]uint64),
		requestDuration:   make(map[string]*histogram),
		rateLimiterTotal:  make(map[string]uint64),
		requestCacheTotal: make(map[string]uint64),
		carrierPollTotal:  make(map[string]uint64),
	}
}

func (r *Registry) ObserveRequest(method, path string, statusCode int, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := labels(method, path, fmt.Sprintf("%d", statusCode))
	r.requestsTotal[key]++
	h := r.requestDuration[key]
	if h == nil {
		h = newHistogram()
		r.requestDuration[key] = h
	}
	h.observe(duration.Seconds())
}

func (r *Registry) ObserveRateLimit(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rateLimiterTotal[result]++
}

func (r *Registry) ObserveRequestCache(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requestCacheTotal[result]++
}

func (r *Registry) ObserveCarrierPoll(status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.carrierPollTotal[status]++
}

func (r *Registry) ObserveCarrierBatch(size int, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCarrierBatch = size
	r.lastCarrierDuration = duration
}

func (r *Registry) ObserveNotificationsSent(count int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notificationsTotal += uint64(count)
}

func (r *Registry) WritePrometheus() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder
	writeRuntimeMetrics(&b)

	writeCounterMap(&b, "subscription_api_requests_total", "Total API requests by method, path, and status code.", r.requestsTotal, []string{"method", "path", "status"})
	writeHistogramMap(&b, "subscription_api_request_duration_seconds", "API request duration in seconds.", r.requestDuration, []string{"method", "path", "status"})
	writeCounterMap(&b, "subscription_api_rate_limiter_total", "Rate limiter decisions.", r.rateLimiterTotal, []string{"result"})
	writeCounterMap(&b, "subscription_api_request_cache_total", "Postgres request cache decisions.", r.requestCacheTotal, []string{"result"})
	writeCounterMap(&b, "subscription_carrier_poll_total", "Carrier poll results.", r.carrierPollTotal, []string{"status"})

	b.WriteString("# HELP subscription_notifications_sent_total Notifications marked as sent.\n")
	b.WriteString("# TYPE subscription_notifications_sent_total counter\n")
	fmt.Fprintf(&b, "subscription_notifications_sent_total %d\n", r.notificationsTotal)

	b.WriteString("# HELP subscription_carrier_last_batch_size Last carrier poll batch size.\n")
	b.WriteString("# TYPE subscription_carrier_last_batch_size gauge\n")
	fmt.Fprintf(&b, "subscription_carrier_last_batch_size %d\n", r.lastCarrierBatch)

	b.WriteString("# HELP subscription_carrier_last_batch_duration_seconds Last carrier poll batch duration.\n")
	b.WriteString("# TYPE subscription_carrier_last_batch_duration_seconds gauge\n")
	fmt.Fprintf(&b, "subscription_carrier_last_batch_duration_seconds %.6f\n", r.lastCarrierDuration.Seconds())

	return b.String()
}

func newHistogram() *histogram {
	return &histogram{
		buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		counts:  make([]uint64, 11),
	}
}

func (h *histogram) observe(value float64) {
	h.count++
	h.sum += value
	for i, bucket := range h.buckets {
		if value <= bucket {
			h.counts[i]++
		}
	}
}

func writeRuntimeMetrics(b *strings.Builder) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	b.WriteString("# HELP go_goroutines Number of goroutines that currently exist.\n")
	b.WriteString("# TYPE go_goroutines gauge\n")
	fmt.Fprintf(b, "go_goroutines %d\n", runtime.NumGoroutine())

	b.WriteString("# HELP go_memstats_alloc_bytes Bytes of allocated heap objects.\n")
	b.WriteString("# TYPE go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(b, "go_memstats_alloc_bytes %d\n", mem.Alloc)

	b.WriteString("# HELP go_memstats_heap_alloc_bytes Bytes of allocated heap objects.\n")
	b.WriteString("# TYPE go_memstats_heap_alloc_bytes gauge\n")
	fmt.Fprintf(b, "go_memstats_heap_alloc_bytes %d\n", mem.HeapAlloc)

	b.WriteString("# HELP go_memstats_heap_sys_bytes Bytes of heap memory obtained from the OS.\n")
	b.WriteString("# TYPE go_memstats_heap_sys_bytes gauge\n")
	fmt.Fprintf(b, "go_memstats_heap_sys_bytes %d\n", mem.HeapSys)

	b.WriteString("# HELP go_gc_duration_seconds_sum Cumulative GC pause duration.\n")
	b.WriteString("# TYPE go_gc_duration_seconds_sum counter\n")
	fmt.Fprintf(b, "go_gc_duration_seconds_sum %.9f\n", float64(mem.PauseTotalNs)/float64(time.Second))

	b.WriteString("# HELP go_gc_duration_seconds_count Number of completed GC cycles.\n")
	b.WriteString("# TYPE go_gc_duration_seconds_count counter\n")
	fmt.Fprintf(b, "go_gc_duration_seconds_count %d\n", mem.NumGC)

	b.WriteString("# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.\n")
	b.WriteString("# TYPE process_cpu_seconds_total counter\n")
	fmt.Fprintf(b, "process_cpu_seconds_total %.6f\n", processCPUSeconds())
}

func processCPUSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	user := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6
	system := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6
	return user + system
}

func writeCounterMap(b *strings.Builder, name, help string, values map[string]uint64, labelNames []string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	for _, key := range sortedKeys(values) {
		fmt.Fprintf(b, "%s{%s} %d\n", name, labelString(labelNames, strings.Split(key, "\xff")), values[key])
	}
}

func writeHistogramMap(b *strings.Builder, name, help string, values map[string]*histogram, labelNames []string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s histogram\n", name)
	for _, key := range sortedKeys(values) {
		parts := strings.Split(key, "\xff")
		h := values[key]
		for i, bucket := range h.buckets {
			labels := append(append([]string{}, parts...), fmt.Sprintf("%.3g", bucket))
			fmt.Fprintf(b, "%s_bucket{%s} %d\n", name, labelString(append(labelNames, "le"), labels), h.counts[i])
		}
		labels := append(append([]string{}, parts...), "+Inf")
		fmt.Fprintf(b, "%s_bucket{%s} %d\n", name, labelString(append(labelNames, "le"), labels), h.count)
		fmt.Fprintf(b, "%s_sum{%s} %.9f\n", name, labelString(labelNames, parts), h.sum)
		fmt.Fprintf(b, "%s_count{%s} %d\n", name, labelString(labelNames, parts), h.count)
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func labels(values ...string) string {
	return strings.Join(values, "\xff")
}

func labelString(names []string, values []string) string {
	parts := make([]string, 0, len(names))
	for i, name := range names {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, name, escapeLabel(values[i])))
	}
	return strings.Join(parts, ",")
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
