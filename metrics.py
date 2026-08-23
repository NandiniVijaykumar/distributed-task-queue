# metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Counters — monotonically increasing, good for rates
jobs_submitted_total = Counter(
    "jobs_submitted_total", "Jobs submitted via API", ["type", "priority"]
)
jobs_completed_total = Counter(
    "jobs_completed_total", "Jobs completed successfully", ["type"]
)
jobs_failed_total = Counter(
    "jobs_failed_total", "Job execution failures (before retry decision)", ["type"]
)
jobs_dead_total = Counter(
    "jobs_dead_total", "Jobs routed to dead-letter queue", ["type"]
)
jobs_retried_total = Counter(
    "jobs_retried_total", "Jobs routed to delayed queue for retry", ["type"]
)
leases_reaped_total = Counter(
    "leases_reaped_total", "Jobs recovered from a crashed worker by the reaper"
)

# Histograms — for latency/duration distributions
job_duration_seconds = Histogram(
    "job_duration_seconds", "Time spent executing a job", ["type"]
)
job_queue_wait_seconds = Histogram(
    "job_queue_wait_seconds", "Time between submission and claim", ["priority"]
)

# Gauges — current point-in-time values
queue_depth = Gauge(
    "queue_depth", "Jobs waiting in a pending queue", ["priority"]
)
workers_online = Gauge("workers_online", "Number of live worker heartbeats")