import logging

from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobModel
from app.domain.state_machine import JobStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section 6.1: Core Prometheus Metrics Definition
# ---------------------------------------------------------------------------

# 1. Total jobs transitioning through states
JOBS_TOTAL = Counter(
    "leaseflow_jobs_total",
    "Total number of jobs transitioning through various states",
    ["type", "status"],
)

# 2. Handler execution duration
JOB_DURATION_SECONDS = Histogram(
    "leaseflow_job_duration_seconds",
    "Time taken to execute job handler in seconds",
    ["type", "outcome"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

# 3. Queue wait latency (from run_at until claimed)
QUEUE_WAIT_SECONDS = Histogram(
    "leaseflow_queue_wait_seconds",
    "Time spent by job waiting in queue before claim in seconds",
    ["type"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

# 4. Total worker lease expirations detected by reaper
LEASE_EXPIRED_TOTAL = Counter(
    "leaseflow_lease_expired_total",
    "Total number of worker lease expirations detected by reaper",
    ["type"],
)

# 5. Stale commit / heartbeat rejections (fencing token violations)
STALE_COMMIT_REJECTED_TOTAL = Counter(
    "leaseflow_stale_commit_rejected_total",
    "Total number of commit/heartbeat operations rejected due to stale fencing token",
    ["type"],
)

# 6. Total retry attempts initiated
RETRIES_TOTAL = Counter(
    "leaseflow_retries_total",
    "Total number of job retry attempts initiated",
    ["type"],
)

# 7. Total jobs permanently moved to Dead-Letter Queue
DEAD_LETTER_TOTAL = Counter(
    "leaseflow_dead_letter_total",
    "Total number of jobs permanently moved to DEAD_LETTER",
    ["type"],
)

# 8. Duplicate delivery attempts suppressed via idempotency
DUPLICATE_DELIVERIES_TOTAL = Counter(
    "leaseflow_duplicate_deliveries_total",
    "Total number of duplicate delivery attempts detected and suppressed via idempotency key",
    ["type"],
)

# 9. Time taken to recover an abandoned job after lease expiration
RECOVERY_SECONDS = Histogram(
    "leaseflow_recovery_seconds",
    "Time taken to recover an abandoned job after lease expiration in seconds",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
)

# 10. Active worker instances
WORKERS_ACTIVE = Gauge(
    "leaseflow_workers_active",
    "Number of active worker instances currently running",
)

# 11. Current count of jobs in each lifecycle status
JOBS_IN_STATE = Gauge(
    "leaseflow_jobs_in_state",
    "Current count of jobs in each lifecycle status",
    ["status"],
)

# 12. Heartbeat lag in seconds
HEARTBEAT_LAG_SECONDS = Histogram(
    "leaseflow_heartbeat_lag_seconds",
    "Time elapsed between scheduled heartbeat and actual execution in seconds",
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0),
)


async def update_queue_state_metrics(session: AsyncSession) -> dict[str, int]:
    """
    Queries live database counts for all job states and updates JOBS_IN_STATE gauges.
    Invoked prior to serving the /metrics scrape response.
    """
    counts = {status.value: 0 for status in JobStatus}

    try:
        stmt = select(JobModel.status, func.count(JobModel.id)).group_by(JobModel.status)
        results = await session.execute(stmt)
        for job_status, count in results.all():
            status_str = job_status.value if isinstance(job_status, JobStatus) else str(job_status)
            counts[status_str] = count

        for status_name, val in counts.items():
            JOBS_IN_STATE.labels(status=status_name).set(val)

    except Exception as e:
        logger.warning("Failed to collect queue state metrics: %s", e)

    return counts
