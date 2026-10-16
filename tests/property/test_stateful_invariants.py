import asyncio
import uuid
from datetime import UTC, datetime, timedelta

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    rule,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.db.models import Base, ExecutionModel, JobModel
from app.db.repositories import JobRepository
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.scheduler.promoter import promote_due_jobs
from app.scheduler.reaper import reap_expired_jobs

WORKER_POOL = ["worker-alpha", "worker-beta", "worker-gamma"]


@settings(
    max_examples=20,
    stateful_step_count=12,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
class JobStateMachine(RuleBasedStateMachine):
    """
    Hypothesis RuleBasedStateMachine that models the full distributed lifecycle of LeaseFlow
    and asserts the 8 Section 5.2 core distributed systems invariants after every single step.
    """

    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Isolated in-memory SQLite database per state machine execution
        self.engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            echo=False,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
        self.backend = PostgresQueueBackend(self.session_factory)
        self.loop.run_until_complete(self._init_db())

        # Local model verification state
        self.known_jobs: dict[uuid.UUID, int] = {}  # job_id -> highest observed fencing token
        self.terminal_jobs: dict[
            uuid.UUID, JobStatus
        ] = {}  # job_id -> terminal status (SUCCEEDED, CANCELLED)
        self.active_claims: dict[
            uuid.UUID, tuple[str, int]
        ] = {}  # job_id -> (worker_id, fencing_token)

    async def _init_db(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def teardown(self) -> None:
        self.loop.run_until_complete(self.engine.dispose())
        self.loop.close()

    # -------------------------------------------------------------------------
    # STATE MACHINE RULES (OPERATIONS)
    # -------------------------------------------------------------------------

    @rule(
        priority=st.integers(min_value=-5, max_value=10),
        max_retries=st.integers(min_value=1, max_value=3),
        timeout_seconds=st.integers(min_value=5, max_value=60),
        as_queued=st.booleans(),
    )
    def create_job(
        self,
        priority: int,
        max_retries: int,
        timeout_seconds: int,
        as_queued: bool,
    ) -> None:
        """Rule: Client submits a new job."""

        async def _run() -> None:
            async with self.session_factory() as session:
                repo = JobRepository(session)
                job, _ = await repo.create_job(
                    job_type="compute_task",
                    payload={"input": 42},
                    priority=priority,
                    max_retries=max_retries,
                    timeout_seconds=timeout_seconds,
                )
                if as_queued:
                    job.status = JobStatus.QUEUED
                await session.commit()
                self.known_jobs[job.id] = job.fencing_token

        self.loop.run_until_complete(_run())

    @rule()
    def promote_jobs(self) -> None:
        """Rule: Promoter background service runs to move ready jobs to QUEUED."""

        async def _run() -> None:
            async with self.session_factory() as session:
                await promote_due_jobs(session)

        self.loop.run_until_complete(_run())

    @rule(worker_id=st.sampled_from(WORKER_POOL))
    def claim_job(self, worker_id: str) -> None:
        """Rule: A worker attempts atomic claim of next available job."""

        async def _run() -> None:
            job = await self.backend.claim(worker_id=worker_id, lease_ttl=10)
            if job:
                self.active_claims[job.id] = (worker_id, job.fencing_token)
                self.known_jobs[job.id] = job.fencing_token

        self.loop.run_until_complete(_run())

    @rule()
    def heartbeat_job(self) -> None:
        """Rule: Worker renews lease on a currently active job."""
        if not self.active_claims:
            return

        job_id = next(iter(self.active_claims))
        worker_id, token = self.active_claims[job_id]

        async def _run() -> None:
            await self.backend.heartbeat(
                job_id=job_id,
                fencing_token=token,
                lease_ttl=10,
            )

        self.loop.run_until_complete(_run())

    @rule(expire_mode=st.sampled_from(["lease", "deadline"]))
    def expire_and_reap(self, expire_mode: str) -> None:
        """Rule: Lease expires or execution deadline passes, and Reaper runs."""

        async def _run() -> None:
            # Artificially expire lease or deadline on an active claim if any
            if self.active_claims:
                target_id = next(iter(self.active_claims))
                async with self.session_factory() as session:
                    j = await session.get(JobModel, target_id)
                    if j and j.status == JobStatus.RUNNING:
                        now = datetime.now(UTC)
                        if expire_mode == "lease":
                            j.lease_expires_at = now - timedelta(seconds=2)
                        else:
                            j.execution_deadline = now - timedelta(seconds=2)
                        await session.commit()
                self.active_claims.pop(target_id, None)

            async with self.session_factory() as session:
                await reap_expired_jobs(session, backoff_seconds=0.0)

        self.loop.run_until_complete(_run())

    @rule()
    def complete_job(self) -> None:
        """Rule: Worker completes job with valid fencing token."""
        if not self.active_claims:
            return

        job_id = next(iter(self.active_claims))
        _, token = self.active_claims.pop(job_id)

        async def _run() -> None:
            await self.backend.complete(
                job_id=job_id,
                fencing_token=token,
                result={"result": "success"},
            )

        self.loop.run_until_complete(_run())

    @rule(retryable=st.booleans())
    def fail_job(self, retryable: bool) -> None:
        """Rule: Worker reports execution error with valid fencing token."""
        if not self.active_claims:
            return

        job_id = next(iter(self.active_claims))
        _, token = self.active_claims.pop(job_id)

        async def _run() -> None:
            await self.backend.fail(
                job_id=job_id,
                fencing_token=token,
                error="synthetic task failure",
                retryable=retryable,
                backoff_seconds=0.0,
            )

        self.loop.run_until_complete(_run())

    @rule()
    def stale_commit_attempt(self) -> None:
        """Rule: Zombie worker attempts commit with stale / outdated fencing token."""
        if not self.known_jobs:
            return

        job_id = next(iter(self.known_jobs))
        stale_token = 0  # Intentionally lower than any real token (fencing_token >= 1)

        async def _run() -> None:
            success = await self.backend.complete(
                job_id=job_id,
                fencing_token=stale_token,
                result={"stale": "corrupt"},
            )
            # Must always be rejected by DB fencing constraint
            assert success is False

        self.loop.run_until_complete(_run())

    @rule()
    def cancel_job(self) -> None:
        """Rule: Client cancels a non-terminal job."""
        if not self.known_jobs:
            return

        job_id = next(iter(self.known_jobs))

        async def _run() -> None:
            async with self.session_factory() as session:
                job = await session.get(JobModel, job_id)
                if job and job.status in (
                    JobStatus.PENDING,
                    JobStatus.QUEUED,
                    JobStatus.RUNNING,
                    JobStatus.RETRY_WAITING,
                ):
                    repo = JobRepository(session)
                    await repo.cancel_job(job_id)
                    self.active_claims.pop(job_id, None)

        self.loop.run_until_complete(_run())

    @rule()
    def operator_manual_retry(self) -> None:
        """Rule: Operator triggers manual retry on a DEAD_LETTER job."""

        async def _run() -> None:
            async with self.session_factory() as session:
                repo = JobRepository(session)
                stmt = select(JobModel).where(JobModel.status == JobStatus.DEAD_LETTER).limit(1)
                dl_job = await session.scalar(stmt)
                if dl_job:
                    await repo.retry_job(dl_job.id)

        self.loop.run_until_complete(_run())

    # -------------------------------------------------------------------------
    # SECTION 5.2 INVARIANTS (ASSERTED AFTER EVERY RULE EXECUTION)
    # -------------------------------------------------------------------------

    @invariant()
    def invariant_1_no_lost_jobs(self) -> None:
        """Invariant 1: No lost jobs — every created job exists in the DB."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(JobModel.id)
                db_ids = set((await session.scalars(stmt)).all())
                for job_id in self.known_jobs:
                    assert job_id in db_ids, f"Job {job_id} vanished from database"

        self.loop.run_until_complete(_check())

    @invariant()
    def invariant_2_single_owner_per_lease(self) -> None:
        """Invariant 2: Single owner per lease — only RUNNING jobs have active leases."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(JobModel)
                jobs = (await session.scalars(stmt)).all()
                for j in jobs:
                    if j.status == JobStatus.RUNNING:
                        assert j.lease_owner is not None, f"RUNNING job {j.id} missing lease_owner"
                        assert j.lease_expires_at is not None, (
                            f"RUNNING job {j.id} missing lease_expires_at"
                        )
                        assert j.fencing_token > 0, f"RUNNING job {j.id} has invalid fencing_token"
                    else:
                        assert j.lease_owner is None, (
                            f"Non-running job {j.id} ({j.status}) has lease_owner={j.lease_owner}"
                        )
                        assert j.lease_expires_at is None, (
                            f"Non-running job {j.id} ({j.status}) has lease_expires_at={j.lease_expires_at}"
                        )

        self.loop.run_until_complete(_check())

    @invariant()
    def invariant_3_monotonic_fencing_token(self) -> None:
        """Invariant 3: Monotonic fencing tokens — token never decreases."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(JobModel)
                jobs = (await session.scalars(stmt)).all()
                for j in jobs:
                    prev_token = self.known_jobs.get(j.id, 0)
                    assert j.fencing_token >= prev_token, (
                        f"Job {j.id} fencing_token regressed from {prev_token} to {j.fencing_token}"
                    )
                    self.known_jobs[j.id] = j.fencing_token

        self.loop.run_until_complete(_check())

    @invariant()
    def invariant_4_single_succeeded_commit(self) -> None:
        """Invariant 4: Single commit — at most one SUCCEEDED execution per job."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(ExecutionModel)
                executions = (await session.scalars(stmt)).all()
                succeeded_by_job: dict[uuid.UUID, int] = {}
                for e in executions:
                    if e.outcome == ExecutionOutcome.SUCCEEDED.value:
                        succeeded_by_job[e.job_id] = succeeded_by_job.get(e.job_id, 0) + 1

                for j_id, count in succeeded_by_job.items():
                    assert count <= 1, (
                        f"Job {j_id} committed multiple SUCCEEDED executions (count={count})"
                    )

                # If job status is SUCCEEDED, exactly one SUCCEEDED execution must exist
                job_stmt = select(JobModel).where(JobModel.status == JobStatus.SUCCEEDED)
                succeeded_jobs = (await session.scalars(job_stmt)).all()
                for sj in succeeded_jobs:
                    assert succeeded_by_job.get(sj.id, 0) == 1, (
                        f"Job {sj.id} marked SUCCEEDED but has {succeeded_by_job.get(sj.id, 0)} SUCCEEDED executions"
                    )

        self.loop.run_until_complete(_check())

    @invariant()
    def invariant_5_no_stale_writes_accepted(self) -> None:
        """Invariant 5: No stale writes — rejected stale attempts never overwrite result."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(ExecutionModel).where(
                    ExecutionModel.outcome == ExecutionOutcome.REJECTED_STALE.value
                )
                stale_executions = (await session.scalars(stmt)).all()
                for se in stale_executions:
                    job = await session.get(JobModel, se.job_id)
                    if job:
                        # Job fencing token must be strictly greater than rejected stale attempt
                        assert job.fencing_token > se.fencing_token

        self.loop.run_until_complete(_check())

    @invariant()
    def invariant_6_retry_bounds(self) -> None:
        """Invariant 6: Retry bounds — attempt never exceeds max_retries + 1."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(JobModel)
                jobs = (await session.scalars(stmt)).all()
                for j in jobs:
                    assert j.attempt <= j.max_retries + 1, (
                        f"Job {j.id} attempt {j.attempt} exceeded max_retries {j.max_retries} + 1"
                    )
                    if j.attempt > j.max_retries and j.status not in (
                        JobStatus.RUNNING,
                        JobStatus.CANCELLED,
                    ):
                        assert j.status == JobStatus.DEAD_LETTER, (
                            f"Job {j.id} exhausted retries (attempt={j.attempt}, max={j.max_retries}) but status is {j.status}"
                        )

        self.loop.run_until_complete(_check())

    @invariant()
    def invariant_7_terminal_is_final(self) -> None:
        """Invariant 7: Terminal is final — SUCCEEDED and CANCELLED states are immutable."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(JobModel)
                jobs = (await session.scalars(stmt)).all()
                for j in jobs:
                    if j.id in self.terminal_jobs:
                        expected_status = self.terminal_jobs[j.id]
                        assert j.status == expected_status, (
                            f"Terminal job {j.id} changed state from {expected_status} to {j.status}"
                        )
                    elif j.status in (JobStatus.SUCCEEDED, JobStatus.CANCELLED):
                        self.terminal_jobs[j.id] = j.status

        self.loop.run_until_complete(_check())

    @invariant()
    def invariant_8_consistent_execution_history(self) -> None:
        """Invariant 8: Consistent execution history — open executions correspond to RUNNING jobs."""

        async def _check() -> None:
            async with self.session_factory() as session:
                stmt = select(ExecutionModel).where(ExecutionModel.finished_at.is_(None))
                open_executions = (await session.scalars(stmt)).all()
                for oe in open_executions:
                    job = await session.get(JobModel, oe.job_id)
                    assert job is not None, (
                        f"Open execution {oe.id} references missing job {oe.job_id}"
                    )
                    assert job.status == JobStatus.RUNNING, (
                        f"Open execution {oe.id} exists for job {job.id} in non-RUNNING state ({job.status})"
                    )
                    assert job.fencing_token == oe.fencing_token, (
                        f"Open execution token {oe.fencing_token} does not match job token {job.fencing_token}"
                    )

        self.loop.run_until_complete(_check())


# Test runner discovered by pytest
TestJobStateMachine = JobStateMachine.TestCase
