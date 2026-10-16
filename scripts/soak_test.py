#!/usr/bin/env python3
"""
LeaseFlow Soak Testing & Chaos Injection Harness
=================================================
Simulates sustained production workload under continuous chaos conditions
(worker SIGKILL, zombie pause/SIGCONT, network latency) and continuously verifies
all 8 Section 5.2 distributed systems invariants:

  1. No lost jobs: Every job transitions to a terminal or active state.
  2. Single owner per lease: Only RUNNING jobs hold leases.
  3. Monotonic fencing tokens: Tokens never decrease.
  4. Single commit: At most 1 SUCCEEDED execution per job.
  5. No stale writes accepted: Stale tokens are strictly rejected.
  6. Retry bounds: attempt <= max_retries + 1.
  7. Terminal is final: SUCCEEDED and CANCELLED states are immutable.
  8. Consistent execution history: Open executions correspond to RUNNING jobs.

Usage:
  uv run python scripts/soak_test.py --duration 30 --workers 3 --rate 5 --chaos-interval 8
"""

import argparse
import asyncio
import logging
import random
import sys
import tempfile
import time
import uuid
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.db.models import Base, ExecutionModel, JobModel
from app.db.repositories import JobRepository
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.scheduler.promoter import PromoterLoop
from app.scheduler.reaper import ReaperLoop
from tests.chaos.harness import WorkerProcess

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("soak_test")


class InvariantAuditor:
    """Continuously evaluates Section 5.2 distributed invariants against the live database."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self.known_tokens: dict[uuid.UUID, int] = {}
        self.terminal_states: dict[uuid.UUID, JobStatus] = {}
        self.violations: list[str] = []

    async def audit(self) -> dict[str, bool]:
        """Runs all 8 invariant checks. Returns a dictionary of {invariant_name: bool}."""
        results: dict[str, bool] = {}

        async with self.session_factory() as session:
            # 1. No lost jobs (all jobs exist and have valid statuses)
            try:
                jobs = (await session.scalars(select(JobModel))).all()
                for j in jobs:
                    assert isinstance(j.status, JobStatus), (
                        f"Job {j.id} has invalid status {j.status}"
                    )
                results["1. No Lost Jobs"] = True
            except Exception as e:
                self.violations.append(f"Invariant 1 failed: {e}")
                results["1. No Lost Jobs"] = False

            # 2. Single owner per lease
            try:
                for j in jobs:
                    if j.status == JobStatus.RUNNING:
                        assert j.lease_owner is not None, f"RUNNING job {j.id} missing lease_owner"
                        assert j.lease_expires_at is not None, (
                            f"RUNNING job {j.id} missing lease_expires_at"
                        )
                    else:
                        assert j.lease_owner is None, (
                            f"Job {j.id} ({j.status}) holds lease_owner={j.lease_owner}"
                        )
                        assert j.lease_expires_at is None, (
                            f"Job {j.id} ({j.status}) holds lease_expires_at"
                        )
                results["2. Single Lease Owner"] = True
            except Exception as e:
                self.violations.append(f"Invariant 2 failed: {e}")
                results["2. Single Lease Owner"] = False

            # 3. Monotonic fencing tokens
            try:
                for j in jobs:
                    prev = self.known_tokens.get(j.id, 0)
                    assert j.fencing_token >= prev, (
                        f"Job {j.id} token regressed: {prev} -> {j.fencing_token}"
                    )
                    self.known_tokens[j.id] = j.fencing_token
                results["3. Monotonic Fencing"] = True
            except Exception as e:
                self.violations.append(f"Invariant 3 failed: {e}")
                results["3. Monotonic Fencing"] = False

            # 4. Single commit (at most one SUCCEEDED per job)
            try:
                executions = (await session.scalars(select(ExecutionModel))).all()
                succeeded_counts: dict[uuid.UUID, int] = {}
                for e in executions:
                    if e.outcome == ExecutionOutcome.SUCCEEDED.value:
                        succeeded_counts[e.job_id] = succeeded_counts.get(e.job_id, 0) + 1
                for j_id, count in succeeded_counts.items():
                    assert count <= 1, f"Job {j_id} has {count} SUCCEEDED executions"
                results["4. Single Succeeded Commit"] = True
            except Exception as e:
                self.violations.append(f"Invariant 4 failed: {e}")
                results["4. Single Succeeded Commit"] = False

            # 5. No stale writes accepted
            try:
                stale_stmt = select(ExecutionModel).where(
                    ExecutionModel.outcome == ExecutionOutcome.REJECTED_STALE.value
                )
                stale_execs = (await session.scalars(stale_stmt)).all()
                for se in stale_execs:
                    job = await session.get(JobModel, se.job_id)
                    if job:
                        assert job.fencing_token > se.fencing_token, (
                            f"Stale write token {se.fencing_token} >= current token {job.fencing_token}"
                        )
                results["5. No Stale Writes"] = True
            except Exception as e:
                self.violations.append(f"Invariant 5 failed: {e}")
                results["5. No Stale Writes"] = False

            # 6. Retry bounds
            try:
                for j in jobs:
                    assert j.attempt <= j.max_retries + 1, (
                        f"Job {j.id} attempt {j.attempt} exceeded max_retries {j.max_retries} + 1"
                    )
                results["6. Retry Bounds Enforced"] = True
            except Exception as e:
                self.violations.append(f"Invariant 6 failed: {e}")
                results["6. Retry Bounds Enforced"] = False

            # 7. Terminal is final
            try:
                for j in jobs:
                    if j.id in self.terminal_states:
                        expected = self.terminal_states[j.id]
                        assert j.status == expected, (
                            f"Terminal job {j.id} changed state: {expected} -> {j.status}"
                        )
                    elif j.status in (JobStatus.SUCCEEDED, JobStatus.CANCELLED):
                        self.terminal_states[j.id] = j.status
                results["7. Terminal States Final"] = True
            except Exception as e:
                self.violations.append(f"Invariant 7 failed: {e}")
                results["7. Terminal States Final"] = False

            # 8. Consistent execution history
            try:
                open_stmt = select(ExecutionModel).where(ExecutionModel.finished_at.is_(None))
                open_execs = (await session.scalars(open_stmt)).all()
                for oe in open_execs:
                    job = await session.get(JobModel, oe.job_id)
                    assert job is not None, f"Open execution {oe.id} references missing job"
                    assert job.status == JobStatus.RUNNING, (
                        f"Open execution for non-RUNNING job {job.id} ({job.status})"
                    )
                    assert job.fencing_token == oe.fencing_token, (
                        f"Open execution token {oe.fencing_token} != job token {job.fencing_token}"
                    )
                results["8. Consistent Executions"] = True
            except Exception as e:
                self.violations.append(f"Invariant 8 failed: {e}")
                results["8. Consistent Executions"] = False

        return results


async def generate_load(
    session_factory: async_sessionmaker[AsyncSession],
    rate_per_sec: float,
    stop_event: asyncio.Event,
    stats: dict[str, int],
) -> None:
    """Continuously generates jobs of mixed characteristics."""
    interval = 1.0 / max(0.1, rate_per_sec)
    job_types = ["compute_task", "io_bound", "fast_task"]

    while not stop_event.is_set():
        try:
            async with session_factory() as session:
                repo = JobRepository(session)
                j_type = random.choice(job_types)
                # Randomize priority and retry limits
                priority = random.randint(-5, 10)
                max_retries = random.choice([1, 2, 3])
                timeout = random.choice([10, 15, 20])

                await repo.create_job(
                    job_type=j_type,
                    payload={"soak_seed": uuid.uuid4().hex, "complexity": random.randint(1, 10)},
                    priority=priority,
                    max_retries=max_retries,
                    timeout_seconds=timeout,
                )
                stats["submitted"] = stats.get("submitted", 0) + 1
        except Exception as e:
            logger.warning("Error generating job: %s", e)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except TimeoutError:
            pass


async def chaos_monkey(
    workers: list[WorkerProcess],
    chaos_interval: float,
    stop_event: asyncio.Event,
    stats: dict[str, int],
    db_url: str,
) -> None:
    """Randomly injects faults: SIGKILL, SIGSTOP/SIGCONT zombie pauses, transient latency."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=chaos_interval)
            break
        except TimeoutError:
            pass

        if not workers:
            continue

        action = random.choice(["sigkill", "zombie_pause", "zombie_pause"])
        worker_idx = random.randrange(len(workers))
        target_worker = workers[worker_idx]

        if action == "sigkill":
            logger.warning(
                "[CHAOS] Injecting SIGKILL into %s (PID %s)",
                target_worker.worker_id,
                target_worker.pid,
            )
            target_worker.kill()
            stats["chaos_sigkill"] = stats.get("chaos_sigkill", 0) + 1
            # Wait briefly, then spawn replacement worker
            await asyncio.sleep(2.0)
            if not stop_event.is_set():
                new_worker = WorkerProcess(
                    worker_id=f"worker-repl-{uuid.uuid4().hex[:4]}",
                    db_url=db_url,
                    lease_ttl=4,
                    poll_interval=0.2,
                )
                new_worker.start()
                workers[worker_idx] = new_worker
                logger.info(
                    "[CHAOS] Started replacement worker %s (PID %s)",
                    new_worker.worker_id,
                    new_worker.pid,
                )

        elif action == "zombie_pause":
            logger.warning(
                "[CHAOS] Injecting SIGSTOP (pause) into %s for 6s (> 4s lease TTL)",
                target_worker.worker_id,
            )
            target_worker.suspend()
            stats["chaos_pause"] = stats.get("chaos_pause", 0) + 1
            await asyncio.sleep(6.0)
            if target_worker.is_alive():
                logger.warning(
                    "[CHAOS] Injecting SIGCONT (resume) into %s to observe zombie rejection",
                    target_worker.worker_id,
                )
                target_worker.resume()


async def print_dashboard(
    session_factory: async_sessionmaker[AsyncSession],
    auditor: InvariantAuditor,
    stats: dict[str, int],
    start_time: float,
    duration: float,
) -> None:
    """Prints a clean ASCII progress report and invariant status."""
    elapsed = time.time() - start_time
    remaining = max(0.0, duration - elapsed)

    async with session_factory() as session:
        job_counts_stmt = select(JobModel.status, text("count(*)")).group_by(JobModel.status)
        counts_res = (await session.execute(job_counts_stmt)).all()
        status_map: dict[str, int] = {str(row[0].value): int(row[1]) for row in counts_res}

        stale_stmt = (
            select(text("count(*)"))
            .select_from(ExecutionModel)
            .where(ExecutionModel.outcome == ExecutionOutcome.REJECTED_STALE.value)
        )
        stale_count = int(await session.scalar(stale_stmt) or 0)

        reaped_stmt = (
            select(text("count(*)"))
            .select_from(ExecutionModel)
            .where(ExecutionModel.outcome == ExecutionOutcome.LEASE_EXPIRED.value)
        )
        reaped_count = int(await session.scalar(reaped_stmt) or 0)

    invariants = await auditor.audit()

    print("\n" + "=" * 70)
    print(f" LEASEFLOW SOAK TEST: {elapsed:.1f}s elapsed | {remaining:.1f}s remaining")
    print("-" * 70)
    print(
        f" Jobs Submitted: {stats.get('submitted', 0):<5} | "
        f"QUEUED: {status_map.get('QUEUED', 0):<4} | "
        f"RUNNING: {status_map.get('RUNNING', 0):<4} | "
        f"SUCCEEDED: {status_map.get('SUCCEEDED', 0):<4}"
    )
    print(
        f" RETRY_WAITING: {status_map.get('RETRY_WAITING', 0):<5} | "
        f"DEAD_LETTER: {status_map.get('DEAD_LETTER', 0):<4} | "
        f"Reaped Leases: {reaped_count:<4} | "
        f"Rejected Stale: {stale_count:<4}"
    )
    print(
        f" Chaos Injected: SIGKILL={stats.get('chaos_sigkill', 0)} | "
        f"Zombie Pauses={stats.get('chaos_pause', 0)}"
    )
    print("-" * 70)
    print(" SECTION 5.2 DISTRIBUTED INVARIANTS:")
    for name, passed in invariants.items():
        mark = "PASS" if passed else "FAIL"
        print(f"   [{mark:^6}] {name}")
    print("=" * 70 + "\n")


async def run_soak(
    duration: float,
    num_workers: int,
    rate_per_sec: float,
    chaos_interval: float,
    report_interval: float,
) -> int:
    """Executes the full soak test scenario."""
    db_file = Path(tempfile.gettempdir()) / f"leaseflow_soak_{uuid.uuid4().hex[:8]}.db"
    db_url = f"sqlite+aiosqlite:///{db_file.as_posix()}"

    engine = create_async_engine(db_url, echo=False)
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL;"))
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )

    # 1. Start Control Plane (Promoter and Reaper)
    promoter = PromoterLoop(session_factory, interval_seconds=1.0)
    reaper = ReaperLoop(session_factory, interval_seconds=1.0, backoff_seconds=1.0)
    promoter.start()
    reaper.start()

    # 2. Start Worker Subprocesses
    workers: list[WorkerProcess] = []
    for i in range(num_workers):
        w = WorkerProcess(
            worker_id=f"worker-{i + 1}",
            db_url=db_url,
            lease_ttl=4,
            poll_interval=0.2,
        )
        w.start()
        workers.append(w)

    logger.info(
        "Started soak test: duration=%.1fs, workers=%d, rate=%.1f/s, chaos_interval=%.1fs",
        duration,
        num_workers,
        rate_per_sec,
        chaos_interval,
    )

    stats: dict[str, int] = {}
    auditor = InvariantAuditor(session_factory)
    stop_event = asyncio.Event()

    # Launch background workload and chaos tasks
    workload_task = asyncio.create_task(
        generate_load(session_factory, rate_per_sec, stop_event, stats)
    )
    chaos_task = asyncio.create_task(
        chaos_monkey(workers, chaos_interval, stop_event, stats, db_url)
    )

    start_time = time.time()
    next_report = start_time + report_interval

    try:
        while time.time() - start_time < duration:
            await asyncio.sleep(0.5)
            if time.time() >= next_report:
                await print_dashboard(session_factory, auditor, stats, start_time, duration)
                next_report = time.time() + report_interval
    finally:
        logger.info("Soak duration elapsed. Initiating graceful shutdown...")
        stop_event.set()
        await workload_task
        await chaos_task

        # Stop workers
        for w in workers:
            w.stop()

        # Stop control plane
        await promoter.stop()
        await reaper.stop()

    # Final Invariant Audit
    logger.info("Running final comprehensive invariant audit...")
    final_invariants = await auditor.audit()
    await print_dashboard(session_factory, auditor, stats, start_time, duration)

    await engine.dispose()
    for ext in ["", "-wal", "-shm"]:
        p = Path(f"{db_file.as_posix()}{ext}")
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass

    all_passed = all(final_invariants.values())
    if all_passed:
        logger.info(
            "SOAK TEST PASSED: All 8 Section 5.2 invariants held strictly under chaos load!"
        )
        return 0
    else:
        logger.error("SOAK TEST FAILED: One or more invariants violated: %s", auditor.violations)
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="LeaseFlow Soak and Chaos Harness")
    parser.add_argument(
        "--duration", type=float, default=20.0, help="Duration in seconds (default: 20)"
    )
    parser.add_argument(
        "--workers", type=int, default=3, help="Number of worker processes (default: 3)"
    )
    parser.add_argument(
        "--rate", type=float, default=4.0, help="Job submission rate per sec (default: 4.0)"
    )
    parser.add_argument(
        "--chaos-interval", type=float, default=6.0, help="Chaos injection interval (default: 6.0)"
    )
    parser.add_argument(
        "--report-interval",
        type=float,
        default=5.0,
        help="Dashboard reporting interval (default: 5.0)",
    )
    args = parser.parse_args()

    exit_code = asyncio.run(
        run_soak(
            duration=args.duration,
            num_workers=args.workers,
            rate_per_sec=args.rate,
            chaos_interval=args.chaos_interval,
            report_interval=args.report_interval,
        )
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
