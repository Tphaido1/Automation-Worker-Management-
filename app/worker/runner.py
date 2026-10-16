import argparse
import asyncio
import logging
import signal
import sys
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.queue.postgres import PostgresQueueBackend
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker

logger = logging.getLogger(__name__)


async def default_sleep_handler(ctx: JobContext) -> dict[str, Any]:
    """Simulates a workload that sleeps for specified duration."""
    duration = float(ctx.payload.get("duration", 1.0))
    logger.info("Executing sleep_job for %.2fs (job_id=%s)", duration, ctx.job_id)
    await ctx.sleep(duration)
    return {"status": "slept", "duration": duration}


async def default_echo_handler(ctx: JobContext) -> dict[str, Any]:
    """Echoes input payload."""
    return {"status": "echoed", "payload": ctx.payload}


async def default_fail_handler(ctx: JobContext) -> dict[str, Any]:
    """Always raises an exception to test error handling and retry."""
    err_msg = ctx.payload.get("error", "Simulated task failure")
    raise RuntimeError(err_msg)


async def default_side_effect_handler(ctx: JobContext) -> dict[str, Any]:
    """Handler that produces transactional outbox events."""
    email = ctx.payload.get("email", "test@example.com")
    ctx.emit_outbox(
        kind="SEND_EMAIL",
        payload={"to": email, "subject": "Job Done"},
        dedup_key=f"email-{ctx.job_id}",
    )
    return {"status": "dispatched", "email": email}


def build_default_registry() -> HandlerRegistry:
    """Builds standard handler registry with common test and benchmark tasks."""
    registry = HandlerRegistry()
    registry.register("sleep_job", default_sleep_handler)
    registry.register("echo_job", default_echo_handler)
    registry.register("fail_job", default_fail_handler)
    registry.register("side_effect_job", default_side_effect_handler)
    return registry


async def run_worker(
    worker_id: str,
    db_url: str,
    lease_ttl: int = 30,
    concurrency: int = 1,
    poll_interval: float = 0.2,
) -> None:
    """Entrypoint function to run a standalone worker instance."""
    logger.info("Starting worker '%s' connecting to '%s' (ttl=%ds)", worker_id, db_url, lease_ttl)

    # Configure database engine
    is_sqlite = db_url.startswith("sqlite")
    connect_args: dict[str, Any] = {"check_same_thread": False} if is_sqlite else {}
    pool_class = StaticPool if db_url == "sqlite+aiosqlite:///:memory:" else None

    engine_kwargs: dict[str, Any] = {"echo": False, "connect_args": connect_args}
    if pool_class:
        engine_kwargs["poolclass"] = pool_class

    engine = create_async_engine(db_url, **engine_kwargs)
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )

    queue = PostgresQueueBackend(session_factory)
    registry = build_default_registry()

    worker = Worker(
        worker_id=worker_id,
        queue=queue,
        registry=registry,
        lease_ttl=lease_ttl,
        concurrency=concurrency,
        poll_interval=poll_interval,
    )

    stop_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info("Shutdown signal received; stopping worker '%s'...", worker_id)
        stop_event.set()

    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, handle_signal)
    else:
        # On Windows, register SIGINT if possible
        try:
            signal.signal(signal.SIGINT, lambda *_: handle_signal())
        except Exception:
            pass

    worker.start()
    logger.info("Worker '%s' running with concurrency=%d", worker_id, concurrency)

    try:
        await stop_event.wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Worker loop cancelled")
    finally:
        await worker.stop()
        await engine.dispose()
        logger.info("Worker '%s' stopped cleanly", worker_id)


def main() -> None:
    """CLI parser for worker runner."""
    settings = get_settings()
    setup_logging(level=logging.INFO, as_json=True)

    parser = argparse.ArgumentParser(description="LeaseFlow Distributed Worker Process")
    parser.add_argument("--worker-id", type=str, default=None, help="Unique worker identifier")
    parser.add_argument(
        "--db-url",
        type=str,
        default=settings.DATABASE_URL,
        help="Database connection URI",
    )
    parser.add_argument("--lease-ttl", type=int, default=15, help="Lease TTL in seconds")
    parser.add_argument("--concurrency", type=int, default=1, help="Worker concurrency")
    parser.add_argument("--poll-interval", type=float, default=0.2, help="Poll interval in seconds")

    args = parser.parse_args()
    worker_id = args.worker_id or f"worker-{argparse.Namespace.__name__}"

    try:
        asyncio.run(
            run_worker(
                worker_id=worker_id,
                db_url=args.db_url,
                lease_ttl=args.lease_ttl,
                concurrency=args.concurrency,
                poll_interval=args.poll_interval,
            )
        )
    except KeyboardInterrupt:
        logger.info("Worker process exited via KeyboardInterrupt")


if __name__ == "__main__":
    main()
