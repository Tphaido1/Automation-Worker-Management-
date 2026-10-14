import json
import logging
from contextvars import ContextVar
from typing import Any

# Context variables for distributed tracing & correlation
ctx_job_id: ContextVar[str | None] = ContextVar("ctx_job_id", default=None)
ctx_worker_id: ContextVar[str | None] = ContextVar("ctx_worker_id", default=None)
ctx_fencing_token: ContextVar[int | None] = ContextVar("ctx_fencing_token", default=None)
ctx_attempt: ContextVar[int | None] = ContextVar("ctx_attempt", default=None)


class JSONFormatter(logging.Formatter):
    """Structured JSON formatter with context variables."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inject context variables if set
        job_id = ctx_job_id.get()
        if job_id:
            log_data["job_id"] = job_id

        worker_id = ctx_worker_id.get()
        if worker_id:
            log_data["worker_id"] = worker_id

        token = ctx_fencing_token.get()
        if token is not None:
            log_data["fencing_token"] = token

        attempt = ctx_attempt.get()
        if attempt is not None:
            log_data["attempt"] = attempt

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging(level: int = logging.INFO, as_json: bool = True) -> None:
    """Setup root logging configuration."""
    handler = logging.StreamHandler()
    if as_json:
        handler.setFormatter(JSONFormatter())
    else:
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        handler.setFormatter(logging.Formatter(fmt))

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [handler]
