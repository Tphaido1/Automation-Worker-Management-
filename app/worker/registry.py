from collections.abc import Awaitable, Callable
from typing import Any

from app.worker.context import JobContext

HandlerFunc = Callable[[JobContext], Awaitable[Any] | Any]


class HandlerRegistry:
    """Registry mapping job types to executable handler functions."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFunc] = {}

    def register(self, job_type: str, func: HandlerFunc | None = None) -> Any:
        """Registers a handler for a job type. Can be used as a decorator or direct call."""
        if func is not None:
            self._handlers[job_type] = func
            return func

        def decorator(fn: HandlerFunc) -> HandlerFunc:
            self._handlers[job_type] = fn
            return fn

        return decorator

    def add_handler(self, job_type: str, func: HandlerFunc) -> None:
        """Directly registers a handler function."""
        self._handlers[job_type] = func

    def get_handler(self, job_type: str) -> HandlerFunc | None:
        """Retrieves handler for a given job type."""
        return self._handlers.get(job_type)
