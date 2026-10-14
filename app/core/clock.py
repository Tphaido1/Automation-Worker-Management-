import time
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    """Protocol for monotonic and wall clock time provider."""

    def now(self) -> datetime:
        """Returns current UTC datetime."""
        ...

    def monotonic(self) -> float:
        """Returns monotonic clock in seconds (float)."""
        ...


class SystemClock:
    """Production clock using real system wall time and monotonic timer."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()


class FrozenClock:
    """Deterministic clock for unit testing and time-travel simulation."""

    def __init__(
        self, initial_time: datetime | None = None, initial_monotonic: float = 0.0
    ) -> None:
        self._current_time: datetime = initial_time or datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        self._current_monotonic: float = initial_monotonic

    def now(self) -> datetime:
        return self._current_time

    def monotonic(self) -> float:
        return self._current_monotonic

    def advance(self, seconds: float) -> None:
        """Advance both wall clock and monotonic time by the given number of seconds."""
        from datetime import timedelta

        self._current_time += timedelta(seconds=seconds)
        self._current_monotonic += seconds
