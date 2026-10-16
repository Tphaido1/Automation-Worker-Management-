from hypothesis import given, settings
from hypothesis import strategies as st

from app.domain.errors import NonRetryableError, RetryableError
from app.domain.retry_policy import (
    RetryPolicy,
    compute_delay,
    is_retryable,
)


def test_exponential_backoff_deterministic_values() -> None:
    """Verifies DoD formula: 5s -> 10s -> 20s exponential progression without jitter."""
    policy = RetryPolicy(base_delay=5.0, max_delay=60.0, full_jitter=False)

    delay_1 = policy.compute_delay(attempt=1)
    delay_2 = policy.compute_delay(attempt=2)
    delay_3 = policy.compute_delay(attempt=3)
    delay_4 = policy.compute_delay(attempt=4)

    assert delay_1 == 5.0
    assert delay_2 == 10.0
    assert delay_3 == 20.0
    assert delay_4 == 40.0


def test_max_delay_capping() -> None:
    """Verifies that computed delay never exceeds max_delay."""
    policy = RetryPolicy(base_delay=10.0, max_delay=25.0, full_jitter=False)

    delay_3 = policy.compute_delay(attempt=3)  # 10 * 2^2 = 40 -> capped at 25.0
    delay_10 = policy.compute_delay(attempt=10)

    assert delay_3 == 25.0
    assert delay_10 == 25.0


def test_deterministic_rng_provider() -> None:
    """Verifies that an external rng_fn controls the jitter ratio deterministically."""
    policy = RetryPolicy(base_delay=10.0, max_delay=60.0, full_jitter=True)

    # With attempt=2, ceiling is 10 * 2^1 = 20.0
    delay_half = policy.compute_delay(attempt=2, rng_fn=lambda: 0.5)
    assert delay_half == 10.0

    delay_zero = policy.compute_delay(attempt=2, rng_fn=lambda: 0.0)
    assert delay_zero == 0.0

    delay_full = policy.compute_delay(attempt=2, rng_fn=lambda: 1.0)
    assert delay_full == 20.0


def test_full_jitter_range() -> None:
    """Verifies that with default random jitter, delay is within [0, ceiling]."""
    for _ in range(50):
        delay = compute_delay(attempt=3, base_delay=2.0, max_delay=50.0, full_jitter=True)
        # Ceiling is 2.0 * 2^2 = 8.0
        assert 0.0 <= delay <= 8.0


@settings(max_examples=100)
@given(
    attempt=st.integers(min_value=1, max_value=20),
    base_delay=st.floats(min_value=0.1, max_value=10.0),
    max_delay=st.floats(min_value=10.0, max_value=300.0),
)
def test_hypothesis_delay_invariants(attempt: int, base_delay: float, max_delay: float) -> None:
    """
    Property test: for any valid attempt and delay parameters,
    the delay must be bounded: 0 <= delay <= max_delay.
    """
    delay = compute_delay(
        attempt=attempt,
        base_delay=base_delay,
        max_delay=max_delay,
        full_jitter=True,
    )
    assert 0.0 <= delay <= max_delay + 1e-9


@settings(max_examples=50)
@given(
    attempt=st.integers(min_value=1, max_value=15),
    base_delay=st.floats(min_value=0.5, max_value=5.0),
    max_delay=st.floats(min_value=50.0, max_value=500.0),
)
def test_hypothesis_monotonic_ceiling(attempt: int, base_delay: float, max_delay: float) -> None:
    """
    Property test: the potential delay ceiling monotonically increases with attempt
    until bounded by max_delay.
    """
    d1 = compute_delay(
        attempt=attempt, base_delay=base_delay, max_delay=max_delay, full_jitter=False
    )
    d2 = compute_delay(
        attempt=attempt + 1, base_delay=base_delay, max_delay=max_delay, full_jitter=False
    )
    assert d1 <= d2


def test_error_classification() -> None:
    """Verifies classification of retryable vs non-retryable exceptions."""
    # Explicit domain exceptions
    assert is_retryable(RetryableError("Transient DB disconnect")) is True
    assert is_retryable(NonRetryableError("Corrupted payload JSON")) is False

    # Programming/structural bugs: non-retryable
    assert is_retryable(TypeError("unsupported operand type")) is False
    assert is_retryable(SyntaxError("invalid syntax")) is False
    assert is_retryable(NotImplementedError("to be done")) is False

    # Transient system / network exceptions: retryable
    assert is_retryable(TimeoutError("Socket timed out")) is True
    assert is_retryable(ConnectionResetError("Connection reset by peer")) is True
    assert is_retryable(OSError("Host unreachable")) is True

    # General unexpected errors: retryable up to max_retries
    assert is_retryable(RuntimeError("Unexpected state")) is True
    assert is_retryable(ValueError("API returned 503")) is True
