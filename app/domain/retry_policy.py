import random
from collections.abc import Callable
from dataclasses import dataclass

from app.domain.errors import NonRetryableError, RetryableError


@dataclass(frozen=True)
class RetryPolicy:
    """
    Configurable exponential backoff retry policy with full jitter (ADR-5).
    Formula:
        delay = min(max_delay, base_delay * 2^(attempt - 1))
        delay = uniform(0, delay)  # if full_jitter is True
    """

    base_delay: float = 1.0
    max_delay: float = 60.0
    max_retries: int = 3
    full_jitter: bool = True

    def compute_delay(
        self,
        attempt: int,
        rng_fn: Callable[[], float] | None = None,
    ) -> float:
        """Computes backoff delay using this policy configuration."""
        return compute_delay(
            attempt=attempt,
            base_delay=self.base_delay,
            max_delay=self.max_delay,
            full_jitter=self.full_jitter,
            rng_fn=rng_fn,
        )


def compute_delay(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    full_jitter: bool = True,
    rng_fn: Callable[[], float] | None = None,
    policy: RetryPolicy | None = None,
) -> float:
    """
    Pure function computing backoff delay in seconds.
    Can be configured via explicit arguments or a RetryPolicy instance.

    Args:
        attempt: Current execution attempt number (1-based: 1, 2, 3, ...).
        base_delay: Initial delay in seconds for the first retry.
        max_delay: Upper ceiling on backoff delay in seconds.
        full_jitter: Whether to apply AWS-style full jitter uniform(0, delay).
        rng_fn: Optional deterministic random provider returning a float in [0.0, 1.0].
        policy: Optional RetryPolicy instance overriding individual parameter defaults.

    Returns:
        Computed delay in seconds as a float.
    """
    if policy is not None:
        base_delay = policy.base_delay
        max_delay = policy.max_delay
        full_jitter = policy.full_jitter

    effective_attempt = max(1, attempt)
    # Cap exponent at 30 to prevent integer/float overflow
    power = min(effective_attempt - 1, 30)
    temp = min(max_delay, base_delay * (2.0**power))

    if not full_jitter:
        return float(temp)

    if rng_fn is not None:
        factor = rng_fn()
        # Bound factor between 0.0 and 1.0
        clamped_factor = max(0.0, min(1.0, float(factor)))
        return float(clamped_factor * temp)

    return float(random.uniform(0.0, temp))


NON_RETRYABLE_EXCEPTIONS = (
    NonRetryableError,
    TypeError,
    SyntaxError,
    NotImplementedError,
)

RETRYABLE_EXCEPTIONS = (
    RetryableError,
    TimeoutError,
    ConnectionError,
    OSError,
)


def is_retryable(exc: Exception) -> bool:
    """
    Classifies whether an exception is retryable (transient) or non-retryable (permanent/poison pill).

    - Non-retryable: NonRetryableError, payload/programming errors (TypeError, SyntaxError, etc.)
    - Retryable: RetryableError, standard network/transient exceptions (TimeoutError, ConnectionError, OSError),
      or general unexpected runtime exceptions.
    """
    if isinstance(exc, NON_RETRYABLE_EXCEPTIONS):
        return False
    if isinstance(exc, RETRYABLE_EXCEPTIONS):
        return True
    # By default, unexpected general exceptions are retryable up to max_retries
    return True
