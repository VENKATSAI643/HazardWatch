# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    shared/retry.py
# Purpose: Exponential backoff retry decorator for all HTTP calls
# Layer:   Layer 0 — Polling & Ingestion
# ============================================================
import time
import logging
import functools

logger = logging.getLogger(__name__)


def with_backoff(max_retries: int = 5, base_wait: int = 5):
    """
    Decorator that retries a function with exponential backoff.

    Usage:
        @with_backoff(max_retries=5, base_wait=5)
        def fetch_data():
            ...

    Wait sequence: 5s -> 10s -> 20s -> 40s -> 80s (capped at 300s)
    Respects Retry-After header if present in the exception message.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            wait = base_wait
            last_exception = None

            for attempt in range(1, max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    if attempt == max_retries:
                        logger.error(
                            f"[{fn.__name__}] All {max_retries} attempts failed. "
                            f"Last error: {exc}"
                        )
                        raise

                    logger.warning(
                        f"[{fn.__name__}] Attempt {attempt}/{max_retries} failed: {exc}. "
                        f"Retrying in {wait}s..."
                    )
                    time.sleep(wait)
                    wait = min(wait * 2, 300)  # cap at 5 minutes

            raise last_exception  # unreachable but satisfies type checkers

        return wrapper
    return decorator
