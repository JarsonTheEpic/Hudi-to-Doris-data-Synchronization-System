"""
Module: retry.py
Function: Provide a retry decorator for functions that may fail transiently.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

Why need a retry decorator?
-------------------------------------------------------------------------
1. Database operations may fail temporarily (network jitter, connection timeout)
2. Hudi table compaction may cause short delays
3. Doris may be busy during peak hours
4. Retry with exponential backoff gives the system time to recover

Usage:
    @retry(max_attempts=3, delay=5)
    def sync_data():
        # operation that may fail transiently
        pass
"""

import time
import functools
import logging

logger = logging.getLogger(__name__)


def retry(max_attempts: int = 3, delay: int = 5, backoff: int = 2):
    """
    Decorator that retries a function if it raises an exception.

    Args:
        max_attempts: Maximum number of attempts (including the first try).
                      Default: 3
        delay: Initial delay in seconds between attempts. Default: 5
        backoff: Multiplier for exponential backoff.
                 Each retry waits: delay * (backoff ^ attempt)
                 Default: 2 (5s, 10s, 20s...)

    Returns:
        Decorated function with retry logic

    Example:
        @retry(max_attempts=3, delay=5)
        def sync_table():
            # If this fails, it will retry up to 2 more times
            # Wait 5s, then 10s, then 20s between attempts
            pass

    Why exponential backoff?
        - Gives the system more time to recover as failures persist
        - Prevents flooding the system with repeated requests
        - Standard practice for distributed systems
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Store the last exception to re-raise if all attempts fail
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    # Attempt to execute the function
                    return func(*args, **kwargs)

                except Exception as e:
                    # Save the exception
                    last_exception = e

                    # If this was the last attempt, don't wait, just raise
                    if attempt == max_attempts - 1:
                        break

                    # Calculate wait time with exponential backoff
                    # attempt=0: delay, attempt=1: delay*backoff, attempt=2: delay*backoff^2
                    wait_time = delay * (backoff ** attempt)

                    logger.warning(
                        f'Function {func.__name__} failed on attempt {attempt + 1}/{max_attempts}. '
                        f'Error: {e}. Retrying in {wait_time}s...'
                    )

                    # Wait before retrying
                    time.sleep(wait_time)

            # All attempts failed, raise the last exception
            logger.error(
                f'Function {func.__name__} failed after {max_attempts} attempts. '
                f'Last error: {last_exception}'
            )
            raise last_exception

        return wrapper
    return decorator


def retry_on_connection_error(max_attempts: int = 3, delay: int = 5):
    """
    Specialized retry decorator for database connection errors.
    Same as @retry but with a more descriptive name for readability.

    Usage:
        @retry_on_connection_error(max_attempts=3, delay=5)
        def query_database():
            # database query that may fail due to connection issues
            pass
    """
    return retry(max_attempts=max_attempts, delay=delay)