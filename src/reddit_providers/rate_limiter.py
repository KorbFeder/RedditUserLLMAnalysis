import asyncio
import time
import logging
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

logger = logging.getLogger(__name__)


class AsyncRateLimiter:
    """Shared rate limiter that ensures minimum delay between requests.

    Thread-safe via asyncio.Lock. Can be shared across multiple client instances.
    """

    def __init__(self, min_delay: float):
        self.min_delay = min_delay
        self.lock = asyncio.Lock()
        self.last_request = 0.0

    async def acquire(self):
        """Wait until we can make a request without violating rate limit."""
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_request
            wait_time = self.min_delay - elapsed

            if wait_time > 0:
                await asyncio.sleep(wait_time)

            self.last_request = time.time()


def is_retryable_error(exception: BaseException) -> bool:
    """Determine if an exception should trigger a retry.

    Retryable:
    - 429 (rate limit)
    - 5xx (server errors)
    - Timeouts and connection errors

    Not retryable:
    - 4xx client errors (except 429)
    """
    if isinstance(exception, httpx.HTTPStatusError):
        status = exception.response.status_code
        if status == 429:
            logger.warning(f"Rate limited (429), will retry")
            return True
        if 500 <= status < 600:
            logger.warning(f"Server error ({status}), will retry")
            return True
        return False  # 4xx errors (except 429) are not retryable

    if isinstance(exception, (httpx.TimeoutException, httpx.ConnectError)):
        logger.warning(f"Network error ({type(exception).__name__}), will retry")
        return True

    return False


# Shared retry decorator for all providers
api_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=1, max=60),
    retry=retry_if_exception(is_retryable_error),
    before_sleep=lambda retry_state: logger.info(
        f"Retry attempt {retry_state.attempt_number} after {retry_state.outcome.exception()}"
    )
)
