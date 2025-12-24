import asyncio
import time


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
