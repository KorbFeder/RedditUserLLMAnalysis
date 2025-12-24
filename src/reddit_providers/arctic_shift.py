import asyncio
import time
import httpx
import logging
from typing import AsyncIterator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.storage.models import Submission, Comment

logger = logging.getLogger(__name__)


class ArcticShiftClient:
    """Implements RedditSource for the Arctic Shift API.

    API Docs: https://arctic-shift.photon-reddit.com
    Note: No uptime or performance guarantees from the service.

    Rate limiting uses X-RateLimit-Remaining and X-RateLimit-Reset headers.
    """

    API_URL = "https://arctic-shift.photon-reddit.com/api"

    # Shared rate limit state across all instances
    _rate_limit_lock: asyncio.Lock | None = None
    _rate_limit_remaining: int | None = None
    _rate_limit_reset: float | None = None

    def __init__(self, config: dict):
        arctic_config = config['reddit_api']['providers']['arctic_shift']
        self.batch_size: int = arctic_config.get('batch_size', 100)
        self.id_batch_size: int = arctic_config.get('id_batch_size', 500)
        self.min_delay: float = arctic_config.get('rate_limit', 0.2)  # Fallback minimum delay
        self.client = httpx.AsyncClient(timeout=30.0)

        # Initialize shared lock once
        if ArcticShiftClient._rate_limit_lock is None:
            ArcticShiftClient._rate_limit_lock = asyncio.Lock()

    @property
    def source_name(self) -> str:
        return "arctic_shift"

    async def _handle_rate_limit(self, response: httpx.Response):
        """Update rate limit state from response headers."""
        remaining = response.headers.get('X-RateLimit-Remaining')
        reset_seconds = response.headers.get('X-RateLimit-Reset')

        if remaining is not None:
            ArcticShiftClient._rate_limit_remaining = int(remaining)
        if reset_seconds is not None:
            # Reset is relative seconds - convert to absolute timestamp
            ArcticShiftClient._rate_limit_reset = time.time() + float(reset_seconds)

        # Log when running low
        if ArcticShiftClient._rate_limit_remaining is not None and ArcticShiftClient._rate_limit_remaining < 10:
            logger.warning(f"Arctic Shift rate limit low: {ArcticShiftClient._rate_limit_remaining} remaining, resets in {reset_seconds}s")

    def _calculate_delay(self) -> float:
        """Calculate adaptive delay based on remaining quota and time until reset.

        Uses proportional pacing: spreads remaining quota evenly across remaining time,
        with a 10% buffer to avoid hitting the wall, and a minimum floor delay.
        """
        remaining = ArcticShiftClient._rate_limit_remaining
        reset_at = ArcticShiftClient._rate_limit_reset

        # No rate limit info yet - use conservative default
        if remaining is None or reset_at is None:
            return self.min_delay

        now = time.time()
        time_left = max(0, reset_at - now)

        # Out of quota - wait for full reset
        if remaining <= 0:
            return time_left if time_left > 0 else self.min_delay

        # Reset happened or about to happen - minimal delay
        if time_left <= 0:
            return self.min_delay

        # Proportional pacing: spread requests evenly with 10% buffer
        usable_quota = remaining * 0.9
        if usable_quota > 0:
            calculated_delay = time_left / usable_quota
        else:
            calculated_delay = time_left

        # Clamp between floor (min_delay) and ceiling (time_left)
        return max(self.min_delay, min(calculated_delay, time_left))

    async def _wait_for_rate_limit(self):
        """Wait based on adaptive rate limit calculation."""
        async with self._rate_limit_lock:
            delay = self._calculate_delay()

            if delay > 1.0:
                logger.info(f"Arctic Shift: pacing delay {delay:.1f}s (remaining: {ArcticShiftClient._rate_limit_remaining})")

            if delay > 0:
                await asyncio.sleep(delay)

            # Reset state if we were waiting for a full reset
            if ArcticShiftClient._rate_limit_remaining is not None and ArcticShiftClient._rate_limit_remaining <= 0:
                ArcticShiftClient._rate_limit_remaining = None
                ArcticShiftClient._rate_limit_reset = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(httpx.HTTPError)
    )
    async def api_request(self, endpoint: str, params: dict):
        await self._wait_for_rate_limit()
        response = await self.client.get(f"{self.API_URL}/{endpoint}", params=params)
        await self._handle_rate_limit(response)
        response.raise_for_status()
        return response.json()

    async def stream_user_submissions(self, username: str) -> AsyncIterator[list[Submission]]:
        """Stream all submissions by a user, newest first."""
        params = {
            "author": username,
            "limit": self.batch_size,
            "sort": "desc"
        }

        logger.info(f"Fetching submissions for user {username} from Arctic Shift")
        count = 0

        while True:
            response = await self.api_request('posts/search', params)
            current_submissions = response.get('data', [])

            if not current_submissions:
                break

            # Pagination: use created_utc of last item as 'before' for next request
            params["before"] = int(current_submissions[-1]["created_utc"])
            count += len(current_submissions)

            logger.info(f"Fetched {count} submissions for user")
            yield [self._to_submission(submission) for submission in current_submissions]

    async def stream_user_comments(self, username: str) -> AsyncIterator[list[Comment]]:
        """Stream all comments by a user, newest first."""
        params = {
            "author": username,
            "limit": self.batch_size,
            "sort": "desc"
        }

        logger.info(f"Fetching comments for user {username} from Arctic Shift")
        count = 0

        while True:
            response = await self.api_request('comments/search', params)
            current_comments = response.get('data', [])

            if not current_comments:
                break

            params["before"] = int(current_comments[-1]["created_utc"])
            count += len(current_comments)

            logger.info(f"Fetched {count} comments for user")
            yield [self._to_comment(comment) for comment in current_comments]

    async def stream_submission_comments(self, submission_id: str) -> AsyncIterator[list[Comment]]:
        """Stream all comments on a submission, newest first."""
        # Arctic Shift expects link_id with t3_ prefix
        link_id = f"t3_{submission_id}" if not submission_id.startswith('t3_') else submission_id

        params = {
            "link_id": link_id,
            "limit": self.batch_size,
            "sort": "desc"
        }

        logger.info(f"Fetching comments for submission {submission_id} from Arctic Shift")
        count = 0

        while True:
            response = await self.api_request('comments/search', params)
            current_comments = response.get('data', [])

            if not current_comments:
                break

            params["before"] = int(current_comments[-1]["created_utc"])
            count += len(current_comments)

            logger.info(f"Fetched {count} comments from submission")
            yield [self._to_comment(comment) for comment in current_comments]

    async def fetch_comment(self, comment_id: str) -> Comment | None:
        """Fetch a single comment by ID."""
        clean_id = comment_id.split('_')[-1]
        params = {'ids': clean_id}
        response = await self.api_request('comments/ids', params)
        data = response.get('data', [])

        if not data:
            return None
        return self._to_comment(data[0])

    async def fetch_submission(self, submission_id: str) -> Submission | None:
        """Fetch a single submission by ID."""
        clean_id = submission_id.split('_')[-1]
        params = {'ids': clean_id}
        response = await self.api_request('posts/ids', params)
        data = response.get('data', [])

        if not data:
            return None
        return self._to_submission(data[0])

    async def fetch_submissions(self, ids: list[str]) -> list[Submission]:
        """Fetch submissions by IDs (up to 500 per request)."""
        if not ids:
            return []

        results = []
        clean_ids = [id.split('_')[-1] for id in ids]

        for i in range(0, len(clean_ids), self.id_batch_size):
            chunk = clean_ids[i:i + self.id_batch_size]
            params = {'ids': ','.join(chunk)}
            response = await self.api_request('posts/ids', params)
            data = response.get('data', [])
            results.extend(self._to_submission(s) for s in data)

        logger.info(f"Arctic Shift: fetched {len(results)}/{len(ids)} submissions")
        return results

    async def fetch_comments(self, ids: list[str]) -> list[Comment]:
        """Fetch comments by IDs (up to 500 per request)."""
        if not ids:
            return []

        results = []
        clean_ids = [id.split('_')[-1] for id in ids]

        for i in range(0, len(clean_ids), self.id_batch_size):
            chunk = clean_ids[i:i + self.id_batch_size]
            params = {'ids': ','.join(chunk)}
            response = await self.api_request('comments/ids', params)
            data = response.get('data', [])
            results.extend(self._to_comment(c) for c in data)

        logger.info(f"Arctic Shift: fetched {len(results)}/{len(ids)} comments")
        return results

    def _strip_prefix(self, reddit_id: str | None) -> str | None:
        """Remove Reddit type prefix (t1_, t3_, etc.) from ID."""
        if not isinstance(reddit_id, str):
            return None
        return reddit_id.split('_')[-1]

    def _to_submission(self, submission: dict) -> Submission:
        return Submission(
            id=submission["id"],
            raw_json=submission,
            author=submission.get('author'),
            subreddit=submission.get('subreddit'),
            title=submission.get('title'),
            selftext=submission.get('selftext'),
            url=submission.get('url'),
            score=submission.get('score'),
            ups=submission.get('ups'),
            upvote_ratio=submission.get('upvote_ratio'),
            num_comments=submission.get('num_comments'),
            gilded=submission.get('gilded'),
            all_awardings=submission.get('all_awardings'),
            created_utc=int(submission['created_utc']) if submission.get('created_utc') is not None else None
        )

    def _to_comment(self, comment: dict) -> Comment:
        return Comment(
            id=comment["id"],
            raw_json=comment,
            submission_id=self._strip_prefix(comment.get('link_id')),
            parent_id=self._strip_prefix(comment.get('parent_id')),
            author=comment.get('author'),
            body=comment.get('body'),
            score=comment.get('score'),
            ups=comment.get('ups'),
            gilded=comment.get('gilded'),
            all_awardings=comment.get('all_awardings'),
            created_utc=int(comment['created_utc']) if comment.get('created_utc') is not None else None
        )

    async def close(self):
        await self.client.aclose()
