import httpx
import os
import asyncio
import base64
import time
from typing import AsyncIterator
import logging

from src.storage.models import Submission, Comment
from src.reddit_providers.rate_limiter import api_retry

logger = logging.getLogger(__name__)


class AsyncRateLimiter:
    """Shared rate limiter that ensures minimum delay between requests."""

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

    def update_delay(self, new_delay: float):
        """Update the minimum delay (e.g., after authentication)."""
        self.min_delay = new_delay


class RedditClient:
    # Shared rate limiter across all instances
    _rate_limiter: AsyncRateLimiter | None = None

    def __init__(self, config: dict, user_agent: str = "SentimentAgent/1.0"):
        self.user_agent = user_agent
        self.client = httpx.AsyncClient(
            headers={"User-Agent": user_agent},
            timeout=httpx.Timeout(30.0)  # 30s timeout (default is 5s)
        )

        self.reddit_config = config["reddit_api"]["providers"]["reddit"]
        self._reddit_id = os.getenv("REDDIT_ID")
        self._reddit_secret = os.getenv("REDDIT_SECRET")

        self.base_url = "https://www.reddit.com"
        self.authenticated = False
        self._token_expires_at = 0
        self._auth_initialized = False

        # Initialize shared rate limiter once (start with unauthenticated rate)
        if RedditClient._rate_limiter is None:
            rate_limit = float(self.reddit_config.get("rate_limit_no_key", "6.0"))
            RedditClient._rate_limiter = AsyncRateLimiter(rate_limit)

    @property
    def source_name(self: "RedditClient") -> str:
        return "reddit"

    async def _ensure_auth(self):
        """Lazy authentication on first request."""
        if self._auth_initialized:
            return
        if self._reddit_id and self._reddit_secret:
            await self._authenticate()
            # Update shared rate limiter to faster authenticated rate
            new_rate = float(self.reddit_config.get("rate_limit_key", "0.5"))
            self._rate_limiter.update_delay(new_rate)
            self.base_url = "https://oauth.reddit.com"
            self.authenticated = True
        self._auth_initialized = True

    async def stream_user_submissions(self: "RedditClient", username: str) -> AsyncIterator[list[Submission]]:
        """Fetch user submissions using sort=top&t=all for maximum coverage.

        Testing shows sort=top returns the most results - other sorts (hot, new,
        controversial) are subsets of top's results.
        """
        after = None
        count = 0

        while True:
            params = {"limit": 100, "sort": "top", "t": "all"}
            if after:
                params["after"] = after

            response = await self._get(f"user/{username}/submitted", params)
            children = response["data"]["children"]

            if not children:
                break

            count += len(children)
            yield [self._to_submission(s["data"]) for s in children]

            after = response["data"].get("after")
            if not after:
                break

        logger.info(f"Fetched {count} submissions for user")

    async def stream_submission_comments(self: "RedditClient", submission_id: str) -> AsyncIterator[list[Comment]]:
        response = await self._get(f"comments/{submission_id}", {"limit": 500})
        comments_data = response[1]["data"]["children"]

        comments = [
            self._to_comment(c["data"])
            for c in comments_data
            if c["kind"] == "t1"
        ]
        if comments:
            yield comments

    async def stream_user_comments(self: "RedditClient", username: str) -> AsyncIterator[list[Comment]]:
        """Fetch user comments using sort=top&t=all for maximum coverage.

        Testing shows sort=top returns the most results - other sorts (hot, new,
        controversial) are subsets of top's results.
        """
        after = None
        count = 0

        while True:
            params = {"limit": 100, "sort": "top", "t": "all"}
            if after:
                params["after"] = after

            response = await self._get(f"user/{username}/comments", params)
            children = response["data"]["children"]

            if not children:
                break

            count += len(children)
            yield [self._to_comment(c["data"]) for c in children]

            after = response["data"].get("after")
            if not after:
                break

        logger.info(f"Fetched {count} comments for user") 
    
    async def fetch_submissions(self: "RedditClient", ids: list[str]) -> list[Submission]:
        """Fetch submissions by ID (no prefix needed)."""
        if not ids:
            return []
        fullnames = [f"t3_{id.split('_')[-1]}" for id in ids]
        subs, _ = await self._fetch_bulk(fullnames)
        return subs

    async def fetch_comments(self: "RedditClient", ids: list[str]) -> list[Comment]:
        """Fetch comments by ID (no prefix needed)."""
        if not ids:
            return []
        fullnames = [f"t1_{id.split('_')[-1]}" for id in ids]
        _, comments = await self._fetch_bulk(fullnames)
        return comments

    async def _fetch_bulk(self: "RedditClient", ids: list[str]) -> tuple[list[Submission], list[Comment]]:
        """Fetch up to 100 items per request - THE FAST PATH."""
        submissions, comments = [], []

        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            response = await self._get("api/info", {"id": ",".join(chunk)})

            for item in response["data"]["children"]:
                if item["kind"] == "t1":
                    comments.append(self._to_comment(item["data"]))
                elif item["kind"] == "t3":
                    submissions.append(self._to_submission(item["data"]))
        return submissions, comments

    async def fetch_comment(self: "RedditClient", comment_id: str) -> Comment | None:
        response = await self._get("api/info", {"id": f"t1_{comment_id}"})
        children = response["data"]["children"]
        if children:
            return self._to_comment(children[0]["data"])
        return None

    async def fetch_submission(self: "RedditClient", submission_id: str) -> Submission | None:
        """Fetch submission metadata using api/info (lightweight, no comment tree)."""
        clean_id = submission_id.split("_")[-1]
        response = await self._get("api/info", {"id": f"t3_{clean_id}"})
        children = response["data"]["children"]
        if children and children[0]["kind"] == "t3":
            return self._to_submission(children[0]["data"])
        return None


    def _strip_prefix(self: "RedditClient", reddit_id: str | None) -> str | None:
        if not isinstance(reddit_id, str):
            return None
        return reddit_id.split('_')[-1]

    def _to_submission(self: "RedditClient", submission: dict) -> Submission:
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

    def _to_comment(self: "RedditClient", comment: dict) -> Comment:
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
    
    @api_retry
    async def _get(self, endpoint: str, params: dict = None, _retry: bool = False) -> dict:
        await self._ensure_auth()

        # Refresh token if about to expire (60s buffer)
        if self.authenticated and time.time() > self._token_expires_at - 60:
            await self._authenticate()

        # Shared rate limiting - wait for slot before request
        await self._rate_limiter.acquire()

        url = f"{self.base_url}/{endpoint}.json"
        response = await self.client.get(url, params=params)

        # Handle 401 - token expired, refresh and retry once
        if response.status_code == 401 and self.authenticated and not _retry:
            await self._authenticate()
            return await self._get(endpoint, params, _retry=True)

        # Let api_retry handle 429 and 5xx errors
        response.raise_for_status()

        # Proactive backoff if running low on quota
        remaining = response.headers.get("X-Ratelimit-Remaining")
        reset = response.headers.get("X-Ratelimit-Reset")

        if remaining is not None and float(remaining) < 3:
            await asyncio.sleep(float(reset) if reset else 60)

        return response.json()

    async def _authenticate(self: "RedditClient") -> None:
        """Fetch or refresh OAuth token."""
        credentials = base64.b64encode(f"{self._reddit_id}:{self._reddit_secret}".encode()).decode()
        response = await self.client.post(
            "https://www.reddit.com/api/v1/access_token",
            headers={
                "Authorization": f"Basic {credentials}",
                "User-Agent": self.user_agent
            },
            data={"grant_type": "client_credentials"}
        )
        response.raise_for_status()
        data = response.json()
        self.client.headers["Authorization"] = f"Bearer {data['access_token']}"
        self._token_expires_at = time.time() + data.get("expires_in", 3600)

    async def close(self):
        await self.client.aclose()