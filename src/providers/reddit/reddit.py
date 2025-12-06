import requests
from requests.auth import HTTPBasicAuth
import os
import time
from typing import Iterator

from src.storage.models import Submission, Comment


class RedditRateLimitException(Exception):
    """Raised when rate limited - lets caller decide how to handle (sleep, reschedule, etc.)"""
    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__(f"Rate limited. Retry after {retry_after}s")


class RedditClient:
    def __init__(self: "RedditClient", config: dict, user_agent: str = "SentimentAgent/1.0"):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent

        self.config = config
        self._reddit_id = os.getenv("REDDIT_ID")
        self._reddit_secret = os.getenv("REDDIT_SECRET")

        self.rate_limit = float(os.getenv("rate_limit_no_key", "6.0"))
        self.base_url = "https://www.reddit.com"
        self.authenticated = False
        self._token_expires_at = 0

        if self._reddit_id and self._reddit_secret:
            self._authenticate()
            self.rate_limit = float(os.getenv("rate_limit_key", "0.5"))
            self.base_url = "https://oauth.reddit.com"
            self.authenticated = True

    @property
    def source_name(self: "RedditClient") -> str: 
        return "reddit"

    def stream_user_submissions(self: "RedditClient", username: str) -> Iterator[list[Submission]]:
        after = None
        while True:
            params = {"limit": 100}
            if after:
                params["after"] = after

            response = self._get(f"user/{username}/submitted", params)
            children = response["data"]["children"]

            if not children:
                break

            yield [self._to_submission(s["data"]) for s in children]
            after = response["data"].get("after")

            if not after:
                break

    def stream_submission_comments(self: "RedditClient", submission_id: str) -> Iterator[list[Comment]]:
        response = self._get(f"comments/{submission_id}", {"limit": 500})
        comments_data = response[1]["data"]["children"] 

        comments = [
            self._to_comment(c["data"])
            for c in comments_data
            if c["kind"] == "t1"
        ]
        if comments:
            yield comments

    def stream_user_comments(self: "RedditClient", username: str) -> Iterator[list[Comment]]:
        after = None
        while True:
            params = {"limit": 100}
            if after:
                params["after"] = after

            response = self._get(f"user/{username}/comments", params)
            children = response["data"]["children"]

            if not children:
                break

            yield [self._to_comment(c["data"]) for c in children]
            after = response["data"].get("after")

            if not after:
                break 

    def fetch_bulk(self: "RedditClient", ids: list[str]) -> tuple[list[Submission], list[Comment]]:
        """Fetch up to 100 items per request - THE FAST PATH."""
        submissions, comments = [], []

        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            response = self._get("api/info", {"id": ",".join(chunk)})

            for item in response["data"]["children"]:
                if item["kind"] == "t1":
                    comments.append(self._to_comment(item["data"]))
                elif item["kind"] == "t3":
                    submissions.append(self._to_submission(item["data"]))
        return submissions, comments

    def fetch_comment(self: "RedditClient", comment_id: str) -> Comment | None:
        response = self._get("api/info", {"id": f"t1_{comment_id}"})
        children = response["data"]["children"]
        if children:
            return self._to_comment(children[0]["data"])
        return None

    def fetch_submission(self: "RedditClient", submission_id: str) -> Submission | None:
        """Fetch submission metadata using api/info (lightweight, no comment tree)."""
        # Strip prefix if present, then add t3_
        clean_id = submission_id.split("_")[-1]
        response = self._get("api/info", {"id": f"t3_{clean_id}"})
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
    
    def _get(self: "RedditClient", endpoint: str, params: dict = None, _retry: bool = False) -> dict:
        # Refresh token if about to expire (60s buffer)
        if self.authenticated and time.time() > self._token_expires_at - 60:
            self._authenticate()

        url = f"{self.base_url}/{endpoint}.json"
        response = self.session.get(url, params=params)

        # Handle 401 - token expired, refresh and retry once
        if response.status_code == 401 and self.authenticated and not _retry:
            self._authenticate()
            return self._get(endpoint, params, _retry=True)

        # Handle 429 - rate limited, raise exception for caller to handle
        if response.status_code == 429:
            reset = response.headers.get("X-Ratelimit-Reset")
            retry_after = float(reset) if reset else 60.0
            raise RedditRateLimitException(retry_after)

        response.raise_for_status()

        # Proactive rate limiting based on headers (for non-authenticated)
        remaining = response.headers.get("X-Ratelimit-Remaining")
        reset = response.headers.get("X-Ratelimit-Reset")

        if remaining is None or reset is None:
            # No headers = unauthenticated, use fixed delay
            time.sleep(self.rate_limit)
        elif float(remaining) < 3:
            # Running low, wait for reset
            time.sleep(float(reset) if reset else 60)

        return response.json()


    def _authenticate(self: "RedditClient") -> None:
        """Fetch or refresh OAuth token."""
        auth = HTTPBasicAuth(self._reddit_id, self._reddit_secret)
        response = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=auth,
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": self.session.headers["User-Agent"]}
        )
        response.raise_for_status()
        data = response.json()
        self.session.headers["Authorization"] = f"Bearer {data['access_token']}"
        self._token_expires_at = time.time() + data.get("expires_in", 3600)