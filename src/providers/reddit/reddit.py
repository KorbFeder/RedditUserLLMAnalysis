import requests
from requests.auth import HTTPBasicAuth
import os
from typing import Iterator

from src.storage.models import Submission, Comment

class RedditClient:
    def __init__(self: "RedditClient", config: dict, user_agent: str = "SentimentAgent/1.0"):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent

        self.config = config
        reddit_id = os.getenv("REDDIT_ID")
        reddit_secret = os.getenv("REDDIT_SECRET")

        self.rate_limit = os.getenv("rate_limit_no_key")
        self.base_url = "https://www.reddit.com"
        self.authenticated = False

        if reddit_id and reddit_secret:
            self._authenticate(reddit_id, reddit_secret)
            self.rate_limit = os.getenv("rate_limit_key")
            self.base_url = "https://oauth.reddit.com" 
            self.authenticated = True

    @property
    def source_name(self: "RedditClient") -> str: 
        return "reddit"

    def _authenticate(self, reddit_id: str, reddit_secret: str):
        auth = HTTPBasicAuth(reddit_id, reddit_secret)
        response = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=auth,
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": self.session.headers["User-Agent"]}
        )
        token = response.json()["access_token"]
        self.session.headers["Authorization"] = f"Bearer {token}"

    def stream_user_submissions(self: "RedditClient", username: str) -> Iterator[list[Submission]]:
        ...

    def stream_user_comments(self: "RedditClient", username: str) -> Iterator[list[Comment]]:
        ...

    def stream_submission_comments(self: "RedditClient", submission_id: str) -> Iterator[list[Comment]]:
        ...

    def fetch_submission(self: "RedditClient", submission_id: str) -> Submission | None:
        ...

    def fetch_comment(self: "RedditClient", comment_id: str) -> Submission | None:
        ...

    def fetch_bulk(self: "RedditClient", ids: list[str]) -> tuple[list[Submission], list[Comment]]:
        ...