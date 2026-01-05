from typing import Protocol, AsyncIterator

from src.storage.models import Submission, Comment

class RedditSource(Protocol):
    @property
    def source_name(self: "RedditSource") -> str:
        ...

    def stream_user_submissions(self: "RedditSource", username: str) -> AsyncIterator[list[Submission]]:
        ...

    def stream_user_comments(self: "RedditSource", username: str) -> AsyncIterator[list[Comment]]:
        ...

    def stream_submission_comments(self: "RedditSource", submission_id: str) -> AsyncIterator[list[Comment]]:
        ...

    def stream_subreddit_submissions(self: "RedditSource", subreddit: str) -> AsyncIterator[list[Submission]]:
        ...

    def stream_subreddit_comments(self: "RedditSource", subreddit: str) -> AsyncIterator[list[Comment]]:
        ...

    def fetch_submission(self: "RedditSource", submission_id: str) -> Submission | None:
        ...

    def fetch_comment(self: "RedditSource", comment_id: str) -> Comment | None:
        ...

    def fetch_submissions(self: "RedditSource", ids: list[str]) -> list[Submission]:
        ...

    def fetch_comments(self: "RedditSource", ids: list[str]) -> list[Comment]:
        ...

    async def close(self: "RedditSource") -> None:
        ...

