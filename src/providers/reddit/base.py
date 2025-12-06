from typing import Protocol, Iterator

from src.storage.models import Submission, Comment

class RedditSource(Protocol):
    @property
    def source_name(self: "RedditSource") -> str: 
        ...

    def stream_user_submissions(self: "RedditSource", username: str) -> Iterator[list[Submission]]:
        ...

    def stream_user_comments(self: "RedditSource", username: str) -> Iterator[list[Comment]]:
        ...

    def stream_submission_comments(self: "RedditSource", submission_id: str) -> Iterator[list[Comment]]:
        ...

    def fetch_submission(self: "RedditSource", submission_id: str) -> Submission | None:
        ...

    def fetch_comment(self: "RedditSource", comment_id: str) -> Submission | None:
        ...

    def fetch_bulk(self: "RedditSource", ids: list[str]) -> tuple[list[Submission], list[Comment]]:
        ...