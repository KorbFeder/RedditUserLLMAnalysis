from typing import Protocol, Iterator

from src.models.reddit import Submission, Comment

class IRedditDataSource(Protocol):
    @property
    def source_name(self: "IRedditDataSource") -> str: 
        ...

    def stream_user_submissions(self: "IRedditDataSource", username: str) -> Iterator[list[Submission]]:
        ...

    def stream_user_comments(self: "IRedditDataSource", username: str) -> Iterator[list[Comment]]:
        ...

    def stream_submission_comments(self: "IRedditDataSource", submission_id: str) -> Iterator[list[Comment]]:
        ...

    def fetch_submission(self: "IRedditDataSource", submission_id: str) -> Submission | None:
        ...