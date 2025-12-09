from src.storage.models import Comment, Submission
from typing import TypedDict
from dataclasses import dataclass
from enum import Enum

class DocumentType(Enum):
    SUBMISSION = 'submission'
    COMMENT = 'comment'

@dataclass
class DocumentMetadata:
    # Identity
    id: str                    # comment_id or submission_id
    document_type: str         # "comment" | "submission"

    # Hierarchy (for reconstruction)
    submission_id: str         # Thread this belongs to
    parent_id: str | None      # Parent comment ID (null if top-level or submission)

    # Attribution
    username: str              # Target user being analyzed
    parent_author: str | None  # Who they're replying to

    # Context
    subreddit: str
    post_title: str            # For display

    # Temporal (critical for reordering)
    created_utc: int           # Unix timestamp

    # Engagement signals
    score: int
    is_top_level: bool         # Direct reply to post?

    # For submissions only
    num_comments: int | None
    upvote_ratio: float | None

class DocumentBuilder:
    def comment(self: "DocumentBuilder", submission: Submission, user_comment: Comment, parent_comment: Comment | None) -> list[str]:
        document = []
        document.append(f"[SUBREDDIT] r/{submission.subreddit}")
        document.append(f"[POST_TITLE] {submission.author}: {submission.title}")
        if parent_comment:
            document.append(f"[PARENT_COMMENT] {parent_comment.author}: {parent_comment.body}")
        document.append(f"[USER_COMMENT] {user_comment.author}: {user_comment.body}")
        return document

    def submission(self: "DocumentBuilder", submission: Submission) -> list[str]: 
        document = []
        document.append(f"[SUBREDDIT] r/{submission.subreddit}")
        document.append(f"[USER_POST_TITLE] {submission.author}: {submission.title}")
        document.append(f"[BODY] {submission.selftext}")
        return document