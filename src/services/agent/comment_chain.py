from dataclasses import dataclass
from textwrap import dedent

from src.storage.models import Comment, Submission


def _safe(value) -> str:
    """Return string representation, 'None' if value is None."""
    return str(value) if value is not None else "None"


@dataclass
class CommentChain:
    submission: Submission
    comments: list[Comment]  # ordered: user's comment first, top-level last

    @property
    def user_comment(self) -> Comment | None:
        """The user's comment (first in chain), or None for submission-only chains."""
        return self.comments[0] if self.comments else None

    def to_context_string(self) -> str:
        """Build full context string in chronological order (post → top-level → user)."""
        sub = self.submission
        document = dedent(f"""
            [SUBMISSION_TITLE] {_safe(sub.title)}
            [AUTHOR] {_safe(sub.author)}
            [SUBREDDIT] /r/{_safe(sub.subreddit)}
            [CREATE_DATE] {_safe(sub.created_utc)}
            [SCORE] {_safe(sub.score)} [UPVOTE_RATIO] {_safe(sub.upvote_ratio)}
            [NUM_COMMENTS] {_safe(sub.num_comments)}
            ----------------------------------------------------
            [BODY] {_safe(sub.selftext)}
        """)

        # Reverse for chronological order: top-level → ... → user's comment
        for comment in reversed(self.comments):
            com_doc = dedent(f"""
                [AUTHOR] {_safe(comment.author)}
                [CREATE_DATE] {_safe(comment.created_utc)}
                [SCORE] {_safe(comment.score)}
                -----------------------------------------------------------
                [BODY] {_safe(comment.body)}
            """)
            document += com_doc

        return document
