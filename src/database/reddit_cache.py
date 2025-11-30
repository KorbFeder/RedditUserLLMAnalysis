import os
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
import logging

from src.models.reddit import Submission, Comment

logger = logging.getLogger(__name__)

class RedditCache:
    def __init__(self):
        url = os.getenv('DATABASE_URL')
        if not url:
            logger.error("DATABASE_URL environment variable not set")
            raise ValueError("DATABASE_URL environment variable not set")
        engine = create_engine(url)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def add_submissions(self: "RedditCache", submissions: list[Submission]) -> None:
        if not submissions:
            return

        comment_ids = [submission.id for submission in submissions]
        existing_ids = self.submissions_exist(comment_ids)

        # Filter to only new comments
        new_submissions = [submission for submission in submissions if submission.id not in existing_ids]

        if new_submissions:
            self.session.add_all(new_submissions)
            self.session.commit()

        logger.info(f"Added {len(submissions)} to the database (submission table)")

    def add_comments(self: "RedditCache", comments: list[Comment]) -> None:
        if not comments:
            return

        # Deduplicate input by ID first
        comments_by_id = {c.id: c for c in comments}
        unique_comments = list(comments_by_id.values())

        # Force fresh read from database
        self.session.expire_all()

        existing_ids = self.comments_exist([c.id for c in unique_comments])
        new_comments = [c for c in unique_comments if c.id not in existing_ids]

        if new_comments:
            self.session.add_all(new_comments)
            self.session.commit()

        logger.info(f"Added {len(comments)} to the database (comment table)")

    def get_submissions(self: "RedditCache", ids: list[str]) -> list[Submission]:
        if not ids:
            return []

        query = select(Submission).where(Submission.id.in_(ids))
        return self.session.scalars(query).all()

    def get_submission(self: "RedditCache", id: str) -> Submission | None:
        return self.session.get(Submission, id)


    def get_comments(self: "RedditCache", ids: list[str]) -> list[Comment]:
        if not ids:
            return []

        query = select(Comment).where(Comment.id.in_(ids))
        return self.session.scalars(query).all()

    def get_comment(self: "RedditCache", id: str) -> Comment | None:
        return self.session.get(Comment, id)

    def get_users_submissions(self: "RedditCache", username: str) -> list[Submission]:
        query = (
            select(Submission)
            .where(Submission.author == username)
            .order_by(Submission.created_utc.desc())
        )
        return self.session.scalars(query).all()

    def get_users_comments(self: "RedditCache", username: list[str]) -> list[Comment]:
        query = (
            select(Comment)
            .where(Comment.author == username)
            .order_by(Comment.created_utc.desc())
        )
        return self.session.scalars(query).all()

    def get_submission_comments(self, submission_id: str) -> list[Comment]:
        query = select(Comment).where(Comment.submission_id == submission_id)
        return list(self.session.scalars(query).all())

    def submissions_exist(self, ids: list[str]) -> set[str]:
        if not ids:
            return set()

        query = select(Submission.id).where(Submission.id.in_(ids))
        return set(self.session.scalars(query).all())

    def comments_exist(self, ids: list[str]) -> set[str]:
        if not ids:
            return set()

        query = select(Comment.id).where(Comment.id.in_(ids))
        return set(self.session.scalars(query).all())
   
    def close(self: "RedditCache"):
        self.session.close()