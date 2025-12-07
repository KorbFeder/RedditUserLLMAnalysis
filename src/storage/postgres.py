import os
import logging
from dataclasses import asdict

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func

from src.storage.models import Submission, Comment, UserContributionCacheStatus, ThreadCacheStatus

logger = logging.getLogger(__name__)

class PostgresStore:
    def __init__(self):
        url = os.getenv('DATABASE_URL')
        if not url:
            logger.error("DATABASE_URL environment variable not set")
            raise ValueError("DATABASE_URL environment variable not set")
        engine = create_engine(url)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def add_submissions(self: "PostgresStore", submissions: list[Submission]) -> None:
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

    def add_comments(self: "PostgresStore", comments: list[Comment]) -> None:
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

    def upsert_thread_cache_status(self: "PostgresStore", thread_cache_status: ThreadCacheStatus):
        self.session.merge(thread_cache_status)
        self.session.commit()

    def upsert_user_cache_status(self: "PostgresStore", status: UserContributionCacheStatus):
        self.session.merge(status)
        self.session.commit()

    def get_submissions(self: "PostgresStore", ids: list[str]) -> list[Submission]:
        if not ids:
            return []

        query = select(Submission).where(Submission.id.in_(ids))
        return self.session.scalars(query).all()

    def get_submission(self: "PostgresStore", id: str) -> Submission | None:
        return self.session.get(Submission, id)


    def get_comments(self: "PostgresStore", ids: list[str]) -> list[Comment]:
        if not ids:
            return []

        query = select(Comment).where(Comment.id.in_(ids))
        return self.session.scalars(query).all()

    def get_comment(self: "PostgresStore", id: str) -> Comment | None:
        return self.session.get(Comment, id)

    def get_users_submissions(self: "PostgresStore", username: str) -> list[Submission]:
        query = (
            select(Submission)
            .where(Submission.author == username)
            .order_by(Submission.created_utc.desc())
        )
        return self.session.scalars(query).all()

    def get_users_comments(self: "PostgresStore", username: str) -> list[Comment]:
        query = (
            select(Comment)
            .where(Comment.author == username)
            .order_by(Comment.created_utc.desc())
        )
        return self.session.scalars(query).all()

    def get_submission_comments(self, submission_id: str) -> list[Comment]:
        query = select(Comment).where(Comment.submission_id == submission_id)
        return list(self.session.scalars(query).all())

    def get_user_cache_status(self: "PostgresStore", username: str):
        return self.session.get(UserContributionCacheStatus, username)

    def get_thread_cache_status(self: "PostgresStore", submission_id: str):
        return self.session.get(ThreadCacheStatus, submission_id)
 
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


    def upsert_submissions(self: "PostgresStore", submissions: list[Submission]) -> int:
        """Upsert submissions - bulk insert with conflict handling."""
        if not submissions:
            return 0

        values_list = []
        for sub in submissions:
            values = asdict(sub)
            values['fetched_at'] = func.now()
            values_list.append(values)

        stmt = insert(Submission).values(values_list).on_conflict_do_update(
            index_elements=['id'],
            set_={
                'raw_json': insert(Submission).excluded.raw_json,
                'score': insert(Submission).excluded.score,
                'ups': insert(Submission).excluded.ups,
                'upvote_ratio': insert(Submission).excluded.upvote_ratio,
                'num_comments': insert(Submission).excluded.num_comments,
                'is_deleted': insert(Submission).excluded.is_deleted,
                'is_archived': insert(Submission).excluded.is_archived,
                'fetched_at': insert(Submission).excluded.fetched_at,
            }
        )
        self.session.execute(stmt)
        self.session.commit()
        logger.info(f"Upserted {len(submissions)} submissions")
        return len(submissions)

    def upsert_comments(self: "PostgresStore", comments: list[Comment]) -> int:
        """Upsert comments - bulk insert with conflict handling."""
        if not comments:
            return 0

        values_list = []
        for com in comments:
            values = asdict(com)
            values['fetched_at'] = func.now()
            values_list.append(values)

        stmt = insert(Comment).values(values_list).on_conflict_do_update(
            index_elements=['id'],
            set_={
                'raw_json': insert(Comment).excluded.raw_json,
                'score': insert(Comment).excluded.score,
                'ups': insert(Comment).excluded.ups,
                'is_deleted': insert(Comment).excluded.is_deleted,
                'is_archived': insert(Comment).excluded.is_archived,
                'fetched_at': insert(Comment).excluded.fetched_at,
            }
        )
        self.session.execute(stmt)
        self.session.commit()
        logger.info(f"Upserted {len(comments)} comments")
        return len(comments)

    def mark_submissions_deleted(self, ids: set[str]) -> None:
        """Mark submissions as deleted (in archive but not on Reddit)."""
        if not ids:
            return
        self.session.query(Submission).filter(Submission.id.in_(ids)).update(
            {'is_deleted': True}, synchronize_session=False
        )
        self.session.commit()

    def mark_comments_deleted(self, ids: set[str]) -> None:
        """Mark comments as deleted (in archive but not on Reddit)."""
        if not ids:
            return
        self.session.query(Comment).filter(Comment.id.in_(ids)).update(
            {'is_deleted': True}, synchronize_session=False
        )
        self.session.commit()

    def mark_submissions_not_archived(self, ids: set[str]) -> None:
        """Mark submissions as not archived (on Reddit but not in archive yet)."""
        if not ids:
            return
        self.session.query(Submission).filter(Submission.id.in_(ids)).update(
            {'is_archived': False}, synchronize_session=False
        )
        self.session.commit()

    def mark_comments_not_archived(self, ids: set[str]) -> None:
        """Mark comments as not archived (on Reddit but not in archive yet)."""
        if not ids:
            return
        self.session.query(Comment).filter(Comment.id.in_(ids)).update(
            {'is_archived': False}, synchronize_session=False
        )
        self.session.commit()

    def get_user_comment_relations(self, username: str) -> list[tuple[str, str, str]]:
        """Returns [(id, parent_id, submission_id), ...] for a user's comments"""
        query = select(Comment.id, Comment.parent_id, Comment.submission_id).where(
            Comment.author == username
        )
        return self.session.execute(query).all()

    def get_comment_relations(self, ids: list[str]) -> list[tuple[str, str, str]]:
        """Returns [(id, parent_id, submission_id), ...] for given comment IDs"""
        if not ids:
            return []
        query = select(Comment.id, Comment.parent_id, Comment.submission_id).where(
            Comment.id.in_(ids)
        )
        return self.session.execute(query).all()

    def get_user_submission_ids(self, username: str) -> set[str]:
        """Returns set of submission IDs for a user"""
        query = select(Submission.id).where(Submission.author == username)
        return set(self.session.scalars(query).all())

    def close(self: "PostgresStore"):
        self.session.close()