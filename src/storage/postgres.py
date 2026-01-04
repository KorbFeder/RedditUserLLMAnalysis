import os
import logging
from dataclasses import asdict

from sqlalchemy import create_engine, select, literal_column
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func

from src.storage.models import Submission, Comment, UserContributionCacheStatus

logger = logging.getLogger(__name__)

class PostgresStore:
    def __init__(self: "PostgresStore", session: Session):
        self.session = session

    def add_submissions(self: "PostgresStore", submissions: list[Submission]) -> int:
        """Add new submissions, skipping duplicates. Returns count of added."""
        if not submissions:
            return 0

        try:
            submission_ids = [submission.id for submission in submissions]
            existing_ids = self.submissions_exist(submission_ids)

            # Filter to only new submissions
            new_submissions = [submission for submission in submissions if submission.id not in existing_ids]

            if new_submissions:
                self.session.add_all(new_submissions)
                self.session.commit()

            logger.info(f"Added {len(new_submissions)} submissions (skipped {len(existing_ids)} existing)")
            return len(new_submissions)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to add submissions: {e}")
            raise

    def add_comments(self: "PostgresStore", comments: list[Comment]) -> int:
        """Add new comments, skipping duplicates. Returns count of added."""
        if not comments:
            return 0

        try:
            # Deduplicate input by ID first
            comments_by_id = {c.id: c for c in comments}
            unique_comments = list(comments_by_id.values())

            existing_ids = self.comments_exist([c.id for c in unique_comments])
            new_comments = [c for c in unique_comments if c.id not in existing_ids]

            if new_comments:
                self.session.add_all(new_comments)
                self.session.commit()

            logger.info(f"Added {len(new_comments)} comments (skipped {len(existing_ids)} existing)")
            return len(new_comments)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to add comments: {e}")
            raise

    def upsert_user_cache_status(self: "PostgresStore", status: UserContributionCacheStatus) -> None:
        """Upsert user cache status record."""
        try:
            self.session.merge(status)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to upsert user cache status for {status.username}: {e}")
            raise

    def get_submissions(self: "PostgresStore", ids: list[str]) -> list[Submission]:
        if not ids:
            return []

        query = select(Submission).where(Submission.id.in_(ids))
        return self.session.scalars(query).all()

    def get_comments(self: "PostgresStore", ids: list[str]) -> list[Comment]:
        if not ids:
            return []

        query = select(Comment).where(Comment.id.in_(ids))
        return self.session.scalars(query).all()

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

    def get_user_cache_status(self: "PostgresStore", username: str):
        return self.session.get(UserContributionCacheStatus, username)

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

        try:
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
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to upsert submissions: {e}")
            raise

    def upsert_comments(self: "PostgresStore", comments: list[Comment]) -> int:
        """Upsert comments - bulk insert with conflict handling."""
        if not comments:
            return 0

        try:
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
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to upsert comments: {e}")
            raise

    def mark_submissions_deleted(self, ids: set[str]) -> None:
        """Mark submissions as deleted (in archive but not on Reddit)."""
        if not ids:
            return
        try:
            self.session.query(Submission).filter(Submission.id.in_(ids)).update(
                {'is_deleted': True}, synchronize_session=False
            )
            self.session.commit()
            logger.debug(f"Marked {len(ids)} submissions as deleted")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to mark submissions deleted: {e}")
            raise

    def mark_comments_deleted(self, ids: set[str]) -> None:
        """Mark comments as deleted (in archive but not on Reddit)."""
        if not ids:
            return
        try:
            self.session.query(Comment).filter(Comment.id.in_(ids)).update(
                {'is_deleted': True}, synchronize_session=False
            )
            self.session.commit()
            logger.debug(f"Marked {len(ids)} comments as deleted")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to mark comments deleted: {e}")
            raise

    def mark_submissions_not_archived(self, ids: set[str]) -> None:
        """Mark submissions as not archived (on Reddit but not in archive yet)."""
        if not ids:
            return
        try:
            self.session.query(Submission).filter(Submission.id.in_(ids)).update(
                {'is_archived': False}, synchronize_session=False
            )
            self.session.commit()
            logger.debug(f"Marked {len(ids)} submissions as not archived")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to mark submissions not archived: {e}")
            raise

    def mark_comments_not_archived(self, ids: set[str]) -> None:
        """Mark comments as not archived (on Reddit but not in archive yet)."""
        if not ids:
            return
        try:
            self.session.query(Comment).filter(Comment.id.in_(ids)).update(
                {'is_archived': False}, synchronize_session=False
            )
            self.session.commit()
            logger.debug(f"Marked {len(ids)} comments as not archived")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to mark comments not archived: {e}")
            raise

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

    def get_comment_chain(self, comment_id: str) -> list[Comment]:
        """
        Fetch comment chain from a comment up to the top-level comment.
        Uses recursive CTE for efficient single-query traversal.

        Returns comments ordered: [user's comment, parent, grandparent, ..., top-level]
        """
        chains = self.get_comment_chains([comment_id])
        return chains.get(comment_id, [])

    def get_comment_chains(self, comment_ids: list[str]) -> dict[str, list[Comment]]:
        """
        Batch fetch multiple comment chains in a single query.
        Uses recursive CTE with origin tracking.

        Returns: {comment_id: [user's comment, parent, ..., top-level]}
        """
        if not comment_ids:
            return {}

        # Base case: start with all given comments, track origin
        base = (
            select(
                Comment.id,
                Comment.parent_id,
                Comment.submission_id,
                Comment.id.label("origin_id"),
                literal_column("0").label("depth")
            )
            .where(Comment.id.in_(comment_ids))
            .cte(name="comment_chain", recursive=True)
        )

        cte_alias = base.alias("cc")

        # Recursive part: walk up to parent, carry origin through
        recursive = (
            select(
                Comment.id,
                Comment.parent_id,
                Comment.submission_id,
                cte_alias.c.origin_id,
                (cte_alias.c.depth + 1).label("depth")
            )
            .join(cte_alias, Comment.id == cte_alias.c.parent_id)
            .where(cte_alias.c.parent_id != cte_alias.c.submission_id)
        )

        # Combine base and recursive
        cte = base.union_all(recursive)

        # Final query: get full Comment objects with origin_id and depth
        query = (
            select(Comment, cte.c.origin_id, cte.c.depth)
            .join(cte, Comment.id == cte.c.id)
            .order_by(cte.c.origin_id, cte.c.depth)
        )

        # Group by origin_id
        results: dict[str, list[Comment]] = {}
        for row in self.session.execute(query).all():
            comment, origin_id, depth = row
            if origin_id not in results:
                results[origin_id] = []
            results[origin_id].append(comment)

        return results

    def get_user_stats(self, username: str) -> dict:
        """Get aggregate statistics about a user's stored content.

        Returns dict with:
            - earliest_utc: Unix timestamp of oldest content
            - latest_utc: Unix timestamp of newest content
            - total_submissions: Number of submissions
            - total_comments: Number of comments
            - activity_by_year: Dict of {year: count}
            - top_subreddits: List of (subreddit, count) tuples
        """
        # Submission stats
        sub_stats = self.session.execute(
            select(
                func.count(Submission.id),
                func.min(Submission.created_utc),
                func.max(Submission.created_utc)
            ).where(Submission.author == username)
        ).one()

        # Comment stats
        com_stats = self.session.execute(
            select(
                func.count(Comment.id),
                func.min(Comment.created_utc),
                func.max(Comment.created_utc)
            ).where(Comment.author == username)
        ).one()

        # Combine time ranges
        times = [t for t in [sub_stats[1], sub_stats[2], com_stats[1], com_stats[2]] if t]
        earliest_utc = min(times) if times else None
        latest_utc = max(times) if times else None

        # Activity by year (comments + submissions combined)
        yearly_comments = self.session.execute(
            select(
                func.extract('year', func.to_timestamp(Comment.created_utc)).label('year'),
                func.count().label('cnt')
            ).where(Comment.author == username)
            .group_by('year')
        ).all()

        yearly_submissions = self.session.execute(
            select(
                func.extract('year', func.to_timestamp(Submission.created_utc)).label('year'),
                func.count().label('cnt')
            ).where(Submission.author == username)
            .group_by('year')
        ).all()

        # Merge yearly counts
        activity_by_year = {}
        for year, cnt in yearly_comments:
            if year:
                activity_by_year[int(year)] = cnt
        for year, cnt in yearly_submissions:
            if year:
                activity_by_year[int(year)] = activity_by_year.get(int(year), 0) + cnt

        # Top subreddits (from submissions - comments don't have subreddit field)
        top_subs = self.session.execute(
            select(Submission.subreddit, func.count().label('cnt'))
            .where(Submission.author == username)
            .group_by(Submission.subreddit)
            .order_by(func.count().desc())
            .limit(10)
        ).all()

        return {
            "earliest_utc": earliest_utc,
            "latest_utc": latest_utc,
            "total_submissions": sub_stats[0] or 0,
            "total_comments": com_stats[0] or 0,
            "activity_by_year": dict(sorted(activity_by_year.items())),
            "top_subreddits": [(sub, cnt) for sub, cnt in top_subs],
        }

