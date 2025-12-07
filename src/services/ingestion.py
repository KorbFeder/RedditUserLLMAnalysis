import logging
import asyncio
from typing import AsyncIterator

from src.storage.postgres import PostgresStore
from src.providers.reddit.reddit import RedditClient
from src.providers.reddit.pushpull import PullPushClient
from src.storage.models import Submission, Comment, UserContributionCacheStatus

logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self: "IngestionService", config: dict):
        self.db = PostgresStore()
        self.push_pull = PullPushClient(config)
        self.reddit = RedditClient(config)
        
    async def sync_users_comment_chain(self, username: str, max_depth: int = 20):
        """Sync user's content and walk parent comment chain.

        Uses lightweight relation queries (id, parent_id, submission_id) instead
        of fetching full objects. Only hits API for comments not in DB.
        """
        await self.sync_user_contributions(username)

        # Get lightweight relations for user's comments
        relations = self.db.get_user_comment_relations(username)
        seen_ids = {r[0] for r in relations}
        parent_ids = {r[1] for r in relations if r[1] and r[1] not in seen_ids}
        submission_ids = {r[2] for r in relations if r[2]}

        # Fetch missing submissions (where user commented but not in DB)
        existing_sub_ids = self.db.submissions_exist(list(submission_ids))
        missing_sub_ids = list(submission_ids - existing_sub_ids)

        if missing_sub_ids:
            logger.info(f"Fetching {len(missing_sub_ids)} missing submissions")
            new_subs = await self.reddit.fetch_submissions(missing_sub_ids)
            # Skip existence check - we already verified these don't exist
            if new_subs:
                self.db.session.add_all(new_subs)
                self.db.session.commit()
                logger.info(f"Saved {len(new_subs)} submissions")

        # Walk parent comment chain using lightweight queries
        for i in range(max_depth):
            if not parent_ids:
                logger.info(f"No more parent comments after {i} iterations")
                break

            # Check which parent comments already exist in DB
            existing_ids = self.db.comments_exist(list(parent_ids))
            missing_ids = list(parent_ids - existing_ids)

            # Get relations for existing comments (for next iteration)
            parent_relations = self.db.get_comment_relations(list(existing_ids)) if existing_ids else []

            # Fetch only the missing ones from APIs
            if missing_ids:
                logger.info(f"Depth {i+1}: fetching {len(missing_ids)} missing comments ({len(existing_ids)} already in DB)")

                # Try Reddit first (bulk API - fast)
                reddit_comments = await self.reddit.fetch_comments(missing_ids)
                fetched_ids = {c.id for c in reddit_comments}

                # Try PullPush for ones Reddit didn't return (deleted/archived)
                still_missing = [id for id in missing_ids if id not in fetched_ids]
                if still_missing:
                    logger.info(f"Depth {i+1}: {len(still_missing)} not on Reddit, trying PullPush...")
                    pushpull_comments = await self.push_pull.fetch_comments(still_missing)
                    all_comments = reddit_comments + pushpull_comments
                else:
                    all_comments = reddit_comments

                # Save all fetched comments (filter duplicates - edge case from concurrent sources)
                if all_comments:
                    # Deduplicate within batch (same ID could come from Reddit and PullPush)
                    unique_comments = {c.id: c for c in all_comments}

                    # Force fresh read from DB (clear session cache)
                    self.db.session.expire_all()
                    already_exists = self.db.comments_exist(list(unique_comments.keys()))
                    truly_new = [c for c in unique_comments.values() if c.id not in already_exists]

                    if truly_new:
                        self.db.session.add_all(truly_new)
                        self.db.session.commit()
                    logger.info(f"Depth {i+1}: saved {len(truly_new)} comments (skipped {len(all_comments) - len(truly_new)} duplicates)")

                # Add their relations for next iteration
                parent_relations.extend((c.id, c.parent_id, c.submission_id) for c in all_comments)
            else:
                logger.info(f"Depth {i+1}: all {len(existing_ids)} comments already in DB")

            # Update seen and get next level (exclude top-level: parent_id == submission_id)
            seen_ids.update(r[0] for r in parent_relations)
            parent_ids = {r[1] for r in parent_relations if r[1] and r[1] not in seen_ids and r[1] != r[2]}
        

    async def sync_user_contributions(self, username: str) -> dict:
        """Sync user's submissions and comments from both APIs to Postgres.

        PullPush upserts (wins for content), Reddit inserts only (fills gaps).
        After both complete, reconciles is_deleted/is_archived flags.
        """
        status = self.db.get_user_cache_status(username)
        sub_cursor = status.newest_submission_cursor if status else None
        com_cursor = status.newest_comment_cursor if status else None

        # Run all 4 streams concurrently
        # PullPush: upsert=True (always wins for content)
        # Reddit: upsert=False (insert only, never overwrites)
        (pp_subs, pp_sub_ts), (pp_coms, pp_com_ts), (r_subs, r_sub_ts), (r_coms, r_com_ts) = await asyncio.gather(
            self._collect_submissions_from_stream(
                self.push_pull.stream_user_submissions(username), sub_cursor, upsert=True
            ),
            self._collect_comments_from_stream(
                self.push_pull.stream_user_comments(username), com_cursor, upsert=True
            ),
            self._collect_submissions_from_stream(
                self.reddit.stream_user_submissions(username), sub_cursor, upsert=False
            ),
            self._collect_comments_from_stream(
                self.reddit.stream_user_comments(username), com_cursor, upsert=False
            ),
        )

        # Reconcile is_deleted / is_archived flags
        # In PullPush but not Reddit → deleted
        deleted_sub_ids = pp_subs - r_subs
        deleted_com_ids = pp_coms - r_coms
        # In Reddit but not PullPush → not archived yet
        not_archived_sub_ids = r_subs - pp_subs
        not_archived_com_ids = r_coms - pp_coms

        if deleted_sub_ids:
            self.db.mark_submissions_deleted(deleted_sub_ids)
        if deleted_com_ids:
            self.db.mark_comments_deleted(deleted_com_ids)
        if not_archived_sub_ids:
            self.db.mark_submissions_not_archived(not_archived_sub_ids)
        if not_archived_com_ids:
            self.db.mark_comments_not_archived(not_archived_com_ids)

        # Calculate newest cursors from all sources
        all_subs = pp_subs | r_subs
        all_coms = pp_coms | r_coms

        sub_timestamps = [t for t in [pp_sub_ts, r_sub_ts] if t is not None]
        com_timestamps = [t for t in [pp_com_ts, r_com_ts] if t is not None]

        newest_sub_cursor = max(sub_timestamps) if sub_timestamps else sub_cursor
        newest_com_cursor = max(com_timestamps) if com_timestamps else com_cursor

        self.db.upsert_user_cache_status(UserContributionCacheStatus(
            username=username,
            newest_submission_cursor=newest_sub_cursor,
            newest_comment_cursor=newest_com_cursor
        ))

        return {
            "submissions": len(all_subs),
            "comments": len(all_coms),
            "deleted_submissions": len(deleted_sub_ids),
            "deleted_comments": len(deleted_com_ids),
        }

    async def _collect_submissions_from_stream(
        self,
        stream: AsyncIterator[list[Submission]],
        stop_at: int | None = None,
        upsert: bool = True
    ) -> tuple[set[str], int | None]:
        """Collect items from a batch stream until stop_at timestamp.

        Args:
            upsert: If True, upsert (PullPush - always wins).
                    If False, insert only (Reddit - skip existing).

        Returns:
            (ids, newest_created_utc)
        """
        ids = set()
        newest_ts = None
        save = self.db.upsert_submissions if upsert else self.db.add_submissions

        async for batch in stream:
            results = []
            for item in batch:
                if stop_at is not None and item.created_utc <= stop_at:
                    if results:
                        save(results)
                    return ids, newest_ts
                results.append(item)
                ids.add(item.id)
                if item.created_utc and (newest_ts is None or item.created_utc > newest_ts):
                    newest_ts = item.created_utc
            if results:
                save(results)
        return ids, newest_ts

    async def _collect_comments_from_stream(
        self,
        stream: AsyncIterator[list[Comment]],
        stop_at: int | None = None,
        upsert: bool = True
    ) -> tuple[set[str], int | None]:
        """Collect items from a batch stream until stop_at timestamp.

        Args:
            upsert: If True, upsert (PullPush - always wins).
                    If False, insert only (Reddit - skip existing).

        Returns:
            (ids, newest_created_utc)
        """
        ids = set()
        newest_ts = None
        save = self.db.upsert_comments if upsert else self.db.add_comments

        async for batch in stream:
            results = []
            for item in batch:
                if stop_at is not None and item.created_utc <= stop_at:
                    if results:
                        save(results)
                    return ids, newest_ts
                results.append(item)
                ids.add(item.id)
                if item.created_utc and (newest_ts is None or item.created_utc > newest_ts):
                    newest_ts = item.created_utc
            if results:
                save(results)
        return ids, newest_ts

    async def close(self):
        """Close all client connections."""
        await self.push_pull.close()
        await self.reddit.close()
        self.db.close()