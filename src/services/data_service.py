import logging
from enum import Enum
from typing import Iterator
from tqdm import tqdm
import requests

from src.storage.postgres import PostgresStore
from src.providers.reddit.reddit import RedditClient, RedditRateLimitException
from src.providers.reddit.pushpull import PullPushClient
from src.storage.models import Submission, Comment, UserContributionCacheStatus, ThreadCacheStatus

logger = logging.getLogger(__name__)

class CacheConfig(Enum):
    DEFAULT = 0
    NO_CACHE = 1 
    CACHE_ONLY = 2
    FULL_SAVE = 3

class DataService:
    def __init__(self: "DataService", config: dict):
        self.cache = PostgresStore()
        self.push_pull = PullPushClient(config)
        self.reddit = RedditClient(config)
        self.use_cache = CacheConfig(config['use_cache'])

    def get_submission(self: "DataService", id: str) -> Submission | None:
        submission = self.cache.get_submission(id)
        if submission:
            return submission
        submission = self.push_pull.fetch_submission(id)
        if submission: 
            return submission

    def get_comment(self: "DataService", id: str) -> Comment | None:
        comment = self.cache.get_comment(id)
        if comment:
            return comment
        comment = self.push_pull.fetch_comment(id)
        if comment: 
            return comment
        
    def fetch_user_with_ancestry(self: "DataService", username: str, max_depth: int = 20) -> tuple[list[Submission], list[Comment]]:
        # Fetch user content (cache-aware, tries Reddit with fallback to PullPush)
        submissions, comments = self.get_user_contributions(username)

        # The submission where the user commented on
        submission_ids = {c.submission_id for c in comments if c.submission_id}
        existing_sub_ids = {s.id for s in submissions}
        missing_sub_ids = submission_ids - existing_sub_ids

        if missing_sub_ids:
            logger.info(f"Fetching the root submission for each comment, number of submissions: {len(missing_sub_ids)}")
            new_subs = self.reddit.fetch_submissions(list(missing_sub_ids))
            submissions.extend(new_subs)

        # Track seen IDs to avoid refetching
        seen_ids = {c.id for c in comments}
        _comments = comments

        for i in tqdm(range(max_depth)):
            # Get parent IDs we haven't seen yet
            parent_ids = {c.parent_id for c in _comments if c.parent_id and c.parent_id not in seen_ids}

            if not parent_ids:
                logger.info(f"No more parent comments to fetch after {i} iterations")
                break

            parent_comments = self.reddit.fetch_comments(list(parent_ids))
            # Filter out comments that point to submissions (reached top of thread)
            parent_comments = [pc for pc in parent_comments if pc.parent_id != pc.submission_id]

            # Update seen set and add to results
            seen_ids.update(pc.id for pc in parent_comments)
            _comments = parent_comments
            comments.extend(parent_comments)

            logger.info(f"Depth {i+1}: fetched {len(parent_comments)} parent comments")

        return submissions, comments
        

    def get_user_contributions(self: "DataService", username: str) -> tuple[list[Submission], list[Comment]]:
        logging.info(f"Using {self.use_cache.name}")
        if self.use_cache == CacheConfig.DEFAULT:
            cached_submissions = self.cache.get_users_submissions(username)
            cached_comments = self.cache.get_users_comments(username)

            logger.info(f"Found {len(cached_submissions)} submissions and {len(cached_comments)} comments in cache")

            # Get the current state of the cache
            status = self.cache.get_user_cache_status(username)
 
            # fetch the freshest data until we either have overlap with the cache or we have all the data
            new_submissions, new_comments = self._fetch_user_content(
                username,
                sub_cursor=status.newest_submission_cursor if status else None,
                com_cursor=status.newest_comment_cursor if status else None
            )

            logger.info(f"Fetched {len(new_submissions)} submission and {len(new_comments)} from the api")

            # cache the new contributions
            if new_submissions:
                self.cache.add_submissions(new_submissions)
            if new_comments:
                self.cache.add_comments(new_comments)

            # update state of the cache 
            self.cache.upsert_user_cache_status(UserContributionCacheStatus(
                username=username,
                newest_submission_cursor = new_submissions[0].created_utc if new_submissions else status.newest_submission_cursor if status else None,
                newest_comment_cursor = new_comments[0].created_utc if new_comments else status.newest_comment_cursor if status else None
            ))

            return new_submissions + cached_submissions, new_comments + cached_comments
        elif self.use_cache == CacheConfig.NO_CACHE:
            return self._fetch_user_content(username)

        elif self.use_cache == CacheConfig.CACHE_ONLY:
            cached_comments = self.cache.get_users_comments(username)
            cached_submissions = self.cache.get_users_submissions(username)
            return cached_submissions, cached_comments

        elif self.use_cache == CacheConfig.FULL_SAVE:
            new_submissions, new_comments = self._fetch_user_content(username)
            status = self.cache.get_user_cache_status(username)

            if new_submissions:
                self.cache.add_submissions(new_submissions)
            if new_comments:
                self.cache.add_comments(new_comments)
 
            self.cache.upsert_user_cache_status(UserContributionCacheStatus(
                username=username,
                newest_submission_cursor = new_submissions[0].created_utc if new_submissions else status.newest_submission_cursor if status else None,
                newest_comment_cursor = new_comments[0].created_utc if new_comments else status.newest_comment_cursor if status else None
            ))

            return new_submissions, new_comments


    def get_thread(self: "DataService", submission_id: str) -> tuple[Submission, list[Comment]] | None:
        cached_comments = []

        if self.use_cache == CacheConfig.DEFAULT:
            submission = self.cache.get_submission(submission_id)
            cached_comments = self.cache.get_submission_comments(submission_id)

            status = self.cache.get_thread_cache_status(submission_id)
            
            if submission is None:
                submission = self.push_pull.fetch_submission(submission_id)
                if submission is None: 
                    logger.error(f"Could not fetch the {submission_id} for creating the thread")
                    return None
                self.cache.add_submissions([submission])

            # if is_history_complete is not True or we dont have a status for the cache yet,
            # then we need to fully fetch the whole thread
            if status is None or not status.is_history_complete:
                logger.info(f"The thread {submission_id} is not in the cache, fetching is completely")
                new_comments = self._collect_from_stream(self.push_pull.stream_submission_comments(submission_id))

                if new_comments:
                    self.cache.add_comments(new_comments)

                self.cache.upsert_thread_cache_status(ThreadCacheStatus(
                    submission_id=submission_id,
                    newest_item_cursor=new_comments[0].created_utc if new_comments else None,
                    is_history_complete=True
                ))
            else:
                logger.info(f"The thread {submission_id} is already fully fetched in the cache just checking for updates")
                new_comments = self._collect_from_stream(
                    self.push_pull.stream_submission_comments(submission_id),
                    stop_at=status.newest_item_cursor
                )

                if new_comments:
                    self.cache.add_comments(new_comments)
                    self.cache.upsert_thread_cache_status(ThreadCacheStatus(
                        submission_id=submission_id,
                        newest_item_cursor=new_comments[0].created_utc,
                        is_history_complete=True
                    ))
        elif self.use_cache == CacheConfig.NO_CACHE:
            submission = self.push_pull.fetch_submission(submission_id)

            if submission is None:
                logger.warning(f"Couldn't fetch submission {submission_id} in NO_CACHE mode")
                return None

            new_comments = self._collect_from_stream(self.push_pull.stream_submission_comments(submission_id))
        elif self.use_cache == CacheConfig.CACHE_ONLY:
            submission = self.cache.get_submission(submission_id)

            if submission is None: 
                logger.warning(f"Couldn't fetch submission {submission_id} in CACHE_ONLY mode")
                return None

            new_comments = self.cache.get_submission_comments(submission_id)
        elif self.use_cache == CacheConfig.FULL_SAVE:
            submission = self.push_pull.fetch_submission(submission_id)
            new_comments = self._collect_from_stream(self.push_pull.stream_submission_comments(submission_id))

            if submission is None:
                logger.warning(f"Couldn't fetch submission {submission_id} in FULL_SAVE mode")
                return None

            self.cache.add_submissions([submission])
            if new_comments:
                self.cache.add_comments(new_comments)

            self.cache.upsert_thread_cache_status(ThreadCacheStatus(
                submission_id=submission_id,
                newest_item_cursor=new_comments[0].created_utc if new_comments else None,
                is_history_complete=True
            ))

        return submission, new_comments + cached_comments


    def _collect_from_stream(self: "DataService", stream: Iterator, stop_at: int | None = None) -> list:
        """Collect items from a batch stream until stop_at timestamp."""
        results = []
        for batch in stream:
            for item in batch:
                if stop_at is not None and item.created_utc <= stop_at:
                    return results
                results.append(item)
        return results

    def _fetch_user_content(
        self: "DataService",
        username: str,
        sub_cursor: int | None = None,
        com_cursor: int | None = None,
    ) -> tuple[list[Submission], list[Comment]]:
        """Fetch user content from PullPush, and Reddit if available."""
        # Always fetch from PullPush (reliable, has historical/deleted)
        subs = self._collect_from_stream(self.push_pull.stream_user_submissions(username), sub_cursor)
        coms = self._collect_from_stream(self.push_pull.stream_user_comments(username), com_cursor)

        try:
            subs_r = self._collect_from_stream(self.reddit.stream_user_submissions(username), sub_cursor)
            coms_r = self._collect_from_stream(self.reddit.stream_user_comments(username), com_cursor)
            subs = self._merge_by_id(subs, subs_r) 
            coms = self._merge_by_id(coms, coms_r)
        except (RedditRateLimitException, requests.RequestException) as e:
            logger.warning(f"Reddit API unavailable, using PullPush only: {e}")

        return subs, coms

    def _merge_by_id(self: "DataService", primary: list, fallback: list) -> list:
        """Merge two lists by ID. Primary wins, fallback fills missing."""
        result = {item.id: item for item in fallback}  # Fallback first
        result.update({item.id: item for item in primary})  # Primary overwrites
        return list(result.values())