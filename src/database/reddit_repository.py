import logging
from enum import Enum

from src.database.reddit_cache import RedditCache
from src.data_providers.pushpull_provider import PushPullProvider
from src.models.reddit import Submission, Comment
from src.models.cache import UserContributionCacheStatus, ThreadCacheStatus

logger = logging.getLogger(__name__)

class CacheConfig(Enum):
    DEFAULT = 0
    NO_CACHE = 1 
    CACHE_ONLY = 2
    FULL_SAVE = 3

class RedditRepository:
    def __init__(self: "RedditRepository", config: dict):
        self.cache = RedditCache()
        self.push_pull = PushPullProvider(config)
        self.use_cache = CacheConfig(config['use_cache'])

    def get_user_contributions(self: "RedditRepository", username: str) -> tuple[list[Submission], list[Comment]]: 
        # check cache
        if self.use_cache == CacheConfig.DEFAULT:
            cached_comments = self.cache.get_users_comments(username)
            cached_submissions = self.cache.get_users_submissions(username)

            # Get the current state of the cache
            status = self.cache.get_user_cache_status(username)
 
            # fetch the freshest data until we either have overlap with the cache or we have all the data
            new_submissions = self._fetch_new_submissions(username, status.newest_submission_cursor if status else None)
            new_comments = self._fetch_new_comments_from_username(username, status.newest_comment_cursor if status else None)

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
            new_submissions = self._fetch_new_submissions(username, None)
            new_comments = self._fetch_new_comments_from_username(username, None)
            return new_submissions, new_comments

        elif self.use_cache == CacheConfig.CACHE_ONLY:
            cached_comments = self.cache.get_users_comments(username)
            cached_submissions = self.cache.get_users_submissions(username)
            return cached_submissions, cached_comments

        elif self.use_cache == CacheConfig.FULL_SAVE:
            new_submissions = self._fetch_new_submissions(username, None)
            new_comments = self._fetch_new_comments_from_username(username, None)

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


    def get_thread(self: "RedditRepository", submission_id: str) -> tuple[Submission, list[Comment]] | None:
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
                new_comments = self._fetch_new_comments_from_submission(submission_id, None)

                if new_comments:
                    self.cache.add_comments(new_comments)

                self.cache.upsert_thread_cache_status(ThreadCacheStatus(
                    submission_id=submission_id,
                    newest_item_cursor=new_comments[0].created_utc if new_comments else None,
                    is_history_complete=True
                ))
            else:
                new_comments = self._fetch_new_comments_from_submission(submission_id, status.newest_item_cursor)

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

            new_comments = self._fetch_new_comments_from_submission(submission_id, None)
        elif self.use_cache == CacheConfig.CACHE_ONLY:
            submission = self.cache.get_submission(submission_id)

            if submission is None: 
                logger.warning(f"Couldn't fetch submission {submission_id} in CACHE_ONLY mode")
                return None

            new_comments = self.cache.get_submission_comments(submission_id)
        elif self.use_cache == CacheConfig.FULL_SAVE:
            submission = self.push_pull.fetch_submission(submission_id)
            new_comments = self._fetch_new_comments_from_submission(submission_id, None)

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


    def _fetch_new_submissions(self: "RedditRepository", username: str, stop_at_timestamp: int | None) -> list[Submission]:
        new_submissions = []
        for current_submission in self.push_pull.stream_user_submissions(username):
            for sub in current_submission:
                if stop_at_timestamp is not None and sub.created_utc <= stop_at_timestamp:
                    return new_submissions
                new_submissions.append(sub)
        return new_submissions
    
    def _fetch_new_comments_from_username(self: "RedditRepository", username: str, stop_at_timestamp: int | None) -> list[Comment]:
        new_comments = []
        for current_comment in self.push_pull.stream_user_comments(username):
            for com in current_comment:
                if stop_at_timestamp is not None and com.created_utc <= stop_at_timestamp:
                    return new_comments
                new_comments.append(com)
        return new_comments

    def _fetch_new_comments_from_submission(self: "RedditRepository", submission_id: str, stop_at_timestamp: int | None) -> list[Comment]:
        new_comments = []
        for current_comment in self.push_pull.stream_submission_comments(submission_id):
            for com in current_comment:
                if stop_at_timestamp is not None and com.created_utc <= stop_at_timestamp:
                    return new_comments
                new_comments.append(com)
        return new_comments

