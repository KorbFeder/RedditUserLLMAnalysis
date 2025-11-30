import logging

from src.database.reddit_cache import RedditCache
from src.data_providers.pushpull_provider import PushPullProvider
from src.models.reddit import Submission, Comment

logger = logging.getLogger(__name__)

class RedditRepository:
    def __init__(self: "RedditRepository", config: dict):
        self.cache = RedditCache()
        self.push_pull = PushPullProvider(config)
        self.use_cache = config['use_cache']

    def get_user_contributions(self: "RedditRepository", username: str) -> tuple[list[Submission], list[Comment]]: 
        # check cache
        if self.use_cache:
            cached_comments = self.cache.get_users_comments(username)
            cached_submissions = self.cache.get_users_submissions(username)

            latest_sub_id = cached_submissions[0].id if cached_submissions else None
            latest_com_id = cached_comments[0].id if cached_comments else None
        else:
            latest_sub_id = None
            latest_com_id = None
 
        # fetch the freshest data until we either have overlap with the cache or we have all the data
        new_submissions = self._fetch_new_submissions(username, latest_sub_id)
        new_comments = self._fetch_new_comments_from_username(username, latest_com_id)

        if self.use_cache:
            # cache the new contributions
            if new_submissions:
                self.cache.add_submissions(new_submissions)
            if new_comments:
                self.cache.add_comments(new_comments)

            return new_submissions + cached_submissions, new_comments + cached_comments

        return new_submissions, new_comments


    def get_thread(self: "RedditRepository", submission_id: str) -> tuple[Submission, list[Comment]] | None:
        cached_comments = []

        if self.use_cache:
            submission = self.cache.get_submission(submission_id)
            cached_comments = self.cache.get_submission_comments(submission_id)

            latest_com_id = cached_comments[0].id if cached_comments else None
            
            if submission is None:
                submission = self.push_pull.fetch_submission(submission_id)
                if submission is None: 
                    logger.error(f"Could not fetch the {submission_id} for creating the thread")
                    return None
                self.cache.add_submissions([submission])

            new_comments = self._fetch_new_comments_from_submission(submission_id, latest_com_id)

            if new_comments:
                self.cache.add_comments(new_comments)
            
        else:
            submission = self.push_pull.fetch_submission(submission_id)
            new_comments = self._fetch_new_comments_from_submission(submission_id, None)

        return submission, new_comments + cached_comments


    def _fetch_new_submissions(self: "RedditRepository", username: str, stop_at_id: str | None) -> list[Submission]:
        new_submissions = []
        for current_submission in self.push_pull.stream_user_submissions(username):
            for sub in current_submission:
                if sub.id == stop_at_id:
                    return new_submissions
                new_submissions.append(sub)
        return new_submissions
    
    def _fetch_new_comments_from_username(self: "RedditRepository", username: str, stop_at_id: str | None) -> list[Comment]:
        new_comments = []
        for current_comment in self.push_pull.stream_user_comments(username):
            for com in current_comment:
                if com.id == stop_at_id:
                    return new_comments
                new_comments.append(com)
        return new_comments

    def _fetch_new_comments_from_submission(self: "RedditRepository", submission_id: str, stop_at_id: str | None) -> list[Comment]:
        new_comments = []
        for current_comment in self.push_pull.stream_submission_comments(submission_id):
            for com in current_comment:
                if com.id == stop_at_id:
                    return new_comments
                new_comments.append(com)
        return new_comments

