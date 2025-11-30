import requests
import time
import logging
from typing import Iterator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.models.reddit import Submission, Comment

logger = logging.getLogger(__name__)

class PushPullProvider:
    """Implements IRedditDataSource for the PullPush.io API"""

    API_URL = "https://api.pullpush.io/reddit/search"

    def __init__(self: "PushPullProvider", config: dict):
        pushpull_config = config['reddit_api']['pushpull']
        self.rate_limit: float = pushpull_config['rate_limit']
        self.batch_size: int = pushpull_config['batch_size']
    
    @property
    def source_name(self: "PushPullProvider") -> str: 
        return "pushpull"
    
    @retry(          
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def api_request(self: "PushPullProvider", endpoint: str, params: dict):
        response = requests.get(f"{self.API_URL}/{endpoint}/", params=params)
        response.raise_for_status()

        time.sleep(self.rate_limit)

        return response.json()

    def stream_user_submissions(self: "PushPullProvider", username: str) -> Iterator[list[Submission]]:
        params = {
          "author": username,
          "size": self.batch_size,
          "sort": "desc",
          "sort_type": "created_utc"
        }

        logger.info(f"fetching submissions for the user {username}")
        count = 0

        while True:
            current_submissions = self.api_request('submission', params).get('data', [])

            if not current_submissions:
                break

            params["before"] = int(current_submissions[-1]["created_utc"]) - 1
            count += len(current_submissions)

            logger.info(f"Fetched {count} submissions for a user")
            yield [self._to_submission(submission) for submission in current_submissions]

    def stream_user_comments(self: "PushPullProvider", username: str) -> Iterator[list[Comment]]:
        params = {
          "author": username,
          "size": self.batch_size,
          "sort": "desc",
          "sort_type": "created_utc"
        }

        logger.info(f"fetching comments for the user {username}")
        count = 0

        while True:

            current_comments = self.api_request('comment', params).get('data', [])

            if not current_comments:
                break

            params["before"] = int(current_comments[-1]["created_utc"]) - 1
            count += len(current_comments)

            logger.info(f"Fetched {count} comments for a user")
            yield [self._to_comment(comment) for comment in current_comments]


    def stream_submission_comments(self: "PushPullProvider", submission_id: str) -> Iterator[list[Comment]]:
        params = {
          "link_id": submission_id,
          "size": self.batch_size,
          "sort": "desc",
          "sort_type": "created_utc"
        }

        logger.info(f"fetching comments for the submission {submission_id}")
        count = 0

        while True:

            current_comments = self.api_request('comment', params).get('data', [])

            if not current_comments:
                break

            params["before"] = int(current_comments[-1]["created_utc"]) - 1
            count += len(current_comments)

            logger.info(f"Fetched {count} comments from a submission")
            yield [self._to_comment(comment) for comment in current_comments]


    def fetch_submission(self: "PushPullProvider", submission_id: str) -> Submission | None:
        params = {'id': submission_id}

        _submission = self.api_request('submission', params).get('data', [])

        if not _submission:
            return None

        submission = _submission[0]

        return self._to_submission(submission)


    def _strip_prefix(self: "PushPullProvider", reddit_id: str | None) -> str | None:
        if not isinstance(reddit_id, str):
            return None
        return reddit_id.split('_')[-1]

    def _to_submission(self: "PushPullProvider", submission: dict) -> Submission:
        return Submission(
            id=submission["id"],
            raw_json=submission,
            author=submission.get('author'),
            subreddit=submission.get('subreddit'),
            title=submission.get('title'),
            selftext=submission.get('selftext'),
            url=submission.get('url'),
            score=submission.get('score'),
            ups=submission.get('ups'),
            upvote_ratio=submission.get('upvote_ratio'),
            num_comments=submission.get('num_comments'),
            gilded=submission.get('gilded'),
            all_awardings=submission.get('all_awardings'),
            created_utc=submission.get('created_utc') 
        )

    def _to_comment(self: "PushPullProvider", comment: dict) -> Comment:
        return Comment(
            id=comment["id"],
            raw_json=comment,
            submission_id=self._strip_prefix(comment.get('link_id')),
            parent_id=self._strip_prefix(comment.get('parent_id')),
            author=comment.get('author'),
            body=comment.get('body'),
            score=comment.get('score'),
            ups=comment.get('ups'),
            gilded=comment.get('gilded'),
            all_awardings=comment.get('all_awardings'),
            created_utc=comment.get('created_utc')
        )
