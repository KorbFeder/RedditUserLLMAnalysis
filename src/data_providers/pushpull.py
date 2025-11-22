import requests
import time
from datetime import datetime
from typing import Optional
from enum import Enum
import logging
from dataclasses import dataclass
from typing import Dict, List

logger = logging.getLogger(__name__)

class ContributionType(Enum):
    POST = 'submission'
    COMMENT = 'comment'


class PushPullProvider:
    API_URL = "https://api.pullpush.io/reddit/search"

    def get_thread(self: "PushPullProvider", thread_id: str):
        """
        Returns an array of root-comments, comments of a post that don't have a parent comment. 
        Each root-comment has a replies field which contains the next replies and each of those 
        replies can have an own replies object with even more replies
        """

        logger.info(f"Fetching of a whole thread/post/submission started, fetching the post with the id: {thread_id}")
        params = {
            "id": thread_id
        }
        response = requests.get(f"{self.API_URL}/submission/", params)
        submission = response.json()['data'][0]
        if len(response.json()['data']) > 1:
            logger.error("found more then one thread for a unique id")
            return {}

        if response.status_code != 200:
            logger.error(f"Couldn't fetch post request status: {response.status_code}")
            return {}

        time.sleep(1)

        params = {
            "link_id": thread_id,
            "size": 100,
            "sort": "desc",
            "sort_type": "created_utc"
        }
        
        comments = []
        while True:
            logger.info(f"fetching comments form the submission/post {submission['title']}")
            logger.info(f"currently {len(comments)} fetched")
            response = requests.get(f"{self.API_URL}/comment/", params)

            if response.status_code != 200:
                logger.info(f"issue trying to fetch submissions form the push pull api response with status code: {response.status_code}")
                break

            new_comments = response.json().get('data', [])

            if not new_comments:
                break

            comments.extend(new_comments)

            # the -1 is because before is inclusive so the next iteration would find the last one again
            params["before"] = int(comments[-1]["created_utc"]) - 1
            logger.info(f"fetched comments of post {submission['title']}, next are the next {params['size']} comments from before the date: {datetime.fromtimestamp(params['before'])}")

            time.sleep(1)
        
        ordered_comments = self._order_comments(comments, submission['id'])
        return submission, ordered_comments

        
    def _order_comments(self, comments, submission_id):
        nodes = {c['id']: {**c, 'replies': []} for c in comments}
        root = []

        for comment in comments:
            parent_id = comment['parent_id'].split('_')[-1]
            if parent_id == submission_id:
                root.append(nodes[comment['id']])
            else:
                nodes[parent_id]['replies'].append(nodes[comment['id']])

        return root  


    def fetch_user_contributions(self: "PushPullProvider", username: str):
        comments = []
        submissions = []

        params = {
            "author": username,
            "size": 100,
            "sort": "desc",
            "sort_type": "created_utc"
        }

        # submission loop:
        while True:
            logger.info(f"fetching submissions for the user {username}")
            logger.info(f"currently {len(submissions)} fetched")
            response = requests.get(f"{self.API_URL}/submission/", params=params)

            if response.status_code != 200:
                logger.info(f"issue trying to fetch submissions form the push pull api response with status code: {response.status_code}")
                break

            current_submissions = response.json().get('data', [])

            if not current_submissions:
                break

            submissions.extend(current_submissions)

            # the -1 is because before is inclusive so the next iteration would find the last one again
            params["before"] = int(submissions[-1]["created_utc"]) - 1
            logger.info(f"fetched submission, next are the next {params['size']} submssions from before the date: {datetime.fromtimestamp(params['before'])}")

            time.sleep(1)

        params.pop("before", None)

        # comment loop
        while True:
            logger.info(f"fetching comments for the user {username}")
            logger.info(f"currently {len(comments)} fetched")
            response = requests.get(f"{self.API_URL}/comment/", params=params)

            if response.status_code != 200:
                logger.info(f"issue trying to fetch comments form the push pull api response with status code: {response.status_code}")
                break

            current_comments = response.json().get('data', [])

            if not current_comments:
                break

            comments.extend(current_comments)

            # the -1 is because before is inclusive so the next iteration would find the last one again
            params["before"] = int(comments[-1]["created_utc"]) - 1
            logger.info(f"fetched comments, next are the next {params['size']} comments from before the date: {datetime.fromtimestamp(params['before'])}")

            time.sleep(1)

        return submissions, comments

        
    def search_contribution(        
        self: "PushPullProvider", 
        search_term: str, 
        contribution_type: ContributionType,
        username: Optional[str] = None,
        subreddit: Optional[str] = None, 
        before: Optional[datetime] = None, 
        after: Optional[datetime] = None,
        limit: int = 100
    ):
        params = {
            "q": search_term,
            "size": limit,
            "sort": "desc",
            "sort_type": "created_utc"
        }
        if subreddit: 
            params["subreddit"] = subreddit
        if username:
            params["author"] = username
        if before:
            params["before"] = before.timestamp()
        if after:
            params["after"] = after.timestamp()

        response = requests.get(f"{self.API_URL}/{contribution_type.value}/", params=params)
        return response.json().get('data', [])



logging.basicConfig(
    level=logging.INFO,  # Set minimum level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

