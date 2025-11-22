import requests
import time
from datetime import datetime
from typing import Optional
from enum import Enum

class ContributionType(Enum):
    POST = 'submission'
    COMMENT = 'comment'

class PushPullProvider:
    API_URL = "https://api.pullpush.io/reddit/search"

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
            response = requests.get(f"{self.API_URL}/submission/", params=params)
            current_submissions = response.json().get('data', [])

            if not current_submissions:
                break

            submissions.extend(current_submissions)

            params["before"] = submissions[-1]["created_utc"]
            time.sleep(1)

        params.pop("before", None)

        # comment loop
        while True:
            response = requests.get(f"{self.API_URL}/comment/", params=params)
            current_comments = response.json().get('data', [])

            if not current_comments:
                break

            comments.extend(current_comments)
            params["before"] = comments[-1]["created_utc"]
            time.sleep(1)

        return {"comments": comments, "submissions": submissions}

        
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




