from pydantic import BaseModel
from enum import Enum


class JobType(Enum):
    USER_SENTIMENT = "user_sentiment"
    SUBREDDIT_SENTIMENT = "subreddit_sentiment"


class JobMessages(BaseModel):
    job_id: str
    job_type: str  # "user_sentiment" or "subreddit_sentiment"
    question: str

    # User sentiment fields
    username: str | None = None

    # Subreddit sentiment fields
    subreddit: str | None = None

class JobStatus(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
