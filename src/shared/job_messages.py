from pydantic import BaseModel
from enum import Enum

class JobMessages(BaseModel):
    job_id: str
    username: str
    question: str

class JobStatus(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
