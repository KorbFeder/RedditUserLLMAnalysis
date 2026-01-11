import os
import logging

from sqlalchemy.orm import Session

from src.storage.models import Job, UserSentimentJob, SubredditSentimentJob
from src.shared.job_messages import JobStatus

logger = logging.getLogger(__name__)

class JobStore:
    def __init__(self: "JobStore", session: Session):
        self.session = session

    def create_user_sentiment_job(self, job_id: str, username: str, question: str) -> Job:
        """Create a user sentiment analysis job with its parameters."""
        job = Job(id=job_id, job_type="user_sentiment")
        user_job = UserSentimentJob(job_id=job_id, username=username, question=question)
        self.session.add(job)
        self.session.add(user_job)
        self.session.commit()
        return job

    def create_subreddit_sentiment_job(self, job_id: str, subreddit: str, question: str) -> Job:
        """Create a subreddit sentiment analysis job with its parameters."""
        job = Job(id=job_id, job_type="subreddit_sentiment")
        subreddit_job = SubredditSentimentJob(job_id=job_id, subreddit=subreddit, question=question)
        self.session.add(job)
        self.session.add(subreddit_job)
        self.session.commit()
        return job

    def update_status(self: "JobStore", job_id: str, service: str, status: JobStatus = JobStatus.COMPLETED, result: str | None = None, error: str | None = None):
        """
        Update job status. Shared method called by all services at end of their stage.

        Args:
            job_id: The job ID
            service: Current service (ingestion, vectorizer, agent)
            status: Job status (default "completed", or "failed")
            result: Final result (set by agent on completion)
            error: Error message if failed
        """
        job = self.session.get(Job, job_id)
        if not job:
            logger.warning(f"Job {job_id} not found")
            return

        job.status = status.value
        if result is not None:
            job.result = result
        if error is not None:
            job.error = error

        # Update service on job details
        if job.user_sentiment_job:
            job.user_sentiment_job.service = service
        if job.subreddit_sentiment_job:
            job.subreddit_sentiment_job.service = service

        self.session.commit()
        logger.info(f"Job {job_id} service={service} status={status.value}")

    def get_job(self: "JobStore", job_id: str) -> Job | None:
        """Retrieve job by ID."""
        return self.session.get(Job, job_id)

