import asyncio
import os
import uuid
import logging
from dotenv import load_dotenv
from faststream.rabbit import RabbitBroker

from src.shared.job_messages import JobMessages
from src.shared.session import create_db_session
from src.storage.jobs import JobStore

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


async def send_sentiment_job(username: str, question: str):
    """Create a job and send it to the ingestion queue."""
    job_id = str(uuid.uuid4())

    # Create job in database
    session = create_db_session()
    job_store = JobStore(session)
    job_store.create_user_sentiment_job(job_id, username, question)
    session.close()

    # Send to ingestion queue
    broker = RabbitBroker(os.getenv("RABBITMQ_URL"))
    async with broker:
        msg = JobMessages(job_id=job_id, username=username, question=question)
        await broker.publish(msg, queue="ingestion")
        logger.info(f"Sent job {job_id} for user: {username}")

    return job_id


if __name__ == "__main__":
    job_id = asyncio.run(send_sentiment_job(
        username="spez",
        question="What is this user's sentiment about Reddit's API changes?"
    ))
    print(f"Job created: {job_id}")

