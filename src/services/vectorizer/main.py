import os
import asyncio
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from src.helpers.settings import load_config
from src.services.vectorizer.vectorizer import Vectorizer
from src.shared.session import create_db_session
from src.shared.job_messages import JobMessages, JobStatus, JobType
from src.storage.jobs import JobStore
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

broker = RabbitBroker(os.getenv("RABBITMQ_URL"))
app = FastStream(broker)

@broker.subscriber("vectorizer")
@broker.publisher("agent")
async def vectorization_handler(msg: JobMessages):
    target = msg.username if msg.job_type == JobType.USER_SENTIMENT.value else f"r/{msg.subreddit}"
    logger.info(f"Starting vectorization for {target} (job_type={msg.job_type})")

    session = create_db_session()
    job_store = JobStore(session)
    job_store.update_status(msg.job_id, 'vectorizer', JobStatus.ACTIVE)

    try:
        def run_vectorization():
            """Run in thread pool - creates all DB resources in worker thread for thread safety."""
            config = load_config()
            vectorizer = Vectorizer(config, session)

            if msg.job_type == JobType.USER_SENTIMENT.value:
                return vectorizer.sync_embeddings(msg.username)
            elif msg.job_type == JobType.SUBREDDIT_SENTIMENT.value:
                return vectorizer.sync_embeddings_for_subreddit(msg.subreddit)
            else:
                raise ValueError(f"Unknown job_type: {msg.job_type}")

        result = await asyncio.to_thread(run_vectorization)
        job_store.update_status(msg.job_id, 'vectorizer', JobStatus.COMPLETED)
        logger.info(f"Vectorization complete: {result}")
        return msg
    except Exception as e:
        logger.error(f"Vectorization failed for {target}: {e}")
        job_store.update_status(msg.job_id, 'vectorizer', JobStatus.FAILED, error=str(e))
        raise
    finally:
        session.close()
