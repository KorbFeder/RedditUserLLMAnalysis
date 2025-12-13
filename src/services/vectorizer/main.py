import os
import asyncio
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from src.helpers.settings import load_config
from src.services.vectorizer.vectorizer import Vectorizer
from src.shared.session import create_db_session
from src.shared.job_messages import JobMessages, JobStatus
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
    logger.info(f"Starting vectorization for {msg.username}")
    session = create_db_session()
    job_store = JobStore(session)
    job_store.update_status(msg.job_id, 'vectorizer', JobStatus.ACTIVE)

    try:
        def run_vectorization():
            """Run in thread pool - creates all DB resources in worker thread for thread safety."""
            config = load_config()
            vectorizer = Vectorizer(config, session)
            return vectorizer.sync_embeddings(msg.username)

        result = await asyncio.to_thread(run_vectorization)
        job_store.update_status(msg.job_id, 'vectorizer', JobStatus.COMPLETED)
        logger.info(f"Vectorization complete: {result}")
        return msg
    except Exception as e:
        logger.error(f"Vectorization failed for {msg.username}: {e}")
        job_store.update_status(msg.job_id, 'vectorizer', JobStatus.FAILED, error=str(e))
        raise
    finally:
        session.close()
