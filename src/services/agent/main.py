import os
import asyncio
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from dotenv import load_dotenv

from src.shared.job_messages import JobMessages, JobStatus
from src.shared.session import create_db_session
from src.storage.jobs import JobStore
from src.services.agent.retrieval.retrieval import Retriever
from src.helpers.settings import load_config
from src.services.agent.sentiment import run_sentiment_analysis

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

broker = RabbitBroker(os.getenv("RABBITMQ_URL"))
app = FastStream(broker)

@broker.subscriber("agent")
async def agent_handler(msg: JobMessages):
    logger.info(f"Starting sentiment analysis for {msg.username}")
    config = load_config()
    session = create_db_session()

    job_store = JobStore(session)
    job_store.update_status(msg.job_id, 'agent', JobStatus.ACTIVE)

    try:
        def run_analysis():
            result = run_sentiment_analysis(config, session, msg.username, msg.question)
            logger.info(result)
            return result

        result = await asyncio.to_thread(run_analysis)
        job_store.update_status(msg.job_id, 'agent', JobStatus.COMPLETED, result=str(result))
        logger.info(f"Sentiment analysis complete for {msg.username}")
    except Exception as e:
        logger.error(f"Sentiment analysis failed for {msg.username}: {e}")
        job_store.update_status(msg.job_id, 'agent', JobStatus.FAILED, error=str(e))
        raise
    finally:
        session.close()
