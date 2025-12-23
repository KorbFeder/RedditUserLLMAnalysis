import os
import logging
from dotenv import load_dotenv
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from src.services.ingestion.ingestion import IngestionService
from src.shared.job_messages import JobMessages, JobStatus
from src.shared.session import create_db_session
from src.storage.jobs import JobStore
from src.helpers.settings import load_config
from src.reddit_providers.source_factory import create_current_source, create_historical_source

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

broker = RabbitBroker(os.getenv("RABBITMQ_URL"))
app = FastStream(broker)

@broker.subscriber("ingestion")
@broker.publisher("vectorizer")
async def ingestion_handler(msg: JobMessages):
    logger.info(f"Starting Ingestion for {msg.username}")
    config = load_config()
    session = create_db_session()

    current_source = create_current_source(config)
    historical_source = create_historical_source(config)
    ingestion_service = IngestionService(session, current_source, historical_source)

    job_store = JobStore(session)
    job_store.update_status(msg.job_id, 'ingestion', JobStatus.ACTIVE)
    try:
        await ingestion_service.sync_users_comment_chain(msg.username)
        job_store.update_status(msg.job_id, 'ingestion', JobStatus.COMPLETED)
        return msg
    except Exception as e:
        logger.error(f"Ingestion failed for {msg.username}: {e}")
        job_store.update_status(msg.job_id, 'ingestion', JobStatus.FAILED, error=str(e))
        raise
    finally:
        await ingestion_service.close()
        session.close()

