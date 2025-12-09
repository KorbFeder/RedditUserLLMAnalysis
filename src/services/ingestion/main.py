import os
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from src.services.ingestion.ingestion import IngestionService
from src.helpers.settings import load_config

logger = logging.getLogger(__name__)

broker = RabbitBroker(os.getenv("RABBITMQ_URL"))
app = FastStream(broker)

@broker.subscriber("ingestion")
@broker.publisher("vectorizer")
async def ingestion_handler(username: str):
    logging.info(f"Starting Ingestion for {username}")
    config = load_config()
    ingestion_service = IngestionService(config)
    try:
        await ingestion_service.sync_users_comment_chain(username)
        return username
    finally:
        await ingestion_service.close()

#if __name__ == "__main__":
#    pass