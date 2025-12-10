import os
import asyncio
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from src.helpers.settings import load_config
from src.services.vectorizer.vectorizer import Vectorizer
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
async def vectorization_handler(username: str):
    logger.info(f"Starting vectorization for {username}")

    def run_vectorization():
        """Run in thread pool - creates all DB resources in worker thread for thread safety."""
        config = load_config()
        vectorizer = Vectorizer(config)
        try:
            return vectorizer.sync_embeddings(username)
        finally:
            vectorizer.close()

    result = await asyncio.to_thread(run_vectorization)
    logger.info(f"Vectorization complete: {result}")
    return username
