import os
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
    config = load_config()
    vectorizer = Vectorizer(config)
    try:
        result = vectorizer.sync_embeddings(username)
        logger.info(f"Vectorization complete: {result}")
        return username
    finally:
        vectorizer.close()
