import os
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from src.helpers.settings import load_config
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
    logging.info(f"Starting Ingestion for {username}")
    config = load_config()
    return username
