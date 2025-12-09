import asyncio
import os
import logging
from dotenv import load_dotenv
from faststream.rabbit import RabbitBroker

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

async def send_ingestion_job(username: str):
    """Send a message to the ingestion queue."""
    broker = RabbitBroker(os.getenv("RABBITMQ_URL"))
    async with broker:
        await broker.publish(username, queue="ingestion")
        logger.info(f"Sent ingestion job for user: {username}")

if __name__ == "__main__":
    asyncio.run(send_ingestion_job("spez"))

