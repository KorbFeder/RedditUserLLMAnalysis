import logging
from dotenv import load_dotenv

from src.agents.user_sentiment import run

load_dotenv()

logging.basicConfig(
    level=logging.INFO,  # Set minimum level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


if __name__ == "__main__":
    run()

