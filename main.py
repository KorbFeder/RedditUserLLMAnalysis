from deepagents import create_deep_agent
import logging
from dotenv import load_dotenv
import yaml

from src.database.reddit_data_manager import DataManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,  # Set minimum level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def load_config(config_path="./config/default.yaml"):
    with open(config_path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    config = load_config()
    dm = DataManager(config)
    dm.store_user_data('swintec')

