import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

logger = logging.getLogger(__name__)


def create_db_session() -> Session:
    url = os.getenv('DATABASE_URL')
    if not url:
        logger.error("DATABASE_URL environment variable not set")
        raise ValueError("DATABASE_URL environment variable not set")
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return Session()