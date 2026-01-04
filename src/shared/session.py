import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

logger = logging.getLogger(__name__)

# Singleton engine - shared across all sessions
_engine = None


def get_engine():
    """Get or create the singleton database engine with connection pooling."""
    global _engine
    if _engine is None:
        url = os.getenv('DATABASE_URL')
        if not url:
            logger.error("DATABASE_URL environment variable not set")
            raise ValueError("DATABASE_URL environment variable not set")

        _engine = create_engine(
            url,
            pool_size=10,           # Maintain 10 connections in pool
            max_overflow=20,        # Allow up to 20 additional connections under load
            pool_pre_ping=True,     # Validate connections before use (handles stale connections)
            pool_recycle=3600,      # Recycle connections after 1 hour
        )
        logger.info("Database engine initialized with connection pooling")
    return _engine


def create_db_session() -> Session:
    """Create a new database session using the shared engine."""
    SessionLocal = sessionmaker(bind=get_engine())
    return SessionLocal()