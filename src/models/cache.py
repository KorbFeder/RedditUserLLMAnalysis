from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column

from src.models.reddit import Base

class UserContributionCacheStatus(Base):
    __tablename__ = 'user_contribution_cache_status'
    username: Mapped[str] = mapped_column(primary_key=True)
    newest_submission_cursor: Mapped[int | None] = mapped_column(default=None)
    newest_comment_cursor: Mapped[int | None] = mapped_column(default=None)


class ThreadCacheStatus(Base):
    __tablename__ = 'thread_cache_status'
    submission_id: Mapped[str] = mapped_column(primary_key=True)
    newest_item_cursor: Mapped[int | None] = mapped_column(default=None)
    is_history_complete: Mapped[bool] = mapped_column(default=False)



