from src.reddit_providers.pullpush import PullPushClient
from src.reddit_providers.arctic_shift import ArcticShiftClient
from src.reddit_providers.reddit import RedditClient
from src.reddit_providers.base import RedditSource

def create_historical_source(config: dict) -> RedditSource | None:
    source_name = config['reddit_api'].get("historical_source", None)
    if source_name == "pullpush":
        return PullPushClient(config)
    elif source_name == "arctic_shift":
        return ArcticShiftClient(config)
    return None

def create_current_source(config: dict) -> RedditSource | None:
    source_name = config['reddit_api'].get("current_source", None)
    if source_name == "reddit":
        return RedditClient(config)
    return None  # Disabled