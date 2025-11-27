from src.database.reddit_cache import RedditCache
from src.data_providers.pushpull import PushPullProvider

class RedditRepository:
    def __init__(self: "RedditRepository"):
        self.cache = RedditCache()
        self.push_pull = PushPullProvider()

    def get_thread(self: "RedditRepository", thread_id: str):
        cached = self.cache.get_thread(thread_id)
        if cached:
            return cached

        result = self.push_pull.get_thread(thread_id)
        if result:
            self._cache_thread(result)
        
        return result