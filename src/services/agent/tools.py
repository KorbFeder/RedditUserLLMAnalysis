from datetime import datetime

from src.services.agent.retrieval.retrieval import Retriever
from src.storage.postgres import PostgresStore
from langchain.tools import tool

def create_tools(config: dict, session, retriever=None):
    """Create tools for the agent.

    Args:
        config: Configuration dict
        session: Database session
        retriever: Optional pre-initialized Retriever (to avoid parallel init issues)
    """
    if retriever is None:
        retriever = Retriever(config, session)
    store = PostgresStore(session)

    @tool
    def search_user_content(
        query: str,
        username: str,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> str:
        """Search a Reddit user's posts and comments for content matching the query.

        Args:
            query: The search query describing what to look for
            username: The Reddit username to search
            start_time: Only include content created after this Unix timestamp (optional)
            end_time: Only include content created before this Unix timestamp (optional)

        Examples:
            - Search all time: search_user_content("climate change", "username")
            - Search 2024 only: search_user_content("climate change", "username", 1704067200, 1735689599)
            - Search after Jan 2023: search_user_content("topic", "user", start_time=1672531200)
        """
        results = retriever.search(
            query, username, start_time=start_time, end_time=end_time
        )
        if not results:
            time_desc = ""
            if start_time or end_time:
                time_desc = f" in the specified time range"
            return f"No results found for user {username} matching: {query}{time_desc}"
        return "\n\n===RESULT_SEPARATOR===\n\n".join([r.to_context_string() for r in results])

    @tool
    def get_user_profile(username: str) -> str:
        """Get metadata about a Reddit user's stored activity to help plan search queries.

        Returns information about:
        - Time range: When their earliest and latest content was posted
        - Content counts: Total submissions and comments stored
        - Activity by year: How much they posted each year
        - Top subreddits: Where they are most active

        Use this BEFORE searching to understand what data is available and plan
        your time-based queries effectively. For example, if a user only has data
        from 2020-2022, don't waste queries on 2023-2024.

        Args:
            username: The Reddit username to get profile for
        """
        stats = store.get_user_stats(username)

        if not stats["earliest_utc"]:
            return f"No stored content found for user {username}"

        # Format timestamps as human-readable dates
        earliest = datetime.utcfromtimestamp(stats["earliest_utc"]).strftime("%Y-%m-%d")
        latest = datetime.utcfromtimestamp(stats["latest_utc"]).strftime("%Y-%m-%d")

        lines = [
            f"=== Profile for u/{username} ===",
            f"",
            f"Time Range: {earliest} to {latest}",
            f"  Earliest UTC: {stats['earliest_utc']}",
            f"  Latest UTC: {stats['latest_utc']}",
            f"",
            f"Content Counts:",
            f"  Submissions: {stats['total_submissions']}",
            f"  Comments: {stats['total_comments']}",
            f"  Total: {stats['total_submissions'] + stats['total_comments']}",
            f"",
            f"Activity by Year:",
        ]

        for year, count in stats["activity_by_year"].items():
            lines.append(f"  {year}: {count} items")

        lines.append("")
        lines.append("Top Subreddits:")
        for subreddit, count in stats["top_subreddits"]:
            lines.append(f"  r/{subreddit}: {count}")

        return "\n".join(lines)

    return [search_user_content, get_user_profile]

def create_timeinterval_tools(retriever, start_time: int | None = None, end_time: int | None = None):
    """Create tools scoped to a specific time interval.

    Args:
        retriever: Pre-initialized Retriever instance (shared to avoid parallel init issues)
        start_time: Start of time interval (Unix timestamp)
        end_time: End of time interval (Unix timestamp)
    """

    @tool
    def search_user_content(
        query: str,
        username: str,
    ) -> str:
        """Search a Reddit user's posts and comments for content matching the query.

        Note: This search is automatically scoped to a specific time interval.

        Args:
            query: The search query describing what to look for
            username: The Reddit username to search
        """
        # start_time and end_time are captured from outer scope via closure
        results = retriever.search(
            query, username, start_time=start_time, end_time=end_time
        )
        if not results:
            return f"No results found for user {username} matching: {query} in this time period"
        return "\n\n===RESULT_SEPARATOR===\n\n".join([r.to_context_string() for r in results])

    return [search_user_content]