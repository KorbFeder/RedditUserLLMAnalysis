from src.services.agent.retrieval.retrieval import Retriever
from langchain.tools import tool

def create_tools(config: dict, session):
    retriever = Retriever(config, session)

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

    return [search_user_content]
