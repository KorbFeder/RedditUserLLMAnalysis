from src.services.agent.retrieval.retrieval import Retriever
from langchain.tools import tool

def create_tools(config: dict, session):
    retriever = Retriever(config, session)

    @tool
    def search_user_content(query: str, username: str) -> str:
        """Search a Reddit user's posts and comments for content matching the query.

        Args:
            query: The search query describing what to look for
            username: The Reddit username to search
        """
        results = retriever.search(query, username)
        if not results:
            return f"No results found for user {username} matching: {query}"
        return "\n\n---\n\n".join([r.to_context_string() for r in results])

    return [search_user_content]
