from langchain.tools import tool
from src.storage.chroma import VectorStore

@tool
def search_users_reddit_contributions(username: str, search_term: str, n_results: int = 20):
    """Search a Reddit user's posts and comments in the RAG database.

    Args:
        username: The Reddit username to search content for
        search_term: Keywords or phrases to search for in their content
        n_results: Number of results to return (default 20)

    Returns:
        Dict with matching documents from the user's Reddit history
    """
    rag = VectorStore()
    return rag.query_user_content(search_term, username, n_results)
