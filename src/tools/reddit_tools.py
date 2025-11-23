from langchain.tools import tool
from src.database.reddit_data_manager import DataManager

@tool
def serach_user_posts(username: str, search_term: str):
    dm = DataManager()
    dm.search_user_data(username, search_term)