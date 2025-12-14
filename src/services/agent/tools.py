# Placeholder for LangChain tools
# The search functionality is now provided by Retriever class
# in src/services/agent/retrieval/retrival.py
#
# To create a LangChain tool, use:
# from langchain.tools import tool
# from src.services.agent.retrieval.retrival import Retriever
#
# @tool
# def search_user_content(query: str, username: str):
#     retriever = Retriever(config, session)
#     return retriever.search(query, username)
