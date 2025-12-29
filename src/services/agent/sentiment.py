from deepagents import create_deep_agent
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.runtime import Runtime
from langchain.messages import HumanMessage
from dataclasses import dataclass 
from textwrap import dedent
from typing import Optional
from sqlalchemy.orm import Session

from src.services.agent.tools import create_tools
from src.services.agent.retrieval.retrieval import Retriever, CommentChain
from src.services.agent.providers.llm.openrouter import get_model as get_openrouter_model

@dataclass
class SentimentContext:
    config: dict
    session: Session

class SentimentState(MessagesState):
    username: str
    query: str
    retrieval_node_docs: Optional[list[CommentChain]] = None


def retrieval_node(state: SentimentState, runtime: Runtime[SentimentContext]):
    if not state['query'] or not state['username']:
        raise ValueError("Error: no query and username")

    retriever = Retriever(runtime.context.config, runtime.context.session)
    rag_result = retriever.search(state['query'], state['username'])
    return {"retrieval_node_docs": rag_result}
    

def main_worker_node(state: SentimentState, runtime: Runtime[SentimentContext]):
    if not state['query'] or not state['username'] or not runtime.context.config:
        raise ValueError("Error: no query and username")


    config = runtime.context.config
    tools = create_tools(config, runtime.context.session)

    system_prompt = dedent(f"""
        You are an expert Reddit analysis agent, your goal is to analyze a single reddit user 
        with regards to a question. Try to dig as deep as possible by utilizing the MCP/Tools given.
        The initial reqeust comes with a simple rag request already but you can do as much rag requests
        as possible. Rewrite the questions into a rag query to get better results the rag system uses
        a sparse + dense searach with rrf and reranking. The Rag database contains entries from historical 
        to current reddit comments and posts as well as already deleted ones. We dont have any rate limits 
        or anything for rag so query as much as you like to get to a very good solution to the question. 
    """)

    user_prompt = HumanMessage(dedent(f"""
        Analyze the user: {state['username']} by answering the following question: {state['query']}. 
        The previous RAG reqeust already gave us this result: 
        {state['retrieval_node_docs']}
        Try to solve the query as good as possible and find as much evidence as possible for it, also add the 
        url of the reddit post as a source for differnet findings. 
    """))

    # Get model based on provider config
    provider = config['agent'].get('provider', 'openrouter')
    model_name = config['agent']['main_model_name']

    if provider == 'openrouter':
        model = get_openrouter_model(model_name)
    else:
        raise ValueError(f"Unsupported provider: {provider}")

    agent = create_deep_agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt
    )

    result = agent.invoke({"messages": [user_prompt]})
    return {"messages": result["messages"]}
    

def build_graph():
    graph = StateGraph(SentimentState, context_schema=SentimentContext)

    graph.add_node("retrieval", retrieval_node)
    graph.add_node("main_worker", main_worker_node)

    graph.add_edge(START, "retrieval")
    graph.add_edge("retrieval", "main_worker")
    graph.add_edge("main_worker", END)

    return graph.compile()


def run_sentiment_analysis(config: dict, session: Session, username: str, query: str):
    app = build_graph()
    context = SentimentContext(config=config, session=session)
    result = app.invoke(
        {"messages": [], "username": username, "query": query},
        context=context
    )
    return result["messages"][-1].content