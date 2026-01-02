from deepagents import create_deep_agent
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.runtime import Runtime
from langgraph.types import Send
from langchain.messages import HumanMessage
from dataclasses import dataclass 
from textwrap import dedent
from typing import Optional
from sqlalchemy.orm import Session
from typing import TypedDict, Annotated
import operator
import logging

from src.services.agent.tools import create_tools, create_timeinterval_tools
from src.services.agent.retrieval.retrieval import Retriever, CommentChain
from src.services.agent.providers.llm.openrouter import get_model as get_openrouter_model
from src.storage.postgres import PostgresStore

logger = logging.getLogger(__name__)


@dataclass
class SentimentContext:
    config: dict
    session: Session
    store: PostgresStore

class SentimentState(MessagesState):
    username: str
    query: str
    time_intervals: list[dict]  # Computed by orchestrator, used by routing function
    worker_results: Annotated[list, operator.add]  # Reducer merges worker outputs
    retrieval_node_docs: Optional[list[CommentChain]] = None

class TimeintervalWorkerState(TypedDict):
    username: str
    query: str
    start_utc: int
    end_utc: int

def retrieval_node(state: SentimentState, runtime: Runtime[SentimentContext]) -> SentimentState:
    if not state['query'] or not state['username']:
        raise ValueError("Error: no query and username")

    retriever = Retriever(runtime.context.config, runtime.context.session)
    rag_result = retriever.search(state['query'], state['username'])
    return {"retrieval_node_docs": rag_result}
    
def timeinterval_orchestrator_node(state: SentimentState, runtime: Runtime[SentimentContext]) -> dict:
    """Compute time intervals and store in state. Routing function handles Send."""
    user_stats = runtime.context.store.get_user_stats(state['username'])
    config = runtime.context.config

    start_utc = user_stats['earliest_utc']
    end_utc = user_stats['latest_utc']
    n_workers = config['agent']['n_workers']

    # Handle edge case: no data for user
    if start_utc is None or end_utc is None:
        logger.warning(f"No data found for user {state['username']}")
        return {"time_intervals": []}

    # Calculate time interval per worker
    total_duration = end_utc - start_utc
    interval = total_duration // n_workers

    # Handle edge case: duration too small for n_workers
    if interval == 0:
        n_workers = 1
        interval = total_duration

    intervals = []
    for i in range(n_workers):
        worker_start = start_utc + (i * interval)
        # Last worker gets everything up to end_utc (catches remainder)
        worker_end = end_utc if i == n_workers - 1 else start_utc + ((i + 1) * interval)
        intervals.append({"start_utc": worker_start, "end_utc": worker_end})

    logger.info(f"Splitting {state['username']} data into {len(intervals)} time intervals")
    return {"time_intervals": intervals}


def route_to_workers(state: SentimentState) -> list[Send]:
    """Routing function for add_conditional_edges. Returns list[Send] to fan-out."""
    if not state.get('time_intervals'):
        return []

    return [
        Send(
            "timeinterval_worker_node",
            {
                "username": state['username'],
                "query": state['query'],
                "start_utc": interval['start_utc'],
                "end_utc": interval['end_utc'],
            }
        )
        for interval in state['time_intervals']
    ]

def timeinterval_worker_node(state: TimeintervalWorkerState, runtime: Runtime[SentimentContext]):
    if not state['query'] or not state['username'] or not runtime.context.config:
        raise ValueError("Error: missing query, username, or config")

    config = runtime.context.config
    start_utc = state['start_utc']
    end_utc = state['end_utc']
    
    tools = create_timeinterval_tools(config, runtime.context.session, start_utc, end_utc)

    system_prompt = dedent(f"""
        You are an expert Reddit analysis agent, your goal is to analyze a single reddit user 
        with regards to a question. Try to dig as deep as possible by utilizing the MCP/Tools given.
        Rewrite the questions into a rag query to get better results the rag system uses
        a sparse + dense search with rrf and reranking. The Rag database contains entries from historical 
        to current reddit comments and posts as well as already deleted ones. We dont have any rate limits 
        or anything for rag so query as much as you like to get to a very good solution to the question. 
    """)

    user_prompt = HumanMessage(dedent(f"""
        Analyze the user: {state['username']} by answering the following question: {state['query']}. 
        Try to solve the query as good as possible and find as much evidence as possible for it, also add the
        url of the reddit post as a source for different findings. 
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
    return {"worker_results": [{"period": f"{state['start_utc']}-{state['end_utc']}", "analysis": result["messages"][-1].content}]}

def timeinterval_reducer_node(state: SentimentState, runtime: Runtime[SentimentContext]) -> dict:
    """Merge results from all time-interval workers into synthesized analysis."""
    worker_results = state.get('worker_results', [])

    if not worker_results:
        logger.warning("No worker results to reduce")
        return {"messages": []}

    # Format worker results for synthesis
    formatted = []
    for r in worker_results:
        formatted.append(f"**Period {r['period']}:**\n{r['analysis']}")

    combined = "\n\n---\n\n".join(formatted)

    config = runtime.context.config
    tools = create_tools(config, runtime.context.session)

    system_prompt = dedent(f"""
        You are a synthesis agent that combines Reddit analysis results from different time periods.
        You have access to tools to query more data if needed to clarify or verify findings.
        Weight recent activity more heavily than older activity when drawing conclusions.
    """)

    user_prompt = HumanMessage(content=dedent(f"""
        Synthesize the following Reddit analysis results for user: {state['username']}

        Original question: {state['query']}

        Results by time period:
        {combined}

        Provide a unified analysis that:
        1. Weighs recent activity more heavily than older activity
        2. Notes any evolution in sentiment/opinions over time
        3. Highlights contradictions between periods if any
        4. Gives a final assessment answering the original question

        Use the tools if you need to query for additional evidence or clarification.
    """))

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


def main_worker_node(state: SentimentState, runtime: Runtime[SentimentContext]):
    if not state['query'] or not state['username'] or not runtime.context.config:
        raise ValueError("Error: missing query, username, or config")


    config = runtime.context.config
    tools = create_tools(config, runtime.context.session)

    system_prompt = dedent(f"""
        You are an expert Reddit analysis agent, your goal is to analyze a single reddit user 
        with regards to a question. Try to dig as deep as possible by utilizing the MCP/Tools given.
        The initial request comes with a simple rag request already but you can do as much rag requests
        as possible. Rewrite the questions into a rag query to get better results the rag system uses
        a sparse + dense search with rrf and reranking. The Rag database contains entries from historical 
        to current reddit comments and posts as well as already deleted ones. We dont have any rate limits 
        or anything for rag so query as much as you like to get to a very good solution to the question. 
    """)

    user_prompt = HumanMessage(dedent(f"""
        Analyze the user: {state['username']} by answering the following question: {state['query']}. 
        The previous RAG request already gave us this result:
        {state['retrieval_node_docs']}
        Try to solve the query as good as possible and find as much evidence as possible for it, also add the
        url of the reddit post as a source for different findings. 
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

    # Add nodes
    graph.add_node("timeinterval_orchestrator_node", timeinterval_orchestrator_node)
    graph.add_node("timeinterval_worker_node", timeinterval_worker_node)
    graph.add_node("timeinterval_reducer_node", timeinterval_reducer_node)

    # Wiring: START -> orchestrator -> (fan-out via Send) -> workers -> reducer -> END
    graph.add_edge(START, "timeinterval_orchestrator_node")
    graph.add_conditional_edges(
        "timeinterval_orchestrator_node",
        route_to_workers,
        ["timeinterval_worker_node"]
    )
    graph.add_edge("timeinterval_worker_node", "timeinterval_reducer_node")
    graph.add_edge("timeinterval_reducer_node", END)

    return graph.compile()


def run_sentiment_analysis(config: dict, session: Session, username: str, query: str) -> dict:
    """Run sentiment analysis and return full result state.

    Returns:
        dict with keys: messages, username, query, time_intervals, worker_results
    """
    app = build_graph()
    context = SentimentContext(config=config, session=session, store=PostgresStore(session))
    result = app.invoke(
        {
            "messages": [],
            "username": username,
            "query": query,
            "time_intervals": [],
            "worker_results": [],
        },
        context=context
    )
    return dict(result)