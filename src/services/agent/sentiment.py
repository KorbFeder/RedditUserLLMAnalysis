from deepagents import create_deep_agent
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.runtime import Runtime
from langgraph.types import Send, RetryPolicy
from langchain.messages import HumanMessage
from dataclasses import dataclass
from textwrap import dedent
from sqlalchemy.orm import Session
from typing import TypedDict, Annotated
import operator
import logging

from src.services.agent.tools import create_tools, create_timeinterval_tools
from src.services.agent.retrieval.retrieval import Retriever
from src.services.agent.providers.llm.openrouter import get_model as get_openrouter_model
from src.storage.postgres import PostgresStore

logger = logging.getLogger(__name__)


@dataclass
class SentimentContext:
    config: dict
    session: Session
    store: PostgresStore
    retriever: Retriever  # Initialized once, shared by all workers

class SentimentState(MessagesState):
    username: str
    query: str
    time_intervals: list[dict]  # Computed by orchestrator, used by routing function
    worker_results: Annotated[list, operator.add]  # Workers write, main_worker reads

class TimeintervalWorkerState(TypedDict):
    username: str
    query: str
    start_utc: int
    end_utc: int

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

    # Use pre-initialized retriever from context to avoid parallel init issues
    tools = create_timeinterval_tools(runtime.context.retriever, start_utc, end_utc)

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


def main_worker_node(state: SentimentState, runtime: Runtime[SentimentContext]):
    """Synthesize results from time-interval workers into final analysis."""
    if not state['query'] or not state['username'] or not runtime.context.config:
        raise ValueError("Error: missing query, username, or config")

    worker_results = state.get('worker_results', [])

    # Format worker results for synthesis
    if worker_results:
        formatted = []
        for r in worker_results:
            formatted.append(f"**Period {r['period']}:**\n{r['analysis']}")
        combined_results = "\n\n---\n\n".join(formatted)
    else:
        combined_results = "No results from time-interval analysis."

    config = runtime.context.config
    tools = create_tools(config, runtime.context.session, runtime.context.retriever)

    system_prompt = dedent(f"""
        You are an expert Reddit analysis agent that synthesizes findings from multiple time periods.
        You have access to tools to query more data if needed to clarify or verify findings.

        Guidelines:
        - Weight recent activity more heavily than older activity
        - Note any evolution in sentiment/opinions over time
        - Highlight contradictions between periods if any
        - Use tools to dig deeper if the existing analysis is insufficient
    """)

    user_prompt = HumanMessage(dedent(f"""
        Synthesize the following Reddit analysis results for user: {state['username']}

        Original question: {state['query']}

        Results by time period:
        {combined_results}

        Provide a unified analysis that answers the original question.
        Include URLs as sources where available. Use the tools if you need additional evidence.
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

    # Retry policy for LLM calls (handles provider 502 errors, rate limits, etc.)
    llm_retry_policy = RetryPolicy(max_attempts=3, initial_interval=2.0, backoff_factor=2.0)

    # Add nodes
    graph.add_node("timeinterval_orchestrator_node", timeinterval_orchestrator_node)
    graph.add_node("timeinterval_worker_node", timeinterval_worker_node, retry_policy=llm_retry_policy)
    graph.add_node("main_worker_node", main_worker_node, retry_policy=llm_retry_policy)

    # Wiring: START -> orchestrator -> (fan-out) workers -> main_worker (synthesizes) -> END
    graph.add_edge(START, "timeinterval_orchestrator_node")
    graph.add_conditional_edges(
        "timeinterval_orchestrator_node",
        route_to_workers,
        ["timeinterval_worker_node"]
    )
    graph.add_edge("timeinterval_worker_node", "main_worker_node")
    graph.add_edge("main_worker_node", END)

    return graph.compile()


def run_sentiment_analysis(config: dict, session: Session, username: str, query: str) -> dict:
    """Run sentiment analysis and return full result state.

    Returns:
        dict with keys: messages, username, query, time_intervals, worker_results
    """
    app = build_graph()

    # Initialize retriever once - shared by all workers to avoid parallel init issues
    retriever = Retriever(config, session)

    context = SentimentContext(
        config=config,
        session=session,
        store=PostgresStore(session),
        retriever=retriever
    )
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