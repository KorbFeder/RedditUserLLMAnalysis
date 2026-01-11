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

from src.services.agent.tools import create_subreddit_tools, create_subreddit_timeinterval_tools
from src.services.agent.retrieval.retrieval import Retriever
from src.services.agent.providers.llm.openrouter import get_model as get_openrouter_model
from src.storage.postgres import PostgresStore

logger = logging.getLogger(__name__)


@dataclass
class SubredditSentimentContext:
    """Shared context for all nodes in the subreddit sentiment workflow."""
    config: dict
    session: Session
    store: PostgresStore
    retriever: Retriever  # Initialized once, shared by all workers


class SubredditSentimentState(MessagesState):
    """State schema for the subreddit sentiment analysis workflow."""
    subreddit: str
    query: str
    time_intervals: list[dict]  # Computed by orchestrator, used by routing function
    worker_results: Annotated[list, operator.add]  # Workers write, main_worker reads


class TimeintervalWorkerState(TypedDict):
    """State for individual time-interval worker nodes."""
    subreddit: str
    query: str
    start_utc: int
    end_utc: int


def timeinterval_orchestrator_node(state: SubredditSentimentState, runtime: Runtime[SubredditSentimentContext]) -> dict:
    """Compute time intervals based on subreddit activity and store in state."""
    subreddit_stats = runtime.context.store.get_subreddit_stats(state['subreddit'])
    config = runtime.context.config

    start_utc = subreddit_stats['earliest_utc']
    end_utc = subreddit_stats['latest_utc']
    n_workers = config['agent']['n_workers']

    # Handle edge case: no data for subreddit
    if start_utc is None or end_utc is None:
        logger.warning(f"No data found for subreddit r/{state['subreddit']}")
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

    logger.info(f"Splitting r/{state['subreddit']} data into {len(intervals)} time intervals")
    return {"time_intervals": intervals}


def route_to_workers(state: SubredditSentimentState) -> list[Send]:
    """Routing function for add_conditional_edges. Returns list[Send] to fan-out."""
    if not state.get('time_intervals'):
        return []

    return [
        Send(
            "timeinterval_worker_node",
            {
                "subreddit": state['subreddit'],
                "query": state['query'],
                "start_utc": interval['start_utc'],
                "end_utc": interval['end_utc'],
            }
        )
        for interval in state['time_intervals']
    ]


def timeinterval_worker_node(state: TimeintervalWorkerState, runtime: Runtime[SubredditSentimentContext]):
    """Analyze community sentiment within a specific time interval."""
    if not state['query'] or not state['subreddit'] or not runtime.context.config:
        raise ValueError("Error: missing query, subreddit, or config")

    config = runtime.context.config
    start_utc = state['start_utc']
    end_utc = state['end_utc']
    subreddit = state['subreddit']

    # Use pre-initialized retriever from context to avoid parallel init issues
    tools = create_subreddit_timeinterval_tools(
        runtime.context.retriever,
        subreddit,
        start_utc,
        end_utc
    )

    system_prompt = dedent(f"""
        You are an expert Reddit community analysis agent analyzing COLLECTIVE sentiment
        in r/{subreddit}.

        CRITICAL: You are analyzing COMMUNITY sentiment, not any individual user.

        Focus on:
        1. RANGE OF OPINIONS: Spectrum of viewpoints from different community members
        2. CONSENSUS AREAS: Topics where community broadly agrees
        3. CONTROVERSY AREAS: Topics generating debate/disagreement
        4. AUTHOR DIVERSITY: Sample diverse authors, not just top contributors

        Guidelines:
        - Search broadly to capture different perspectives
        - Note when multiple users express similar sentiments (consensus indicators)
        - Note when users argue or disagree (controversy indicators)
        - Quote examples from DIFFERENT authors
        - Pay attention to upvote patterns if visible
        - Consider both posts and comments as they may reveal different perspectives

        The RAG system uses sparse + dense search with RRF and reranking.
        Query as much as needed - there are no rate limits.
        Rewrite questions into effective RAG queries to capture diverse viewpoints.
    """)

    user_prompt = HumanMessage(dedent(f"""
        Analyze the community discourse in r/{subreddit} regarding: {state['query']}

        For this time period, identify:

        1. **Consensus Points**: What positions/opinions do multiple community members agree on?
           - List specific points of agreement
           - Note how many different authors expressed similar views
           - Include representative quotes with author attribution

        2. **Controversial Points**: What topics generate debate or disagreement?
           - List specific points of contention
           - Describe the opposing viewpoints
           - Quote examples from both/multiple sides of debates

        3. **Sentiment Spectrum**: Map the range of opinions from most positive to most negative
           - Note any extreme positions (both supportive and critical)
           - Identify moderate/nuanced views

        4. **Notable Discussions**: Highlight any particularly insightful or representative threads
           - Include Reddit post/comment URLs as sources

        Remember: Sample from DIVERSE authors. Avoid over-representing any single user's views.
        The goal is to understand what THE COMMUNITY thinks, not what one vocal user thinks.
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
    return {
        "worker_results": [{
            "period": f"{state['start_utc']}-{state['end_utc']}",
            "analysis": result["messages"][-1].content
        }]
    }


def main_worker_node(state: SubredditSentimentState, runtime: Runtime[SubredditSentimentContext]):
    """Synthesize results from time-interval workers into final community analysis."""
    if not state['query'] or not state['subreddit'] or not runtime.context.config:
        raise ValueError("Error: missing query, subreddit, or config")

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
    tools = create_subreddit_tools(config, runtime.context.session, runtime.context.retriever)

    system_prompt = dedent(f"""
        You are an expert Reddit community analysis agent that synthesizes findings from multiple
        time periods to provide a comprehensive view of community sentiment in r/{state['subreddit']}.

        You have access to tools to query additional data if needed to clarify or verify findings.

        Your synthesis should emphasize:

        1. CONSENSUS VS CONTROVERSY:
           - Clearly distinguish between areas where the community has reached broad agreement
           - Highlight persistent controversies vs resolved debates
           - Note any false consensus (loud minority vs silent majority patterns)

        2. TEMPORAL EVOLUTION:
           - How has community opinion shifted over time?
           - Did early controversies become consensus (or vice versa)?
           - What triggered opinion shifts if visible?

        3. BALANCED REPRESENTATION:
           - Give voice to BOTH majority AND minority viewpoints
           - Avoid overweighting the most vocal participants
           - Acknowledge uncertainty when community opinion is unclear

        4. EVIDENCE-BASED CLAIMS:
           - Support claims with specific examples from the subreddit
           - Include URLs to representative posts/comments
           - Note the strength of evidence (many users vs few)

        Guidelines for weighting:
        - Recent sentiment is generally more relevant than old sentiment
        - Consistent views across time periods indicate strong consensus
        - Changing views may indicate the topic is evolving or controversial
    """)

    user_prompt = HumanMessage(dedent(f"""
        Synthesize the following community analysis results for r/{state['subreddit']}

        Original question: {state['query']}

        Results by time period:
        {combined_results}

        Provide a unified community sentiment analysis that addresses:

        ## 1. Areas of Community Consensus
        - What does the community BROADLY AGREE on regarding this topic?
        - How consistent has this consensus been over time?
        - Cite specific examples with URLs

        ## 2. Areas of Controversy and Debate
        - What aspects remain CONTENTIOUS within the community?
        - What are the main opposing viewpoints?
        - Has the nature of controversy changed over time?
        - Present both/all sides fairly with representative quotes

        ## 3. Sentiment Evolution
        - How has community opinion changed from earlier to later periods?
        - What events or factors may have influenced changes?
        - Are there emerging trends in community sentiment?

        ## 4. Minority vs Majority Views
        - What does the majority of the community think?
        - What significant minority viewpoints exist?
        - Are there underrepresented perspectives worth noting?

        ## 5. Confidence Assessment
        - How confident can we be in these findings?
        - Where is the evidence strong vs sparse?
        - What limitations exist in this analysis?

        Use the tools to gather additional evidence if the existing analysis is insufficient.
        Include URLs as sources throughout your analysis.
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
    """Construct the LangGraph workflow for subreddit sentiment analysis."""
    graph = StateGraph(SubredditSentimentState, context_schema=SubredditSentimentContext)

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


def run_subreddit_sentiment_analysis(config: dict, session: Session, subreddit: str, query: str) -> dict:
    """Run subreddit sentiment analysis and return full result state.

    Args:
        config: Application configuration dict
        session: Database session
        subreddit: Subreddit name (without r/ prefix)
        query: Question to analyze

    Returns:
        dict with keys: messages, subreddit, query, time_intervals, worker_results
    """
    app = build_graph()

    # Initialize retriever once - shared by all workers to avoid parallel init issues
    retriever = Retriever(config, session)

    context = SubredditSentimentContext(
        config=config,
        session=session,
        store=PostgresStore(session),
        retriever=retriever
    )
    result = app.invoke(
        {
            "messages": [],
            "subreddit": subreddit,
            "query": query,
            "time_intervals": [],
            "worker_results": [],
        },
        context=context
    )
    return dict(result)
