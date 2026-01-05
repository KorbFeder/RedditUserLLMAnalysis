import os
import asyncio
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from dotenv import load_dotenv
from langchain_core.messages import AIMessage, ToolMessage

from src.shared.job_messages import JobMessages, JobStatus, JobType
from src.shared.session import create_db_session
from src.storage.jobs import JobStore
from src.helpers.settings import load_config
from src.services.agent.sentiment import run_sentiment_analysis


def parse_search_results(content: str) -> list[dict]:
    """Parse search result content to extract titles and details."""
    if not content or "No results found" in content:
        return []

    results = []
    # Split by separator
    separator = "\n\n===RESULT_SEPARATOR===\n\n"
    chunks = content.split(separator) if separator in content else [content]

    for chunk in chunks:
        result_info = {}
        lines = chunk.split("\n")
        for line in lines:
            line = line.strip()  # Remove leading/trailing whitespace
            if line.startswith("[SUBMISSION_TITLE]"):
                result_info["title"] = line.replace("[SUBMISSION_TITLE]", "").strip()
            elif line.startswith("[SUBREDDIT]"):
                result_info["subreddit"] = line.replace("[SUBREDDIT]", "").strip()
            elif line.startswith("[AUTHOR]") and "author" not in result_info:
                result_info["author"] = line.replace("[AUTHOR]", "").strip()
        if result_info:
            results.append(result_info)

    return results


def extract_diagnostics(result: dict) -> dict:
    """Extract diagnostics from sentiment analysis result."""
    messages = result.get("messages", [])
    retrieval_docs = result.get("retrieval_node_docs", [])

    # Build a map of tool_call_id -> response content
    tool_responses_map = {}
    for msg in messages:
        if isinstance(msg, ToolMessage):
            tool_responses_map[msg.tool_call_id] = msg.content

    # Extract tool calls from AIMessages and match with responses
    tool_calls = []
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                tool_call_id = tc.get("id", "")
                response_content = tool_responses_map.get(tool_call_id, "")

                # Parse results for search_user_content
                search_results = []
                if tc.get("name") == "search_user_content" and response_content:
                    search_results = parse_search_results(response_content)

                tool_calls.append({
                    "tool": tc.get("name", "unknown"),
                    "args": tc.get("args", {}),
                    "result_count": len(search_results),
                    "results": search_results,
                })

    # Get final answer
    final_answer = messages[-1].content if messages else ""

    return {
        "total_messages": len(messages),
        "initial_rag_docs": len(retrieval_docs),
        "tool_calls_count": len(tool_calls),
        "tool_calls": tool_calls,
        "tool_responses_count": len(tool_responses_map),
        "final_answer_length": len(final_answer),
    }


def log_result(username: str, answer: str, diagnostics: dict):
    """Log the answer followed by diagnostics."""
    logger.info("=" * 60)
    logger.info(f"=== ANSWER for {username} ===")
    logger.info("=" * 60)
    logger.info(f"\n{answer}\n")
    logger.info("=" * 60)
    logger.info("=== DIAGNOSTICS ===")
    logger.info(f"Initial RAG documents: {diagnostics['initial_rag_docs']}")
    logger.info(f"Total messages: {diagnostics['total_messages']}")
    logger.info(f"Tool calls: {diagnostics['tool_calls_count']}")

    # Log search tool calls with results
    search_calls = [tc for tc in diagnostics['tool_calls'] if tc['tool'] == 'search_user_content']
    if search_calls:
        logger.info("-" * 40)
        logger.info("SEARCH QUERIES & RESULTS:")
        for i, tc in enumerate(search_calls, 1):
            query = tc['args'].get('query', 'N/A')
            result_count = tc.get('result_count', 0)
            logger.info(f"  [{i}] Query: \"{query}\"")
            logger.info(f"      Results: {result_count}")
            for r in tc.get('results', []):
                title = r.get('title', 'Unknown')[:50]
                subreddit = r.get('subreddit', '')
                logger.info(f"        - {subreddit}: {title}...")

    logger.info("-" * 40)
    logger.info(f"Answer length: {diagnostics['final_answer_length']} chars")
    logger.info("=" * 60)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

broker = RabbitBroker(os.getenv("RABBITMQ_URL"))
app = FastStream(broker)

@broker.subscriber("agent")
async def agent_handler(msg: JobMessages):
    target = msg.username if msg.job_type == JobType.USER_SENTIMENT.value else f"r/{msg.subreddit}"
    logger.info(f"Starting sentiment analysis for {target} (job_type={msg.job_type})")

    config = load_config()
    session = create_db_session()

    job_store = JobStore(session)
    job_store.update_status(msg.job_id, 'agent', JobStatus.ACTIVE)

    try:
        def run_analysis():
            if msg.job_type == JobType.USER_SENTIMENT.value:
                result = run_sentiment_analysis(config, session, msg.username, msg.question)

                # Get final answer
                answer = result["messages"][-1].content if result.get("messages") else ""

                # Extract diagnostics and log everything
                diagnostics = extract_diagnostics(result)
                log_result(msg.username, answer, diagnostics)

                return answer
            elif msg.job_type == JobType.SUBREDDIT_SENTIMENT.value:
                # TODO: Implement subreddit sentiment analysis workflow
                raise NotImplementedError(
                    f"Subreddit sentiment analysis for r/{msg.subreddit} is not yet implemented. "
                    "The agent workflow is deferred."
                )
            else:
                raise ValueError(f"Unknown job_type: {msg.job_type}")

        answer = await asyncio.to_thread(run_analysis)
        job_store.update_status(msg.job_id, 'agent', JobStatus.COMPLETED, result=answer)
        logger.info(f"Sentiment analysis complete for {target}")
    except Exception as e:
        logger.error(f"Sentiment analysis failed for {target}: {e}")
        job_store.update_status(msg.job_id, 'agent', JobStatus.FAILED, error=str(e))
        raise
    finally:
        session.close()
