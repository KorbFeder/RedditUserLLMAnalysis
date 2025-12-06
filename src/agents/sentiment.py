from langgraph.graph import StateGraph, START, END
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage

from src.providers.llm.openrouter import get_model
from src.services.vectorizer import Vectorizer
from src.helpers.settings import load_config
from src.agents.tools import search_users_reddit_contributions

from typing import TypedDict

config = load_config()

class UserSentimentState(TypedDict):
    question: str
    username: str


def fetch_context(state: UserSentimentState):
    data_manager = Vectorizer(config)
    data_manager.fill_vector_db(state["username"])
    return {}


def compute_sentiment(state: UserSentimentState):
    model = get_model(config["llm_model_name"])

    system_prompt = f"""You are an expert Reddit user analyst researching u/{state["username"]}.

You have access to a RAG tool that searches this user's Reddit posts and comments.
To build a comprehensive analysis, you MUST:

1. Make multiple searches with different queries to gather diverse context:
   - Search for emotional/opinion keywords (e.g., "love", "hate", "think", "feel")
   - Search for topic-specific terms related to the user's question
   - Search for discussion patterns (e.g., "agree", "disagree", "problem", "solution")

2. After gathering enough context, provide your analysis with:
   - **Overall Sentiment**: positive, negative, neutral, or mixed
   - **Key Topics**: Main subjects this user discusses
   - **Communication Style**: How they express themselves
   - **Summary**: 2-3 sentence overview

Username for searches: {state["username"]}
Be thorough - make at least 3-5 different searches before concluding."""

    inner_agent = create_agent(
        model=model,
        tools=[search_users_reddit_contributions],
        system_prompt=system_prompt
    )

    result = inner_agent.invoke({
        "messages": [HumanMessage(content=state["question"])]
    })

    final_message = result["messages"][-1]
    print("\n" + "=" * 60)
    print(f"SENTIMENT ANALYSIS FOR u/{state['username']}")
    print("=" * 60)
    print(final_message.content)
    print("=" * 60 + "\n")

    return {}



def run():
    # Build graph
    builder = StateGraph(UserSentimentState)
    builder.add_node("fetch", fetch_context)
    builder.add_node("analyze", compute_sentiment)
    builder.add_edge(START, "fetch")
    builder.add_edge("fetch", "analyze")
    builder.add_edge("analyze", END)

    sentiment_agent = builder.compile()

    sentiment_agent.invoke({
        "username": "",
        "question": ""
    })
