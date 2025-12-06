import os
import requests
import logging

from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)

OPEN_ROUTER_BASE_URL = "https://openrouter.ai/api/v1"

def get_model(model_name: str, use_fallback: bool = True) -> ChatOpenAI:
    if use_fallback:
        model_name = fall_back_model(model_name)
    
    return ChatOpenAI(
        api_key=os.getenv("OPENROUTER_API_KEY"),
        base_url=OPEN_ROUTER_BASE_URL,
        model=model_name,
    )

def fall_back_model(model_name: str):
    # fetch open router models
    url = f"{OPEN_ROUTER_BASE_URL}/models"
    headers = {"Authorization": f"Bearer {os.getenv("OPENROUTER_API_KEY")}"}
    response = requests.get(url, headers=headers).json()

    models = response["data"]

    # check if model exists
    if model_name in [model["id"] for model in models]:
        logger.info(f"The model {model_name} exists on the open router API")
        return model_name

    # if the model is not anymore in the API chose a fallback model
    free_models = [model for model in models if all(float(model["pricing"][key]) == 0 for key in ["prompt", "completion", "request"])]
    best_free_models = sorted(free_models, key=lambda x: x["context_length"] or 0, reverse=True) if free_models else None

    new_model_name = best_free_models[0]["id"]
    logger.info(f"The model {model_name} does not exists on the open router API, using fallback model {new_model_name}")
    return new_model_name
