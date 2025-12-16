import os

from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel, OpenAIChatModelSettings
from pydantic_ai.providers.azure import AzureProvider

from src.data_models import AlsoBuyQueries

SYSTEM_PROMPT_SPECIFIC = """Your goal is to suggest complementary products for the input product and basically mimic the concept of 'users also buy'
To do so, you will receive an input product (its metadata) and you need to suggest search queries that a user could use to find complementary products.
These search queries should be keyword based to be easily used in a search engine. Each query should have between 1 and 5 words.
You should suggest between 1 and 3 search queries.
These queries will be used in a search engine whose catalog is
Your output should be in english.
Only suggest more than 1 search query if you are very confident the extra queries are still very relevant to find complementary products."""

SYSTEM_PROMPT_GENERIC = """Your goal is to suggest complementary products for the input product and basically mimic the concept of 'users also buy'
To do so, you will receive an input product (its metadata) and you need to suggest search queries that a user could use to find complementary products.
These search queries should be keyword based to be easily used in a search engine. Each query should have between 1 and 5 words.
You should suggest between 1 and 3 search queries.
These queries will be used in a search engine whose catalog is
Your output should be in english.
Your queries should avoid specific details as the catalog we have is not vast and the chance to miss very specific items is high.
Only suggest more than 1 search query if you are very confident the extra queries are still very relevant to find complementary products."""


def get_agent(generic_variant: bool) -> Agent[None, AlsoBuyQueries]:
    model = OpenAIChatModel(
        "gpt-5-mini",
        provider=AzureProvider(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        ),
        settings=OpenAIChatModelSettings(openai_reasoning_effort="minimal"),
    )
    return Agent(
        model=model,
        output_type=AlsoBuyQueries,
        system_prompt=SYSTEM_PROMPT_GENERIC
        if generic_variant
        else SYSTEM_PROMPT_SPECIFIC,
    )
