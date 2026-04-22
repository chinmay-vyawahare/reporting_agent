"""
LLM Provider — instance-based ChatOpenAI wrapper.

Usage:
    from services.llm_provider import LLMProvider

    provider = LLMProvider(model="gpt-4o-mini")
    llm = provider.get_llm()
"""
from __future__ import annotations

import os

from langchain_openai import ChatOpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


class LLMProvider:
    """
    Instance-based LLM provider.

    Each instance wraps a single ChatOpenAI configured for a specific model.
    """

    def __init__(self, model="gpt-4o-mini", temperature=0.2):
        self.llm = ChatOpenAI(
            api_key=OPENAI_API_KEY,
            model=model,
            temperature=temperature,
        )

    def get_llm(self):
        return self.llm

    def invoke(self, messages):
        return self.llm.invoke(messages)

    def stream(self, messages):
        return self.llm.stream(messages)

    async def ainvoke(self, messages):
        return await self.llm.ainvoke(messages)
