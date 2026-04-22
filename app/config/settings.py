"""
Configuration settings for the Reporting Agent system.
"""
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Neo4jConfig:
    uri: str = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")
    user: str = os.getenv("NEO4J_USER", "neo4j")
    password: str = os.getenv("NEO4J_PASSWORD", "password")
    database: str = os.getenv("NEO4J_DATABASE", "nokia-v-one")


@dataclass
class LLMConfig:
    model: str = os.getenv("LLM_MODEL", "gpt-4o")
    fast_model: str = os.getenv("LLM_FAST_MODEL", "gpt-4o-mini")
    temperature: float = 0.0
    max_tokens: int = 4096
    api_key: Optional[str] = os.getenv("OPENAI_API_KEY")


@dataclass
class AppConfig:
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    max_traversal_steps: int = 15
    max_retries: int = 3
    verbose: bool = True


# Singleton config
config = AppConfig()
