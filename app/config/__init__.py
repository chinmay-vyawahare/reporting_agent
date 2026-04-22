"""
Config package — exposes flat attributes consumed by tools and agents.

Flat attributes (used by bkg_tool, python_sandbox, etc.):
    import config
    config.NEO4J_URI, config.PG_HOST, ...

Dataclass-based config (used by existing agents):
    from config.settings import config   # AppConfig instance
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Neo4j ──────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "neo4j://127.0.0.1:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "nokia-v-one")

# ── PostgreSQL ──────────────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5433"))
PG_DATABASE = os.getenv("PG_DATABASE", "bkg_agent")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

# ── LLM ────────────────────────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_MODEL      = os.getenv("LLM_MODEL", "gpt-4o")
