"""
Health endpoint — GET /api/v1/health

Checks connectivity for all three external services:
  • Neo4j      (Knowledge Graph)
  • PostgreSQL  (operational data + semantic store)
  • OpenAI API  (validates key by retrieving the configured LLM model)

Response shape:
  {
    "status": "ok" | "degraded",
    "services": {
      "neo4j":    { "status": "connected"|"unavailable", "detail": "...", "latency_ms": 12.5 },
      "postgres": { "status": "connected"|"unavailable", "detail": "...", "latency_ms": 5.2  },
      "openai":   { "status": "connected"|"unavailable", "detail": "...", "latency_ms": 230  }
    },
    "checked_at": "2026-03-02T10:30:00Z"
  }
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import psycopg2
from fastapi import APIRouter
from openai import OpenAI
from typing import Optional

import config
from tools.bkg_tool import BKGTool

logger = logging.getLogger(__name__)
_instance: Optional[BKGTool] = None
router = APIRouter(tags=["System"])

def _get_tool() -> BKGTool:
    global _instance
    if _instance is None:
        _instance = BKGTool()
    return _instance

def health() -> dict:
    """Return Neo4j connectivity status."""
    try:
        tool = _get_tool()
        return {"status": "connected", "node_count": len(tool.nodes)}
    except Exception as e:
        logger.warning("Neo4j health check failed: %s", e)
        return {"status": "unavailable", "error": str(e)}

# ── Individual connectivity checks ─────────────────────────────────────────

def _check_neo4j() -> dict:


    t0 = time.perf_counter()
    try:
        result = health()
        latency = round((time.perf_counter() - t0) * 1000, 1)
        if result["status"] == "connected":
            return {
                "status": "connected",
                "detail": f"{result.get('node_count', '?')} nodes loaded",
                "latency_ms": latency,
            }
        return {
            "status": "unavailable",
            "detail": result.get("error", "unknown error"),
            "latency_ms": latency,
        }
    except Exception as e:
        latency = round((time.perf_counter() - t0) * 1000, 1)
        logger.warning("Neo4j health check failed: %s", e)
        return {"status": "unavailable", "detail": str(e), "latency_ms": latency}


def _check_postgres() -> dict:
    t0 = time.perf_counter()
    try:
        conn = psycopg2.connect(
            host=config.PG_HOST,
            port=config.PG_PORT,
            database=config.PG_DATABASE,
            user=config.PG_USER,
            password=config.PG_PASSWORD,
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM pwc_semantic_information_schema.semantics_simulation"
        )
        row_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        latency = round((time.perf_counter() - t0) * 1000, 1)
        return {
            "status": "connected",
            "detail": f"semantics_simulation has {row_count} scenario(s) indexed",
            "latency_ms": latency,
        }
    except Exception as e:
        latency = round((time.perf_counter() - t0) * 1000, 1)
        logger.warning("PostgreSQL health check failed: %s", e)
        return {"status": "unavailable", "detail": str(e), "latency_ms": latency}


def _check_openai() -> dict:
    """Validate OpenAI API key by retrieving the configured LLM model."""
    t0 = time.perf_counter()
    if not config.OPENAI_API_KEY:
        return {
            "status": "unavailable",
            "detail": "OPENAI_API_KEY is not set in environment",
            "latency_ms": 0.0,
        }
    try:
        client = OpenAI(api_key=config.OPENAI_API_KEY)
        # Retrieve model metadata — zero-cost, validates key + model access
        model_info = client.models.retrieve(config.LLM_MODEL)
        latency = round((time.perf_counter() - t0) * 1000, 1)
        return {
            "status": "connected",
            "detail": f"model '{model_info.id}' is accessible",
            "latency_ms": latency,
        }
    except Exception as e:
        latency = round((time.perf_counter() - t0) * 1000, 1)
        logger.warning("OpenAI health check failed: %s", e)
        return {"status": "unavailable", "detail": str(e), "latency_ms": latency}


# ── Endpoint ───────────────────────────────────────────────────────────────

@router.get("/health", summary="System health check")
def health_check():
    """
    Verify connectivity to all external services.

    - **neo4j** — checks Neo4j Knowledge Graph (node count)
    - **postgres** — checks PostgreSQL and counts indexed scenarios in semantics_simulation
    - **openai** — validates API key by retrieving the configured LLM model (`LLM_MODEL` from env)

    Returns `status: ok` only when **all three** services are reachable.
    Returns `status: degraded` if any service is unavailable.
    """
    neo4j = _check_neo4j()
    postgres = _check_postgres()
    openai = _check_openai()

    all_statuses = [neo4j["status"], postgres["status"], openai["status"]]
    overall = "ok" if all(s == "connected" for s in all_statuses) else "degraded"

    return {
        "status": overall,
        "services": {
            "neo4j": neo4j,
            "postgres": postgres,
            "openai": openai,
        },
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
