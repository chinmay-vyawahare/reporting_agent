"""
Reporting Service — Orchestrates the full reporting pipeline via SSE streaming.

Pipeline:
    1. Schema Discovery → neo4j_tool.get_schema()
    2. Traversal Agent  → traversal_node(state) → raw data + findings
    3. Chart Generation → generate_charts(LLM gets raw tool outputs directly)
"""
from __future__ import annotations

import time
import logging
from typing import Any, Callable

from tools.neo4j_tool import neo4j_tool
from agents.traversal import traversal_node
from agents.graph_agent import generate_charts
from services.embedding_retrieval import retrieve_context

logger = logging.getLogger(__name__)

# ── ANSI colors ──────────────────────────────────────────────────────────────
_CYAN = "\033[96m"
_GREEN = "\033[92m"
_YELLOW = "\033[93m"
_RED = "\033[91m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def stream_report(
    query: str,
    project_type: str,
    query_id: str,
    emit: Callable[[str, dict], None],
    max_charts: int = 3,
) -> dict[str, Any]:
    """
    Execute the reporting pipeline with SSE events emitted at each step.

    Args:
        query: Natural language user query
        project_type: "NTM" | "AHLOB Modernization" | "Both"
        query_id: Unique ID for this report request
        emit: Callback to send SSE events — emit(event_name, data_dict)
        max_charts: Maximum number of charts to generate
    """
    errors = []
    pipeline_start = time.perf_counter()

    emit("step", {"step": 1, "total": 4, "label": "Discovering Knowledge Graph schema..."})

    # ── Step 1: Schema Discovery ──────────────────────────────────────────
    print(f"\n  {_BOLD}{_CYAN}Step 1/4:{_RESET} Discovering KG schema...", flush=True)
    t0 = time.perf_counter()
    try:
        kg_schema = neo4j_tool.get_schema()
    except Exception as e:
        logger.error("Schema discovery failed: %s", e)
        print(f"  {_RED}X Schema discovery failed: {e}{_RESET}\n", flush=True)
        emit("error", {"message": f"Schema discovery failed: {e}"})
        return {"status": "error", "charts": [], "rationale": "", "traversal_steps": 0,
                "traversal_findings": "", "errors": [f"Schema discovery failed: {e}"]}
    schema_ms = (time.perf_counter() - t0) * 1000
    print(f"  {_GREEN}OK Schema:{_RESET} {kg_schema.count(chr(10)) + 1} lines in {schema_ms:.0f}ms", flush=True)

    # ── Step 2: Embedding-based retrieval ─────────────────────────────────
    emit("step", {"step": 2, "total": 4, "label": "Embedding-based KG retrieval..."})
    print(f"\n  {_BOLD}{_CYAN}Step 2/4:{_RESET} Embedding retrieval (nodes + paths)...", flush=True)
    retrieval = retrieve_context(query)
    emit("retrieval_done", {
        "used": retrieval.get("retrieval_used", False),
        "nodes": len(retrieval.get("retrieval_nodes", [])),
        "paths": len(retrieval.get("retrieval_paths", [])),
        "elapsed_ms": retrieval.get("retrieval_elapsed_ms", 0),
    })

    # ── Step 3: Traversal Agent ───────────────────────────────────────────
    emit("step", {"step": 3, "total": 4, "label": "Running traversal agent — querying databases..."})
    print(f"\n  {_BOLD}{_CYAN}Step 3/4:{_RESET} Running traversal agent...", flush=True)
    t0 = time.perf_counter()
    state = {
        "user_query": query,
        "project_type": project_type,
        "kg_schema": kg_schema,
        "max_traversal_steps": 15,
        "retrieval_used": retrieval.get("retrieval_used", False),
        "retrieval_summary": retrieval.get("retrieval_summary", ""),
        "retrieval_nodes": retrieval.get("retrieval_nodes", []),
        "retrieval_paths": retrieval.get("retrieval_paths", []),
        "retrieval_hydrated": retrieval.get("retrieval_hydrated", {}),
    }

    traversal_result = traversal_node(state)
    traversal_ms = (time.perf_counter() - t0) * 1000

    traversal_findings = traversal_result.get("traversal_findings", "")
    traversal_steps = traversal_result.get("traversal_steps_taken", 0)
    tool_calls = traversal_result.get("traversal_tool_calls", [])

    if traversal_result.get("errors"):
        errors.extend(traversal_result["errors"])

    print(f"  {_GREEN}OK Traversal:{_RESET} {traversal_steps} tool call(s) in {traversal_ms:.0f}ms", flush=True)
    emit("traversal_done", {"steps": traversal_steps, "elapsed_ms": round(traversal_ms)})

    if traversal_findings.startswith("Traversal failed"):
        print(f"  {_RED}X Traversal failed — aborting pipeline{_RESET}\n", flush=True)
        emit("error", {"message": traversal_findings})
        return {"status": "error", "charts": [], "rationale": "", "traversal_steps": traversal_steps,
                "traversal_findings": traversal_findings, "errors": errors or [traversal_findings]}

    # ── Step 4: Chart Generation ──────────────────────────────────────────
    emit("step", {"step": 4, "total": 4, "label": "Generating Highcharts visualizations..."})
    print(f"\n  {_BOLD}{_CYAN}Step 4/4:{_RESET} Generating Highcharts (max {max_charts})...", flush=True)
    t0 = time.perf_counter()
    try:
        chart_result = generate_charts(
            user_query=query,
            tool_calls=tool_calls,
            traversal_findings=traversal_findings,
            max_charts=max_charts,
        )
        chart_ms = (time.perf_counter() - t0) * 1000
        charts = chart_result.get("charts", [])
        total_ms = (time.perf_counter() - pipeline_start) * 1000

        print(f"  {_GREEN}OK Charts:{_RESET} {len(charts)} chart(s) in {chart_ms:.0f}ms", flush=True)
        print(f"\n  {_BOLD}Pipeline complete — {total_ms:.0f}ms total{_RESET}\n", flush=True)

        result = {
            "status": "success",
            "charts": charts,
            "rationale": chart_result.get("rationale", ""),
            "traversal_steps": traversal_steps,
            "traversal_findings": traversal_findings,
            "evidence": chart_result.get("evidence", []),
            "retrieval_used": retrieval.get("retrieval_used", False),
            "retrieval_nodes": retrieval.get("retrieval_nodes", []),
            "retrieval_paths": retrieval.get("retrieval_paths", []),
            "errors": errors,
        }
        emit("complete", result)
        return result

    except (ValueError, Exception) as e:
        chart_ms = (time.perf_counter() - t0) * 1000
        logger.error("Chart generation failed: %s", e)
        print(f"  {_RED}X Chart generation failed after {chart_ms:.0f}ms: {e}{_RESET}\n", flush=True)
        errors.append(f"Chart generation failed: {e}")
        emit("error", {"message": str(e)})
        return {"status": "error", "charts": [], "rationale": "", "traversal_steps": traversal_steps,
                "traversal_findings": traversal_findings, "errors": errors}
