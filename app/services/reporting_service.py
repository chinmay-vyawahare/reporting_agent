"""
Reporting Service — Orchestrates the full reporting pipeline via SSE streaming.

Pipeline:
    1. Embedding Retrieval → retrieve_context(query)  → hydrated KG context
    2. Traversal Agent     → traversal_node(state)    → raw data + findings
    3. Chart Generation    → generate_charts(tool_outputs) → charts + insights
"""
from __future__ import annotations

import time
import logging
from typing import Any, Callable

from agents.traversal import traversal_node
from agents.graph_agent import generate_charts
from services.embedding_retrieval import retrieve_context
from services.thread_memory import get_thread_context_summary
from tools.neo4j_tool import neo4j_tool

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
    thread_id: str | None = None,
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

    # ── Step 1: Embedding-based retrieval (with schema fallback) ──────────
    # Primary: retrieve_context returns ~8 hydrated nodes targeted to the query.
    # Fallback: if retrieval fails (OpenAI down, embedding table empty/missing,
    # Neo4j hydration fails, etc.) we fetch the full KG schema dump so the
    # agent still has a menu to work from. Pipeline never runs "blind".
    emit("step", {"step": 1, "total": 3, "label": "Embedding-based KG retrieval..."})
    print(f"\n  {_BOLD}{_CYAN}Step 1/3:{_RESET} Embedding retrieval (nodes + paths)...", flush=True)
    retrieval = retrieve_context(query)

    kg_schema_fallback = ""
    if not retrieval.get("retrieval_used"):
        err = retrieval.get("retrieval_error", "unknown")
        print(f"  {_YELLOW}! Retrieval failed:{_RESET} {err}", flush=True)
        print(f"  {_DIM}  Falling back to neo4j_tool.get_schema() so the agent has a menu.{_RESET}", flush=True)
        try:
            kg_schema_fallback = neo4j_tool.get_schema()
            print(f"  {_GREEN}OK fallback schema:{_RESET} "
                  f"{kg_schema_fallback.count(chr(10)) + 1} lines", flush=True)
            emit("retrieval_fallback", {
                "reason": "retrieval_failed",
                "error": err,
                "schema_lines": kg_schema_fallback.count(chr(10)) + 1,
            })
        except Exception as e2:
            # Both retrieval and schema fallback dead — the agent will still
            # run, but it's essentially blind. Emit a loud warning so the UI
            # can show a specific "degraded" state.
            print(f"  {_RED}X fallback schema ALSO failed:{_RESET} {e2}", flush=True)
            emit("retrieval_fallback", {
                "reason": "retrieval_and_schema_failed",
                "error": f"retrieval={err} · schema={e2}",
                "schema_lines": 0,
            })
            errors.append(f"Retrieval + schema fallback both failed: {err} / {e2}")

    emit("retrieval_done", {
        "used": retrieval.get("retrieval_used", False),
        "nodes": len(retrieval.get("retrieval_nodes", [])),
        "paths": len(retrieval.get("retrieval_paths", [])),
        "elapsed_ms": retrieval.get("retrieval_elapsed_ms", 0),
        "fallback_schema": bool(kg_schema_fallback),
    })

    # ── Thread memory: summarise the last few turns of this chat thread so
    # the traversal agent has conversation context for the new query.
    # Excludes the current query_id (already inserted by the caller) so the
    # in-flight question isn't summarised against itself.
    thread_context_summary = get_thread_context_summary(thread_id, exclude_query_id=query_id)
    if thread_context_summary:
        print(f"  {_DIM}Thread memory: {len(thread_context_summary)} chars of context "
              f"summary injected.{_RESET}", flush=True)

    # ── Step 2: Traversal Agent ───────────────────────────────────────────
    emit("step", {"step": 2, "total": 3, "label": "Running traversal agent — querying databases..."})
    print(f"\n  {_BOLD}{_CYAN}Step 2/3:{_RESET} Running traversal agent...", flush=True)
    t0 = time.perf_counter()
    state = {
        "user_query": query,
        "project_type": project_type,
        # kg_schema is empty on the happy path (retrieval delivers targeted
        # hydrated context instead). It's populated only when retrieval fails,
        # so the agent falls back to the full KG menu in the prompt.
        "kg_schema": kg_schema_fallback,
        "max_traversal_steps": 15,
        "retrieval_used": retrieval.get("retrieval_used", False),
        "retrieval_summary": retrieval.get("retrieval_summary", ""),
        "retrieval_nodes": retrieval.get("retrieval_nodes", []),
        "retrieval_paths": retrieval.get("retrieval_paths", []),
        "retrieval_hydrated": retrieval.get("retrieval_hydrated", {}),
        "thread_context": thread_context_summary,
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
    emit("step", {"step": 3, "total": 3, "label": "Generating Highcharts visualizations..."})
    print(f"\n  {_BOLD}{_CYAN}Step 3/3:{_RESET} Generating Highcharts (max {max_charts})...", flush=True)
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

        # Slim payload for the UI: chart configs + per-chart insight + overall
        # rationale + the Python/SQL `script` that produced each chart (so the
        # canvas can save it and the template re-run can replay it with today's
        # data). Drops retrieval_nodes (props.kpi_python_function etc.),
        # traversal scratchpad, and the raw row dump — those stay in `result`
        # so the DB persistence below still saves them for debugging.
        _CHART_KEEP = {"chart_id", "chart", "title", "subtitle", "series",
                       "xAxis", "yAxis", "plotOptions", "legend", "tooltip",
                       "colors", "description", "insight"}

        def _slim_chart(c: dict) -> dict:
            out = {k: v for k, v in c.items() if k in _CHART_KEEP}
            ev = c.get("evidence") or {}
            # `script` is the canonical, slim handle the UI persists. Save the
            # sql_index too so canvas → template can match a chart back to its
            # original SQL block if multiple charts share scripts.
            out["script"] = ev.get("code", "") if isinstance(ev, dict) else ""
            out["sql_index"] = ev.get("sql_index") if isinstance(ev, dict) else None
            return out

        slim_charts = [_slim_chart(c) for c in charts]
        slim_result = {
            "status": "success",
            "charts": slim_charts,
            "rationale": chart_result.get("rationale", ""),
            "errors": errors,
        }
        emit("complete", slim_result)
        return result

    except (ValueError, Exception) as e:
        chart_ms = (time.perf_counter() - t0) * 1000
        logger.error("Chart generation failed: %s", e)
        print(f"  {_RED}X Chart generation failed after {chart_ms:.0f}ms: {e}{_RESET}\n", flush=True)
        errors.append(f"Chart generation failed: {e}")
        emit("error", {"message": str(e)})
        return {"status": "error", "charts": [], "rationale": "", "traversal_steps": traversal_steps,
                "traversal_findings": traversal_findings, "errors": errors}
