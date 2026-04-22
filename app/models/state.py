"""
Shared state models for the Reporting Agent system.
"""
from __future__ import annotations

import operator
from typing import Any, Literal, TypedDict, Annotated


class ToolCallRecord(TypedDict):
    """Record of a single tool invocation by the traversal agent."""
    tool_name: str
    tool_input: dict[str, Any]
    tool_output: Any
    status: Literal["success", "error"]
    execution_time_ms: float


class ReportingState(TypedDict):
    """
    Shared state for the reporting agent pipeline.
    Uses Annotated + operator.add for list fields so that
    each node appends rather than overwrites.
    """
    # ── Input ──
    user_query: str
    project_type: str            # "NTM" | "AHLOB Modernization" | "Both" | ""

    # ── Phase tracking ──
    current_phase: Literal[
        "discovery", "traversal", "charting", "complete", "error"
    ]

    # ── Knowledge Graph Schema (discovered once) ──
    kg_schema: str

    # ── Embedding-based retrieval (runs before traversal) ──
    retrieval_used: bool
    retrieval_summary: str                       # Markdown block for prompt
    retrieval_nodes: list[dict[str, Any]]        # top nodes w/ scores + props
    retrieval_paths: list[dict[str, Any]]        # top paths w/ scores
    retrieval_hydrated: dict[str, Any]           # element_id -> full Neo4j record

    # ── Traversal Agent ──
    traversal_findings: str
    traversal_tool_calls: Annotated[list[ToolCallRecord], operator.add]
    traversal_steps_taken: int
    max_traversal_steps: int

    # ── Chart Output ──
    charts: list[dict[str, Any]]
    rationale: str

    # ── Error handling ──
    errors: Annotated[list[str], operator.add]

    # ── Metadata ──
    messages: Annotated[list[dict], operator.add]
