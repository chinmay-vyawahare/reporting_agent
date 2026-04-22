"""
LangChain tool wrappers for the Reporting Agent's Traversal Agent.
Wraps existing tools (neo4j_tool, bkg_tool, python_sandbox) as
@tool functions that the ReAct agent can call.
"""
from __future__ import annotations

import json
import re
from typing import Optional

from langchain_core.tools import tool, StructuredTool

from tools.neo4j_tool import neo4j_tool
from tools.bkg_tool import BKGTool
from tools.python_sandbox import execute_python, PythonSandbox


def _truncate_tool_output(tool_name: str, raw_json: str) -> str:
    """Passthrough — tool output is returned to the traversal agent in full.

    Truncating tool responses that flow back into the ReAct loop means the
    LLM sees partial JSON, half-rows, or Python code snipped mid-string —
    which derails subsequent steps and corrupts chart generation downstream.
    We deliberately never clip here; the traversal prompt already instructs
    the agent to aggregate/GROUP BY in SQL so the natural payload stays small.
    """
    return raw_json


# Lazy singleton for BKGTool
_bkg: BKGTool | None = None

def _get_bkg() -> BKGTool:
    global _bkg
    if _bkg is None:
        _bkg = BKGTool()
    return _bkg


@tool
def run_cypher(query: str) -> str:
    """Execute a read-only Cypher query against the Neo4j Business Knowledge Graph."""
    result = neo4j_tool.run_cypher_safe(query)
    return _truncate_tool_output("run_cypher", json.dumps(result, default=str))


@tool
def get_node(node_id: str) -> str:
    """Fetch a single BKGNode from the Knowledge Graph by its node_id.
    Returns all properties plus incoming and outgoing relationships.
    Supports aliases like 'GC' for general_contractor, 'NAS' for nas_session, etc."""
    result = _get_bkg().query({"mode": "get_node", "node_id": node_id})

    # Process map_python_function: extract GROUP BY dimensions and replace
    # with a placeholder so the traversal agent picks only relevant ones
    if isinstance(result, dict):
        func_str = result.get("map_python_function")
        if func_str and isinstance(func_str, str):
            extracted = _extract_group_by_dimensions(func_str)
            if extracted:
                result["map_python_function"] = extracted["modified_function"]
                result["available_group_by_dimensions"] = extracted["available_dimensions"]

    return _truncate_tool_output("get_node", json.dumps(result, default=str))


@tool
def find_relevant(question: str) -> str:
    """Keyword search across all BKGNodes in the Knowledge Graph.
    Returns up to 10 nodes ranked by relevance score."""
    result = _get_bkg().query({"mode": "find_relevant", "question": question})
    return _truncate_tool_output("find_relevant", json.dumps(result, default=str))


@tool
def traverse_graph(start: str, depth: int = 2, rel_type: Optional[str] = None) -> str:
    """Walk the Knowledge Graph starting from a BKGNode, following RELATES_TO
    relationships up to a given depth (1-4)."""
    req: dict = {"mode": "traverse", "start": start, "depth": depth}
    if rel_type:
        req["rel_type"] = rel_type
    result = _get_bkg().query(req)
    return _truncate_tool_output("traverse_graph", json.dumps(result, default=str))


# ─────────────────────────────────────────────
# GROUP BY dimension extraction
# ─────────────────────────────────────────────

def _extract_group_by_dimensions(python_function: str) -> dict | None:
    """
    Parse a kpi_python_function / map_python_function string to:
    1. Extract the GROUP BY column list
    2. Replace the GROUP BY line with a placeholder comment

    This lets the traversal agent choose only the dimensions
    relevant to the user's query instead of using all hardcoded ones.
    """
    if not python_function or not isinstance(python_function, str):
        return None

    pattern = r'(GROUP\s+BY\s+)([\w\s,\.]+?)(\s*(?:\n|"""|\'\'\'|\)|$))'
    match = re.search(pattern, python_function, re.IGNORECASE)
    if not match:
        return None

    raw_columns = match.group(2)
    dimensions = [col.strip() for col in raw_columns.split(",") if col.strip()]
    if not dimensions:
        return None

    placeholder = "-- GROUP BY: <SELECT from available_dimensions based on your sub-query granularity>"
    modified_function = (
        python_function[:match.start()]
        + placeholder
        + python_function[match.end():]
    )

    return {
        "available_dimensions": dimensions,
        "modified_function": modified_function,
    }


@tool
def get_kpi(node_id: str) -> str:
    """Get detailed information about a KPI node including its definition,
    formula description, business logic, Python function, source tables/columns,
    dimensions, filters, output schema, and related core nodes."""
    result = _get_bkg().query({"mode": "get_kpi", "node_id": node_id})

    # Process kpi_python_function: extract GROUP BY dimensions and replace
    # with a placeholder so the traversal agent picks only relevant ones
    if isinstance(result, dict):
        for key in ("kpi_python_function", "map_python_function"):
            func_str = result.get(key)
            if func_str and isinstance(func_str, str):
                extracted = _extract_group_by_dimensions(func_str)
                if extracted:
                    result[key] = extracted["modified_function"]
                    result["available_group_by_dimensions"] = extracted["available_dimensions"]

    return _truncate_tool_output("get_kpi", json.dumps(result, default=str))


@tool
def run_python(code: str) -> str:
    """Execute Python code in a sandboxed environment for calculations.
    Available modules: math, json, statistics, collections, datetime, itertools, functools.
    Set a variable named 'result' to return structured data."""
    result = execute_python(code)
    return _truncate_tool_output("run_python", json.dumps(result, default=str))


@tool
def run_sql_python(code: str, timeout_seconds: int = 30) -> str:
    """Execute Python code with access to a PostgreSQL database connection.
    Pre-imported: conn (psycopg2 read-only), pd (pandas), np (numpy), json,
    execute_query (helper: execute_query(sql) -> list[dict]).
    Set result = {...} to return data."""
    sandbox = PythonSandbox()
    result = sandbox.execute(code, timeout_seconds)
    return _truncate_tool_output("run_sql_python", json.dumps(result, default=str))


# Project-type filter enforcement on SQL tool
_MACRO_TABLE = "stg_ndpd_mbt_tmobile_macro_combined"
_BOTH_SMP_VALUES = ("NTM", "AHLOB Modernization")


def _check_macro_combined_filter(code: str, project_type: str) -> str | None:
    """If code references the macro_combined table, verify smp_name filter is present."""
    if _MACRO_TABLE not in code:
        return None

    if project_type == "Both":
        has_in = re.search(r"smp_name\s+IN\s*\(", code, re.IGNORECASE)
        has_both = (_BOTH_SMP_VALUES[0] in code and _BOTH_SMP_VALUES[1] in code)
        if has_in and has_both:
            return None
        return (
            f"ERROR: Your SQL references `{_MACRO_TABLE}` but is missing the "
            f"mandatory filter: smp_name IN ('{_BOTH_SMP_VALUES[0]}', '{_BOTH_SMP_VALUES[1]}'). "
            f"Add this WHERE/AND condition and retry."
        )

    # Match smp_name = 'NTM' or smp_name = "NTM" or smp_name='NTM' or
    # smp_name = '{project_type}' (f-string) or smp_name = variable
    patterns = [
        re.compile(rf"""smp_name\s*=\s*'{re.escape(project_type)}'""", re.IGNORECASE),
        re.compile(rf'''smp_name\s*=\s*"{re.escape(project_type)}"''', re.IGNORECASE),
        re.compile(r"""smp_name\s*=\s*['\"]?\{""", re.IGNORECASE),  # f-string variable
        re.compile(rf"""smp_name\s*=\s*%s""", re.IGNORECASE),       # parameterized query
        re.compile(rf"""smp_name\s*=\s*\?""", re.IGNORECASE),       # parameterized query
    ]
    for pattern in patterns:
        if pattern.search(code):
            return None

    # Also check if the project_type value appears near smp_name in any form
    if "smp_name" in code.lower() and project_type in code:
        return None

    return (
        f"ERROR: Your SQL references `{_MACRO_TABLE}` but is missing the "
        f"mandatory filter: smp_name = '{project_type}'. "
        f"Add this WHERE/AND condition and retry."
    )


def _make_filtered_run_sql_python(project_type: str) -> StructuredTool:
    """Create a run_sql_python tool that validates smp_name filter before execution."""

    @tool
    def run_sql_python_filtered(code: str, timeout_seconds: int = 30) -> str:
        """Execute Python code with access to a PostgreSQL database connection.
        Pre-imported: conn (psycopg2 read-only), pd (pandas), np (numpy), json,
        execute_query (helper: execute_query(sql) -> list[dict]).
        Set result = {...} to return data."""
        err = _check_macro_combined_filter(code, project_type)
        if err:
            return json.dumps({"status": "error", "error": err})

        sandbox = PythonSandbox()
        result = sandbox.execute(code, timeout_seconds)
        return _truncate_tool_output("run_sql_python", json.dumps(result, default=str))

    run_sql_python_filtered.name = "run_sql_python"
    return run_sql_python_filtered


# Tool registry

def get_all_tools(project_type: str = "") -> list:
    """Return all tools for the traversal agent."""
    sql_tool = _make_filtered_run_sql_python(project_type) if project_type else run_sql_python
    return [run_cypher, get_node, find_relevant, traverse_graph, get_kpi, run_python, sql_tool]


def get_fast_tools(project_type: str = "") -> list:
    """Minimal tool set: get_kpi, get_node, run_sql_python, run_python."""
    sql_tool = _make_filtered_run_sql_python(project_type) if project_type else run_sql_python
    return [get_kpi, get_node, sql_tool, run_python]


def get_analysis_tools() -> list:
    """Return tools for the analysis agent (python sandbox only)."""
    return [run_python]
