"""
Traversal Agent — Autonomous ReAct agent that explores the Neo4j
Knowledge Graph and PostgreSQL to gather data for reporting.
"""
from __future__ import annotations

import json
import time
import logging
import warnings
from typing import Any

from langgraph.prebuilt import create_react_agent

from models.state import ReportingState, ToolCallRecord
from services.llm_provider import LLMProvider
from tools.langchain_tools import get_fast_tools
from prompts.traversal_prompt import TRAVERSAL_SYSTEM

logger = logging.getLogger(__name__)
logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)

DEFAULT_MAX_STEPS = 10
_BOTH_SMP_VALUES = ("NTM", "AHLOB Modernization")

# ── ANSI colors ──────────────────────────────────────────────────────────────
_CYAN = "\033[96m"
_GREEN = "\033[92m"
_YELLOW = "\033[93m"
_RED = "\033[91m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _print_divider(char: str = "-", width: int = 70):
    print(f"{_DIM}{char * width}{_RESET}", flush=True)


def _print_tool_call(step_num: int, tool_name: str, tool_input: dict):
    _print_divider()
    print(f"{_BOLD}{_CYAN}  Step {step_num}: {tool_name}{_RESET}", flush=True)
    for key, val in tool_input.items():
        val_str = str(val)
        if key == "code" and tool_name in ("run_sql_python", "run_python"):
            print(f"     {_DIM}{key}:{_RESET}", flush=True)
            for line in val_str.splitlines():
                print(f"       {_DIM}{line}{_RESET}", flush=True)
        else:
            if len(val_str) > 200:
                val_str = val_str[:200] + "..."
            print(f"     {_DIM}{key}:{_RESET} {val_str}", flush=True)


def _print_tool_result(status: str, output: str):
    if status == "error":
        icon, color = "X", _RED
    else:
        icon, color = "OK", _GREEN

    display = output
    try:
        parsed = json.loads(output)
        if isinstance(parsed, dict):
            if parsed.get("status") == "error":
                display = parsed.get("error", output)[:300]
            elif "result" in parsed:
                res = parsed["result"]
                if isinstance(res, dict):
                    keys = list(res.keys())
                    display = f"dict with keys: {keys}"
                elif isinstance(res, list):
                    display = f"list with {len(res)} item(s)"
                else:
                    display = str(res)[:300]
            else:
                display = str(parsed)[:300]
        else:
            display = str(parsed)[:300]
    except (json.JSONDecodeError, TypeError):
        display = output[:300]

    print(f"     {color}{icon} Result:{_RESET} {display}", flush=True)


def _print_agent_thinking(content: str):
    if not content.strip():
        return
    text = content.strip()
    if len(text) > 400:
        text = text[:400] + "..."
    print(f"  {_YELLOW}Agent:{_RESET} {text}", flush=True)


def _build_project_type_filter(project_type: str) -> str:
    """Build the prompt instruction for smp_name filtering on macro_combined."""
    if not project_type:
        return ""

    if project_type == "Both":
        smp_clause = f"smp_name IN ('{_BOTH_SMP_VALUES[0]}', '{_BOTH_SMP_VALUES[1]}')"
        return (
            f'\n12. **MANDATORY Project Type Filter**: The user selected **Both** project types. '
            f'Whenever you query the table '
            f'`pwc_macro_staging_schema.stg_ndpd_mbt_tmobile_macro_combined`, '
            f'you MUST include `WHERE {smp_clause}` (or add it as '
            f'an AND condition if other WHERE clauses exist). This filter is NON-NEGOTIABLE '
            f'— every single SQL query touching this table must have it. '
            f'This filter applies ONLY to `stg_ndpd_mbt_tmobile_macro_combined` — '
            f'do NOT add it to other tables. '
            f'When comparing project types, GROUP BY smp_name to show results side by side.\n'
            f'13. **NEVER use `pj_project_type`** to filter project type (AHLOA, NTM, etc.). '
            f'The correct column is ALWAYS `smp_name`. `pj_project_type` does NOT exist for '
            f'this purpose.'
        )

    return (
        f'\n12. **MANDATORY Project Type Filter**: The user selected project type '
        f'**{project_type}**. Whenever you query the table '
        f'`pwc_macro_staging_schema.stg_ndpd_mbt_tmobile_macro_combined`, '
        f'you MUST include `WHERE smp_name = \'{project_type}\'` (or add it as '
        f'an AND condition if other WHERE clauses exist). This filter is NON-NEGOTIABLE '
        f'— every single SQL query touching this table must have it. '
        f'This filter applies ONLY to `stg_ndpd_mbt_tmobile_macro_combined` — '
        f'do NOT add it to other tables.\n'
        f'13. **NEVER use `pj_project_type`** to filter project type (AHLOA, NTM, etc.). '
        f'The correct column is ALWAYS `smp_name`. `pj_project_type` does NOT exist for '
        f'this purpose.'
    )


def _extract_and_print(messages: list) -> tuple[list[ToolCallRecord], str]:
    """Walk the agent message history, print each step, return (tool_call_records, findings)."""
    records: list[ToolCallRecord] = []
    step_num = 0
    findings = "No findings extracted."

    for msg in messages:
        if msg.type == "ai":
            text = getattr(msg, "content", "") or ""
            if text.strip() and not getattr(msg, "tool_calls", None):
                findings = text
                _print_agent_thinking(text)

            if hasattr(msg, "tool_calls") and msg.tool_calls:
                for tc in msg.tool_calls:
                    step_num += 1
                    _print_tool_call(step_num, tc["name"], tc["args"])
                    records.append(ToolCallRecord(
                        tool_name=tc["name"],
                        tool_input=tc["args"],
                        tool_output="",
                        status="success",
                        execution_time_ms=0,
                    ))

        elif msg.type == "tool":
            output = msg.content or ""
            for rec in reversed(records):
                if rec["tool_output"] == "":
                    rec["tool_output"] = output
                    if "error" in output.lower()[:200]:
                        rec["status"] = "error"
                    _print_tool_result(rec["status"], output)
                    break

    return records, findings


def traversal_node(state: dict[str, Any]) -> dict[str, Any]:
    """Autonomous Traversal Agent for the Reporting pipeline."""
    warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

    print(f"\n{_BOLD}{'=' * 70}", flush=True)
    print(f"  TRAVERSAL AGENT — Investigating Data", flush=True)
    print(f"{'=' * 70}{_RESET}\n", flush=True)

    provider = LLMProvider(model="gpt-4o")
    llm = provider.get_llm()

    kg_schema = state.get("kg_schema", "Schema not available")
    safe_kg_schema = kg_schema.replace("{", "{{").replace("}", "}}")

    project_type = state.get("project_type", "")
    project_type_filter = _build_project_type_filter(project_type)

    print(f"  {_DIM}Project type in state: '{project_type}'{_RESET}", flush=True)
    if project_type:
        print(f"  {_GREEN}Project type filter injected for: {project_type}{_RESET}", flush=True)
    else:
        print(f"  {_YELLOW}No project type in state — smp_name filter NOT applied{_RESET}", flush=True)

    retrieval_context = state.get("retrieval_summary", "") or ""
    if retrieval_context:
        safe_retrieval_context = retrieval_context.replace("{", "{{").replace("}", "}}")
    else:
        safe_retrieval_context = ""

    # Conversation memory: a 3-5 line summary of the last few turns in this
    # thread, generated upstream in stream_report. Empty when there's no
    # prior history (or no thread_id).
    from services.thread_memory import render_for_prompt as _render_thread_context
    thread_context_block = _render_thread_context(state.get("thread_context", ""))
    safe_thread_context = thread_context_block.replace("{", "{{").replace("}", "}}")
    if thread_context_block:
        print(f"  {_GREEN}Thread context injected ({len(thread_context_block)} chars){_RESET}", flush=True)

    from datetime import date as _date
    system_prompt = TRAVERSAL_SYSTEM.format(
        kg_schema=safe_kg_schema,
        today_date=_date.today().isoformat(),
        project_type_filter=project_type_filter,
        thread_context=safe_thread_context,
        retrieval_context=safe_retrieval_context,
    )

    max_steps = state.get("max_traversal_steps", DEFAULT_MAX_STEPS)

    tools = get_fast_tools(project_type=project_type)
    agent = create_react_agent(
        model=llm,
        tools=tools,
        prompt=system_prompt,
    )

    print(f"\n{_DIM}  Query: {state['user_query']}{_RESET}", flush=True)
    print(f"{_DIM}  Max steps: {max_steps}{_RESET}", flush=True)
    print(f"{_DIM}  Pre-retrieval context injected: "
          f"{'yes' if retrieval_context else 'no'}{_RESET}\n", flush=True)

    start_time = time.perf_counter()
    try:
        result = agent.invoke(
            {"messages": [("human", state["user_query"])]},
            config={"recursion_limit": max_steps * 3 + 10},
        )

        elapsed = time.perf_counter() - start_time
        agent_messages = result.get("messages", [])
        tool_call_records, findings = _extract_and_print(agent_messages)
        steps_taken = len(tool_call_records)

        _print_divider("=")
        print(f"  {_BOLD}Traversal complete: {steps_taken} tool calls{_RESET}", flush=True)
        print(f"  {_DIM}Total time: {elapsed:.1f}s{_RESET}", flush=True)
        _print_divider("=")
        print(flush=True)

        return {
            "traversal_findings": findings,
            "traversal_tool_calls": tool_call_records,
            "traversal_steps_taken": steps_taken,
            "current_phase": "charting",
            "messages": [{"agent": "traversal", "content": f"Data gathering complete: {steps_taken} tool calls, {elapsed:.1f}s elapsed"}],
        }

    except Exception as e:
        elapsed = time.perf_counter() - start_time
        logger.error("Traversal agent failed: %s", e)
        print(f"\n  {_RED}Traversal failed after {elapsed:.1f}s: {e}{_RESET}\n", flush=True)
        return {
            "traversal_findings": f"Traversal failed: {e}",
            "traversal_tool_calls": [],
            "traversal_steps_taken": 0,
            "current_phase": "error",
            "errors": [f"Traversal agent error: {e}"],
            "messages": [{"agent": "traversal", "content": f"Traversal failed after {elapsed:.1f}s: {e}"}],
        }


async def atraversal_node(state: dict[str, Any]) -> dict[str, Any]:
    """Async version of traversal_node."""
    warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

    provider = LLMProvider(model="gpt-4o")
    llm = provider.get_llm()

    kg_schema = state.get("kg_schema", "Schema not available")
    safe_kg_schema = kg_schema.replace("{", "{{").replace("}", "}}")

    project_type = state.get("project_type", "")
    project_type_filter = _build_project_type_filter(project_type)

    retrieval_context = state.get("retrieval_summary", "") or ""
    safe_retrieval_context = retrieval_context.replace("{", "{{").replace("}", "}}") if retrieval_context else ""

    from datetime import date as _date
    system_prompt = TRAVERSAL_SYSTEM.format(
        kg_schema=safe_kg_schema,
        today_date=_date.today().isoformat(),
        project_type_filter=project_type_filter,
        retrieval_context=safe_retrieval_context,
    )

    max_steps = state.get("max_traversal_steps", DEFAULT_MAX_STEPS)
    tools = get_fast_tools(project_type=project_type)
    agent = create_react_agent(model=llm, tools=tools, prompt=system_prompt)

    query = state["user_query"]

    start_time = time.perf_counter()
    try:
        result = await agent.ainvoke(
            {"messages": [("human", query)]},
            config={"recursion_limit": max_steps * 3 + 10},
        )
        elapsed = time.perf_counter() - start_time
        agent_messages = result.get("messages", [])
        tool_call_records, findings = _extract_and_print(agent_messages)
        steps_taken = len(tool_call_records)

        return {
            "traversal_findings": findings,
            "traversal_tool_calls": tool_call_records,
            "traversal_steps_taken": steps_taken,
            "current_phase": "charting",
            "messages": [{"agent": "traversal", "content": f"Data gathering complete: {steps_taken} tool calls, {elapsed:.1f}s"}],
        }

    except Exception as e:
        elapsed = time.perf_counter() - start_time
        logger.error("Async traversal failed after %.1fs: %s", elapsed, e)
        print(f"\n  {_RED}Traversal failed after {elapsed:.1f}s: {e}{_RESET}\n", flush=True)
        return {
            "traversal_findings": f"Traversal failed: {e}",
            "traversal_tool_calls": [],
            "traversal_steps_taken": 0,
            "current_phase": "error",
            "errors": [f"Traversal agent error: {e}"],
            "messages": [{"agent": "traversal", "content": f"Traversal failed after {elapsed:.1f}s: {e}"}],
        }
