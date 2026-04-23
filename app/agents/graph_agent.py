"""
Graph Agent — LLM-powered Highcharts chart generation.

Analyzes raw tool call outputs from the traversal agent and produces
insightful Highcharts configuration objects.
"""
from __future__ import annotations

import json
import time
import logging
from typing import Any

from services.llm_provider import LLMProvider
from prompts.graph_agent_prompt import GRAPH_AGENT_SYSTEM, GRAPH_AGENT_USER

logger = logging.getLogger(__name__)

MAX_RETRIES = 2

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


def _strip_markdown_fences(content: str) -> str:
    """Remove markdown code fences if the LLM wraps its JSON output."""
    content = content.strip()
    if content.startswith("```"):
        first_newline = content.index("\n")
        content = content[first_newline + 1:]
        if content.endswith("```"):
            content = content[:-3].strip()
    return content


def _validate_chart_structure(parsed: dict) -> list[str]:
    """Validate the parsed JSON has the required structure. Returns list of issues."""
    issues = []

    if not isinstance(parsed, dict):
        issues.append("Response is not a JSON object")
        return issues

    if "charts" not in parsed:
        issues.append("Missing required 'charts' key")
        return issues

    if not isinstance(parsed["charts"], list):
        issues.append("'charts' must be an array")
        return issues

    for i, chart in enumerate(parsed["charts"]):
        if not isinstance(chart, dict):
            issues.append(f"Chart {i+1} is not a JSON object")
            continue

        chart_type = None
        if "chart" in chart and isinstance(chart["chart"], dict):
            chart_type = chart["chart"].get("type")
        elif "type" in chart:
            chart_type = chart["type"]

        if not chart_type:
            issues.append(f"Chart {i+1} missing chart type (need chart.type or type)")

        if "title" not in chart:
            issues.append(f"Chart {i+1} missing title")

        if "series" not in chart or not isinstance(chart.get("series"), list):
            issues.append(f"Chart {i+1} missing series array")
        elif chart["series"]:
            for j, s in enumerate(chart["series"]):
                if "data" not in s:
                    issues.append(f"Chart {i+1}, series {j+1} missing data")

    return issues


def _format_tool_call_outputs(tool_calls: list[dict]) -> str:
    """Format raw run_sql_python tool call outputs for the LLM.

    Passes each run_sql_python output as-is — the LLM understands JSON
    natively and can extract chartable data from any structure.
    Each section is labeled `## SQL Result N` (1-indexed); the LLM is instructed
    to set `evidence_sql_index` on each chart to the matching N so the UI can
    later link the chart back to the code + rows.
    """
    sections = []
    sql_idx = 0

    for tc in tool_calls:
        if tc.get("tool_name") != "run_sql_python":
            continue

        sql_idx += 1
        output = tc.get("tool_output", "") or ""
        code = ""
        try:
            tool_input = tc.get("tool_input") or {}
            code = tool_input.get("code", "") if isinstance(tool_input, dict) else ""
        except Exception:
            code = ""

        # Never truncate: a clipped Python script becomes broken code and a clipped
        # JSON output becomes invalid JSON — either would make the chart agent flop.
        code_preview = code.strip()

        section = [f"## SQL Result {sql_idx}"]
        if code_preview:
            section.append("### Python/SQL used:")
            section.append("```python")
            section.append(code_preview)
            section.append("```")
        section.append("### Output:")
        section.append(output)
        sections.append("\n".join(section))

    if not sections:
        return "No SQL execution results available."

    return "\n\n---\n\n".join(sections)


def _extract_evidence_records(tool_calls: list[dict]) -> list[dict[str, Any]]:
    """Return a 1-indexed list describing each run_sql_python call.

    Each record is: { "index": int, "code": str, "result": any, "status": str, "error": str }.
    The UI uses this to display the exact Python + SQL that produced each chart.
    """
    evidence: list[dict[str, Any]] = []
    sql_idx = 0
    for tc in tool_calls:
        if tc.get("tool_name") != "run_sql_python":
            continue
        sql_idx += 1
        code = ""
        try:
            ti = tc.get("tool_input") or {}
            if isinstance(ti, dict):
                code = ti.get("code", "")
        except Exception:
            code = ""

        raw = tc.get("tool_output", "")
        parsed: Any = None
        status = "success"
        err = ""
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict) and parsed.get("status") == "error":
                status = "error"
                err = parsed.get("error", "")
        except (json.JSONDecodeError, TypeError):
            parsed = raw

        result_payload: Any = parsed
        if isinstance(parsed, dict):
            if "result" in parsed:
                result_payload = parsed["result"]
            elif "records" in parsed:
                result_payload = parsed["records"]

        evidence.append({
            "index": sql_idx,
            "code": code,
            "result": result_payload,
            "raw": parsed,
            "status": status,
            "error": err,
        })

    return evidence


DEFAULT_COLOR_PALETTE = [
    "#2E86AB",  # primary blue
    "#F18F01",  # orange
    "#A23B72",  # magenta
    "#3B8EA5",  # teal
    "#C73E1D",  # red
    "#6A994E",  # green
]


def _assign_chart_ids(charts: list[dict]) -> None:
    """Stamp each chart with a UUID so callers can reference it without
    relying on its position in the array.

    `chart_id` is the stable handle used by /charts/edit, canvas slots,
    template selections, and DB-side chart-edit propagation. Charts
    already carrying a `chart_id` (e.g. coming back from chart-edit)
    keep their existing value.
    """
    import uuid
    for c in charts:
        if not isinstance(c, dict):
            continue
        if not c.get("chart_id"):
            c["chart_id"] = str(uuid.uuid4())


def _ensure_default_colors(charts: list[dict]) -> None:
    """Defensive backstop for prompt rule #15.

    Newly-generated charts must use the fixed palette so saved reports stay
    visually consistent. The chart-edit API is the only path that should
    change colors. If a chart already has a `colors` array (e.g. a prior
    edit applied a custom one), leave it alone.
    """
    for c in charts:
        if not isinstance(c, dict):
            continue
        existing = c.get("colors")
        if isinstance(existing, list) and existing:
            continue
        c["colors"] = list(DEFAULT_COLOR_PALETTE)


def _round_floats(node: Any, ndigits: int = 2) -> Any:
    """Recursively round every float in a chart config to `ndigits` decimals.

    Defensive backstop for the prompt's "max 2 decimals" rule — the LLM
    sometimes emits 0.3333333333. Integers, bools, and non-numeric values
    pass through untouched.
    """
    if isinstance(node, float):
        # bool is a subclass of int (not float), so no special-case needed
        return round(node, ndigits)
    if isinstance(node, dict):
        for k, v in node.items():
            node[k] = _round_floats(v, ndigits)
        return node
    if isinstance(node, list):
        for i, v in enumerate(node):
            node[i] = _round_floats(v, ndigits)
        return node
    return node


def _attach_evidence_to_charts(charts: list[dict], evidence: list[dict]) -> None:
    """Mutate each chart dict to include its linked evidence payload."""
    ev_by_index = {e["index"]: e for e in evidence}
    fallback = evidence[-1] if evidence else None
    for c in charts:
        idx = c.get("evidence_sql_index")
        ev = ev_by_index.get(idx) if isinstance(idx, int) else None
        if ev is None:
            ev = fallback
        if ev is not None:
            c["evidence"] = {
                "sql_index": ev["index"],
                "code": ev.get("code", ""),
                "result": ev.get("result"),
                "status": ev.get("status", "success"),
                "error": ev.get("error", ""),
            }


def generate_charts(
    user_query: str,
    tool_calls: list[dict],
    traversal_findings: str,
    max_charts: int = 3,
) -> dict[str, Any]:
    """
    Generate Highcharts configurations from raw traversal tool call outputs.

    Passes run_sql_python outputs directly to GPT-4o — the LLM extracts
    chartable data from any JSON structure without brittle parsing.

    Returns:
        {"charts": [...], "rationale": "..."}

    Raises:
        ValueError if all retries fail to produce valid JSON.
    """
    print(f"\n{_BOLD}{'=' * 70}", flush=True)
    print(f"  CHART AGENT — Generating Highcharts Visualizations", flush=True)
    print(f"{'=' * 70}{_RESET}\n", flush=True)

    # Count run_sql_python calls
    sql_calls = [tc for tc in tool_calls if tc.get("tool_name") == "run_sql_python"]
    print(f"  {_DIM}SQL tool calls: {len(sql_calls)}, max charts: {max_charts}{_RESET}", flush=True)
    print(f"  {_DIM}Findings: {len(traversal_findings)} chars{_RESET}\n", flush=True)

    provider = LLMProvider(model="gpt-4o", temperature=0.1)

    tool_call_outputs = _format_tool_call_outputs(tool_calls)
    print(f"  {_DIM}Formatted tool outputs for LLM: {len(tool_call_outputs)} chars{_RESET}", flush=True)

    system_prompt = GRAPH_AGENT_SYSTEM.format(max_charts=max_charts)
    user_message = GRAPH_AGENT_USER.format(
        user_query=user_query,
        traversal_findings=traversal_findings,
        tool_call_outputs=tool_call_outputs,
        max_charts=max_charts,
    )

    messages = [
        ("system", system_prompt),
        ("human", user_message),
    ]

    last_error = None

    for attempt in range(MAX_RETRIES + 1):
        _print_divider()
        print(f"{_BOLD}{_CYAN}  LLM Call {attempt + 1}/{MAX_RETRIES + 1}{_RESET}", flush=True)
        t0 = time.perf_counter()
        try:
            response = provider.invoke(messages)
            llm_ms = (time.perf_counter() - t0) * 1000
            content = response.content.strip()
            print(f"     {_GREEN}OK Response:{_RESET} {len(content)} chars in {llm_ms:.0f}ms", flush=True)

            content = _strip_markdown_fences(content)

            try:
                parsed = json.loads(content)
            except json.JSONDecodeError as e:
                last_error = f"Invalid JSON: {e}"
                print(f"     {_RED}X JSON parse failed:{_RESET} {e}", flush=True)
                print(f"     {_DIM}Preview: {content[:150]}...{_RESET}", flush=True)
                if attempt < MAX_RETRIES:
                    messages.append(("assistant", content))
                    messages.append(("human",
                        f"Your response was not valid JSON. Error: {e}. "
                        f"Please output ONLY a valid JSON object with 'charts' and 'rationale' keys. "
                        f"No markdown, no code fences, no text outside the JSON."
                    ))
                    continue
                raise ValueError(f"Failed to get valid JSON after {MAX_RETRIES + 1} attempts: {e}")

            issues = _validate_chart_structure(parsed)
            if issues:
                last_error = f"Structural issues: {'; '.join(issues)}"
                print(f"     {_RED}X Validation failed:{_RESET} {'; '.join(issues)}", flush=True)
                if attempt < MAX_RETRIES:
                    messages.append(("assistant", content))
                    messages.append(("human",
                        f"Your JSON is valid but has structural issues: {'; '.join(issues)}. "
                        f"Fix these issues and output the corrected JSON. "
                        f"Every chart needs: chart.type, title, and series with data."
                    ))
                    continue
                if "charts" in parsed:
                    logger.warning("Chart validation issues on final attempt: %s", issues)
                    break
                raise ValueError(f"Invalid chart structure after {MAX_RETRIES + 1} attempts: {issues}")

            if "rationale" not in parsed:
                parsed["rationale"] = "Charts generated based on the available data."

            # Defensive backstops for the prompt rules + identity:
            #   #14   — every float in the chart config rounded to 2 decimals
            #   #15   — every chart carries the fixed `colors` palette
            #   id    — every chart gets a stable UUID `chart_id`
            # All three run BEFORE evidence attachment so the raw evidence rows
            # keep full precision for debugging.
            _round_floats(parsed.get("charts", []))
            _ensure_default_colors(parsed.get("charts", []))
            _assign_chart_ids(parsed.get("charts", []))

            # Attach per-chart evidence: the Python/SQL code + resulting rows that
            # produced the chart. The UI (and report templates) need this so every
            # chart can be shown alongside its generating script and data.
            evidence_records = _extract_evidence_records(tool_calls)
            _attach_evidence_to_charts(parsed.get("charts", []), evidence_records)
            parsed["evidence"] = evidence_records

            # Print chart summary
            print(flush=True)
            charts = parsed.get("charts", [])
            for i, c in enumerate(charts, 1):
                ctype = c.get("chart", {}).get("type", c.get("type", "?"))
                ctitle = c.get("title", {}).get("text", "?") if isinstance(c.get("title"), dict) else str(c.get("title", "?"))
                num_series = len(c.get("series", []))
                print(f"  {_CYAN}  Chart {i}:{_RESET} {ctype} — \"{ctitle}\" ({num_series} series)", flush=True)

            rationale = parsed.get("rationale", "")
            if rationale:
                print(f"\n  {_YELLOW}Rationale:{_RESET} {rationale[:200]}", flush=True)

            _print_divider("=")
            print(f"  {_BOLD}Chart generation complete: {len(charts)} chart(s){_RESET}", flush=True)
            _print_divider("=")
            print(flush=True)

            return parsed

        except ValueError:
            raise
        except Exception as e:
            llm_ms = (time.perf_counter() - t0) * 1000
            last_error = str(e)
            logger.error("Chart generation attempt %d failed: %s", attempt + 1, e)
            print(f"     {_RED}X Exception after {llm_ms:.0f}ms:{_RESET} {e}", flush=True)
            if attempt >= MAX_RETRIES:
                raise ValueError(f"Chart generation failed after {MAX_RETRIES + 1} attempts: {last_error}")

    # Fallback — should not reach here normally
    return parsed
