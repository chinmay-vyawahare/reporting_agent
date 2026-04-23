"""
Chart-edit service — schema-aware natural-language patcher.

Takes a Highcharts config + an NL instruction (e.g. "make the bars red",
"rotate the pie 30 degrees", "rename the x-axis to Market") and returns the
patched config. The LLM is given the EXACT per-type schema so it knows which
key to mutate without breaking validation.

Two flavours, dispatched on `chart.chart.type`:
  cartesian  → CartesianChart schema
  pie / donut → PieChart schema

Output is validated against `models.chart_types.parse_chart` before returning.
If the LLM produces something off-schema, we raise ValueError so the caller
can surface a clean 400.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from pydantic import ValidationError

from models.chart_types import (
    CARTESIAN_TYPES,
    PIE_TYPES,
    parse_chart,
)
from services.llm_provider import LLMProvider

logger = logging.getLogger(__name__)


# ── Per-type schema cheat sheets the LLM sees ───────────────────────────────

_CARTESIAN_REFERENCE = """\
This is a CARTESIAN chart (chart.type ∈ {column, bar, line, area, spline, areaspline, scatter}).
Mutate ONLY these keys:
  chart.type           — only if user asks to change shape (e.g. "make it a line chart")
  colors[]             — top-level palette; affects every series in order
  series[i].color      — override one series' color
  series[i].name       — series label in legend / tooltip
  title.text           — chart title
  subtitle.text        — scope context line
  xAxis.categories[]   — x-tick labels (REORDER only — don't drop unless user says)
  xAxis.title.text     — x axis title
  yAxis.title.text     — y axis title
  yAxis.min / max      — bound the y axis if asked
  legend.enabled       — show / hide legend
  tooltip.valueSuffix  — unit suffix
  plotOptions.<type>.dataLabels.enabled — show / hide value labels
  plotOptions.<type>.stacking — "normal" / "percent" for stacked variants
NEVER touch series[i].data values (only the order may change if categories reorder)."""

_PIE_REFERENCE = """\
This is a PIE chart (chart.type ∈ {pie, donut}).
Mutate ONLY these keys:
  chart.type            — only if user asks to switch between pie and donut
  colors[]              — slice palette (order matches series[0].data)
  series[0].name        — series label
  series[0].data[i].name  — slice label
  series[0].data[i].color — per-slice color override
  title.text            — chart title
  subtitle.text         — scope context line
  legend.enabled        — show / hide legend
  tooltip.valueSuffix   — unit suffix
  plotOptions.pie.dataLabels.enabled / format — slice labels
  plotOptions.pie.startAngle / innerSize — rotation / donut hole size
NEVER touch series[0].data[i].y values."""


# Identity keys we add back unconditionally — the LLM is told to keep them
# but we also re-stamp them defensively in case it drops anything.
_IDENTITY_KEYS = ("chart_id", "script", "sql_index", "description", "insight")


def _system_prompt(chart_type: str) -> str:
    if chart_type in CARTESIAN_TYPES:
        type_block = _CARTESIAN_REFERENCE
    elif chart_type in PIE_TYPES:
        type_block = _PIE_REFERENCE
    else:
        type_block = "(Unknown chart type — proceed with caution.)"

    return (
        "You are a Highcharts chart-config editor. You receive a chart's full\n"
        "JSON options object and one natural-language edit instruction. Return\n"
        "the SAME object with the instruction applied.\n\n"
        f"{type_block}\n\n"
        "Hard rules:\n"
        "  * Output ONLY the updated JSON object. No markdown fences, no commentary.\n"
        "  * Keep the keys: chart_id, script, sql_index, description, insight — DO NOT remove or modify them.\n"
        "  * Never invent new numeric series values.\n"
        "  * Match the existing color-hex format (e.g. \"#2E86AB\").\n"
        "  * If the instruction is ambiguous, make the smallest, most conservative edit that satisfies it.\n"
        "  * Round any new numeric values you DO add (axis bounds, etc.) to 2 decimals.\n"
    )


def _strip_fences(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        nl = text.find("\n")
        if nl >= 0:
            text = text[nl + 1:]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
    return text.strip()


def apply_chart_edit(
    chart_config: dict[str, Any],
    instruction: str,
    model: str = "gpt-4o",
) -> dict[str, Any]:
    """Return a patched copy of `chart_config` reflecting `instruction`.

    The LLM is given the per-type schema for the chart's type so it knows
    which exact key/value to touch. Output is then validated against the
    strict CartesianChart / PieChart Pydantic schema. ValueError is raised
    on empty instruction, invalid LLM JSON, or schema mismatch.
    """
    if not instruction or not instruction.strip():
        raise ValueError("Edit instruction is empty.")

    chart_type = ((chart_config.get("chart") or {}).get("type") or "").lower()
    if chart_type not in CARTESIAN_TYPES and chart_type not in PIE_TYPES:
        raise ValueError(
            f"Cannot edit chart of unknown type {chart_type!r}. "
            f"Allowed: {list(CARTESIAN_TYPES) + list(PIE_TYPES)}"
        )

    # Preserve identity / runtime fields — the LLM is told not to touch them
    # but we re-attach them after the round-trip just in case.
    preserved = {k: chart_config[k] for k in _IDENTITY_KEYS if k in chart_config}

    user_message = (
        f"# Current chart options (JSON)\n{json.dumps(chart_config, default=str)}\n\n"
        f"# Edit instruction\n{instruction.strip()}\n\n"
        "Return the updated options object."
    )

    provider = LLMProvider(model=model, temperature=0.0)
    t0 = time.perf_counter()
    resp = provider.invoke([
        ("system", _system_prompt(chart_type)),
        ("human",  user_message),
    ])
    elapsed_ms = (time.perf_counter() - t0) * 1000

    raw = _strip_fences(getattr(resp, "content", "") or "")
    try:
        patched = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("Chart-edit JSON parse failed: %s | preview=%s", e, raw[:200])
        raise ValueError(f"LLM did not return valid JSON: {e}")

    if not isinstance(patched, dict):
        raise ValueError(f"LLM returned a {type(patched).__name__}, expected object.")

    # Re-stamp preserved identity fields if the LLM dropped any.
    for k, v in preserved.items():
        patched.setdefault(k, v)

    # Validate against the strict per-type schema.
    try:
        parse_chart(patched)
    except (ValidationError, ValueError) as e:
        logger.error("Chart-edit produced off-schema output: %s", e)
        raise ValueError(f"Edited chart failed schema validation: {e}")

    patched["_edit_history"] = (chart_config.get("_edit_history") or []) + [{
        "instruction": instruction.strip(),
        "at": time.time(),
        "elapsed_ms": round(elapsed_ms, 2),
    }]
    return patched
