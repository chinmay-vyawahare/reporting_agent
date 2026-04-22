"""
Chart-edit service.

Takes a Highcharts config and a natural-language instruction such as
    "change the color of the bars to red"
    "rename the x-axis to 'Market', y-axis to 'Pending Sites'"
    "sort bars descending and hide the legend"
and returns the patched config.

Rules the LLM is held to:
  * Mutate ONLY styling / labels / sorting / ordering — never fabricate numbers.
  * Keep series[*].data arrays the same length and the same values.
  * Output only JSON, no markdown.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from services.llm_provider import LLMProvider

logger = logging.getLogger(__name__)


_SYSTEM_PROMPT = """You are a Highcharts chart-config editor.

You receive:
  1. A Highcharts options object (JSON).
  2. A natural-language edit instruction from the user.

Return the SAME options object with the instruction applied. You may modify:
  - colors (per-series `color`, per-point `{y, color}`, `colors: []` palette)
  - titles, subtitles, axis titles, axis labels, tooltip text
  - series names, category labels
  - legend / dataLabels / plotOptions styling
  - sorting of categories + series data in a matching order
  - chart.type (only if the user explicitly asks to change it)

You MUST NOT:
  - invent or change numeric values in series[*].data (other than reordering)
  - drop or duplicate categories unless the user explicitly says so
  - remove the `description` or `evidence` fields if they exist — leave them intact

Output rules:
  - Return ONLY the updated options object as a single JSON object.
  - No markdown code fences, no commentary, no trailing text.
  - If the instruction is ambiguous, make the most reasonable conservative edit
    and leave the rest untouched.
"""


def _strip_fences(text: str) -> str:
    text = text.strip()
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
    """
    Return a patched copy of `chart_config` reflecting `instruction`.

    Raises ValueError if the LLM response cannot be parsed as a JSON object.
    """
    if not instruction or not instruction.strip():
        raise ValueError("Edit instruction is empty.")

    # Preserve auxiliary fields the UI depends on — the LLM is told not to
    # touch them, but we re-attach defensively at the end.
    preserve_keys = ("description", "evidence", "evidence_sql_index")
    preserved = {k: chart_config[k] for k in preserve_keys if k in chart_config}

    scrubbed = {k: v for k, v in chart_config.items() if k not in ("evidence",)}
    user_message = (
        f"# Current chart options (JSON)\n{json.dumps(scrubbed, default=str)}\n\n"
        f"# Edit instruction\n{instruction.strip()}\n\n"
        "Return the updated options object."
    )

    provider = LLMProvider(model=model, temperature=0.0)
    t0 = time.perf_counter()
    resp = provider.invoke([
        ("system", _SYSTEM_PROMPT),
        ("human", user_message),
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

    # Restore preserved aux fields.
    for k, v in preserved.items():
        patched.setdefault(k, v)

    patched["_edit_history"] = (chart_config.get("_edit_history") or []) + [{
        "instruction": instruction.strip(),
        "at": time.time(),
        "elapsed_ms": round(elapsed_ms, 2),
    }]

    return patched
