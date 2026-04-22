"""
Regenerate a chart's `insight` block against fresh data, preserving the same
structured shape (headline + what_the_data_shows + why_it_matters + next_step).

Used by the template re-run endpoint.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from services.llm_provider import LLMProvider

logger = logging.getLogger(__name__)


_SYSTEM = """You are refreshing the insight block of a business report chart.

You receive:
  1. The ORIGINAL insight — either a structured object or a markdown string.
  2. The FRESH pre-aggregated data the chart is now showing.
  3. The chart's title.

Produce an UPDATED insight as a JSON object with EXACTLY these keys:
  - "headline"                 : one string, one line, must contain a concrete number
  - "what_the_data_shows"      : array of 2-4 short strings (plain sentences, no leading "- ")
  - "why_it_matters"           : array of 1-3 short strings
  - "recommended_next_step"    : one short sentence

Rules:
  * Every bullet must reference a CONCRETE NUMBER taken from the fresh data.
  * Do NOT invent values that are not in the fresh data.
  * If a finding from the original insight is no longer true, replace that bullet.
  * Match the voice and length of the original; match the number of bullets per
    section to the original when possible.
  * Return ONLY the JSON object — no markdown fences, no commentary.
"""


def _coerce_structured(x: Any) -> dict | None:
    """Return a dict-shaped insight if possible; else None."""
    if isinstance(x, dict) and "headline" in x:
        return x
    if isinstance(x, str):
        return {
            "headline": "",
            "what_the_data_shows": [],
            "why_it_matters": [],
            "recommended_next_step": "",
            "_legacy_markdown": x,
        }
    return None


def _strip_fences(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        nl = text.find("\n")
        if nl >= 0:
            text = text[nl + 1:]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
    return text.strip()


def refresh_insight(
    chart_title: str,
    original_insight: Any,
    fresh_data: Any,
    model: str = "gpt-4o",
) -> Any:
    """
    Return the updated insight (same shape as the original).

    If the original was a structured object, returns an object.
    If the original was a plain markdown string, returns a structured object
    (the UI renders both forms).
    On any failure, returns `original_insight` untouched.
    """
    if not original_insight:
        return original_insight

    try:
        provider = LLMProvider(model=model, temperature=0.1)
        t0 = time.perf_counter()
        resp = provider.invoke([
            ("system", _SYSTEM),
            ("human",
             f"# Chart title\n{chart_title}\n\n"
             f"# Original insight\n{json.dumps(original_insight, default=str)}\n\n"
             f"# Fresh data (full, not truncated)\n{json.dumps(fresh_data, default=str)}\n\n"
             "Return the refreshed insight as a JSON object with the four keys."),
        ])
        elapsed_ms = (time.perf_counter() - t0) * 1000
        content = _strip_fences(getattr(resp, "content", ""))
        if not content:
            return original_insight

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            logger.warning("refresh_insight returned non-JSON; keeping original.")
            return original_insight

        if not isinstance(parsed, dict) or "headline" not in parsed:
            logger.warning("refresh_insight shape missing 'headline'; keeping original.")
            return original_insight

        logger.info("insight refreshed for '%s' in %.0fms", chart_title, elapsed_ms)
        return parsed
    except Exception as e:
        logger.warning("refresh_insight failed for '%s': %s — keeping original", chart_title, e)
        return original_insight
