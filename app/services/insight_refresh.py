"""
Regenerate a chart's `insight` (a plain 2-3 line string) against fresh data.

Used by the template re-run endpoint: same chart, today's numbers, freshly
written insight that mimics the original's voice.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from services.llm_provider import LLMProvider

logger = logging.getLogger(__name__)


_SYSTEM = """You are refreshing the insight line of a business report chart.

You receive:
  1. The ORIGINAL insight (a short string).
  2. The FRESH pre-aggregated data the chart is now showing.
  3. The chart's title.

Produce an UPDATED insight as a PLAIN STRING — 2-3 sentences, max 3 lines:
  * Lead with the single most important finding AND a concrete number from the fresh data.
  * Add ONE comparison, ranking, or outlier callout.
  * Optional third sentence only if it adds value.

Rules:
  * Every number you write must appear in (or be derivable from) the FRESH data — never invent.
  * Round numbers to at most 2 decimal places (e.g., 2.23 not 2.3333).
  * No markdown, no bullet markers, no JSON, no headings — plain sentences.
  * Match the voice and length of the original.
  * Return ONLY the string itself, nothing else.
"""


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
) -> str:
    """Return the updated insight as a 2-3 line plain string.

    On any failure, returns the original insight string untouched so the
    re-render path always has *something* to display.
    """
    original_str = str(original_insight or "").strip()
    if not original_str:
        return original_str

    try:
        provider = LLMProvider(model=model, temperature=0.1)
        t0 = time.perf_counter()
        resp = provider.invoke([
            ("system", _SYSTEM),
            ("human",
             f"# Chart title\n{chart_title}\n\n"
             f"# Original insight\n{original_str}\n\n"
             f"# Fresh data (full, not truncated)\n{json.dumps(fresh_data, default=str)}\n\n"
             "Return the refreshed insight as a PLAIN 2-3 line string."),
        ])
        elapsed_ms = (time.perf_counter() - t0) * 1000
        content = _strip_fences(getattr(resp, "content", "")).strip()
        if not content:
            return original_str
        logger.info("insight refreshed for '%s' in %.0fms", chart_title, elapsed_ms)
        return content
    except Exception as e:
        logger.warning("refresh_insight failed for '%s': %s — keeping original", chart_title, e)
        return original_str
