"""
Thread memory — summarise the last N (Q, A) turns in a chat thread so the
next query lands with conversation context.

Each "turn" = one /report/stream call:
  Q  the analyst's natural-language question (queries.original_query)
  A  the chart titles + 2-3 line insights produced for that question
     (joined from reporting_agent_charts).

A small LLM call distils the last N turns into a 3-5 line prose summary that
gets injected into the traversal agent's system prompt as conversation
context. Without this, every query starts blind — the agent has no idea what
the analyst was just exploring.

Cached per (thread_id, latest_query_started_at) so repeated calls within the
same thread state are free.
"""
from __future__ import annotations

import logging
import time

from services import db_service
from services.llm_provider import LLMProvider

logger = logging.getLogger(__name__)

DEFAULT_HISTORY_TURNS = 5
SUMMARY_MODEL = "gpt-4o-mini"  # tiny + cheap; this is just summarisation

# Cache key: (thread_id, latest_started_at_iso) → summary string.
# Bumps automatically the moment a new query lands in the thread.
_SUMMARY_CACHE: dict[tuple[str, str], str] = {}


_SYSTEM = """You summarise a chat thread between a business analyst and a reporting agent.

Each turn is one Q (the analyst's question) followed by A (chart titles and 2-3 line insights produced).

Produce a tight 3-5 line summary covering:
  * What the analyst has been investigating (the recurring intent / domain).
  * Key findings already surfaced (specific numbers when present in the turns).
  * What's likely next, based on the trajectory of questions.

Rules:
  * Plain prose, 3-5 short sentences total.
  * No bullets, no markdown, no headings.
  * Never invent numbers — only reuse what's in the turns provided.
  * Return ONLY the summary, with no preamble.
"""


def _format_turns_as_transcript(queries: list[dict]) -> str:
    """Render queries + their charts as a Q/A transcript for the summariser."""
    lines: list[str] = []
    for i, q in enumerate(queries, 1):
        question = (q.get("original_query") or "").strip()
        if not question:
            continue
        lines.append(f"Q{i}: {question}")
        charts = db_service.get_charts_for_query(q["query_id"])
        if not charts:
            lines.append(f"A{i}: (no charts returned)")
        else:
            for c in charts:
                title = (c.get("title") or "(untitled)").strip()
                insight = (c.get("insight") or "").strip()
                if insight:
                    lines.append(f"A{i}: [{title}] {insight}")
                else:
                    lines.append(f"A{i}: [{title}]")
        lines.append("")
    return "\n".join(lines).strip()


def get_thread_context_summary(
    thread_id: str | None,
    exclude_query_id: str | None = None,
    limit: int = DEFAULT_HISTORY_TURNS,
) -> str:
    """Return a 3-5 line summary of the last `limit` turns in the thread.

    Returns "" if there is no thread_id, no prior history, or summarisation
    fails (the agent should still work — context is a boost, not a requirement).

    `exclude_query_id` keeps the in-flight query from being summarised against
    itself when the caller has already inserted the query row before invoking
    the pipeline.
    """
    if not thread_id:
        return ""

    # Most recent N+1 turns so we can drop the in-flight one and still keep N.
    queries = db_service.get_queries_for_thread(thread_id, limit=limit + 1)
    if exclude_query_id:
        queries = [q for q in queries if q.get("query_id") != exclude_query_id]
    queries = queries[-limit:]
    if not queries:
        return ""

    cache_key = (thread_id, str(queries[-1].get("started_at", "")))
    if cache_key in _SUMMARY_CACHE:
        return _SUMMARY_CACHE[cache_key]

    transcript = _format_turns_as_transcript(queries)
    if not transcript:
        return ""

    try:
        provider = LLMProvider(model=SUMMARY_MODEL, temperature=0.1)
        t0 = time.perf_counter()
        resp = provider.invoke([
            ("system", _SYSTEM),
            ("human", f"# Thread transcript ({len(queries)} turns, oldest first)\n\n"
                      f"{transcript}\n\nWrite the summary."),
        ])
        elapsed_ms = (time.perf_counter() - t0) * 1000
        summary = (getattr(resp, "content", "") or "").strip()
        # Tolerate stray code fences from the LLM.
        if summary.startswith("```"):
            nl = summary.find("\n")
            if nl >= 0:
                summary = summary[nl + 1:]
            if summary.rstrip().endswith("```"):
                summary = summary.rstrip()[:-3]
            summary = summary.strip()
        if not summary:
            return ""
        logger.info("thread summary generated in %.0fms (thread=%s, %d turns)",
                    elapsed_ms, thread_id, len(queries))
        _SUMMARY_CACHE[cache_key] = summary
        return summary
    except Exception as e:
        logger.warning("thread summary failed for %s: %s", thread_id, e)
        return ""


def render_for_prompt(summary: str) -> str:
    """Wrap a summary string for direct injection into a system prompt.

    Returns "" when summary is empty so the prompt slot stays clean — no
    "Conversation context: (none)" filler.
    """
    s = (summary or "").strip()
    if not s:
        return ""
    return (
        "# Conversation context (last few turns in this thread)\n"
        f"{s}\n\n"
        "Use this context only as background — answer the current question, "
        "but lean on prior findings (e.g. region focus, time window, prior "
        "definitions) when the new question is ambiguous.\n"
    )
