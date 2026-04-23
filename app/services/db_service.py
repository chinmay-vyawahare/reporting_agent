"""
DB Service — handles all persistence to pwc_agent_utility_schema.

Writes to one table:
  • reporting_agent_queries — one row per report generation request

Design rules:
  - Every function opens and closes its own connection.
  - DB errors are logged but NEVER raised — DB failures must not block
    the agent from returning a response to the user.
"""
from __future__ import annotations

import json
import logging

import psycopg2

import config
from utils.json_safe import sanitize_for_json

logger = logging.getLogger(__name__)

_SCHEMA = "pwc_agent_utility_schema"


# ─────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────

def _conn():
    """Open a new read-write psycopg2 connection."""
    return psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        database=config.PG_DATABASE,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        connect_timeout=5,
    )


def ensure_tables() -> None:
    """
    Create all required tables in pwc_agent_utility_schema if they do not exist.
    Called once at application startup.
    """
    # Query metadata only — no `charts` JSONB. Each chart lives as its own
    # row in reporting_agent_charts so we get real columns to query against.
    create_queries = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_agent_queries (
            query_id            VARCHAR(100)    PRIMARY KEY,
            user_id             VARCHAR(100)    NOT NULL,
            thread_id           VARCHAR(100),
            original_query      TEXT            NOT NULL,
            project_type        VARCHAR(50)     NOT NULL,
            max_charts          SMALLINT        NOT NULL DEFAULT 3,
            status              VARCHAR(20)     NOT NULL DEFAULT 'running',
            rationale           TEXT,
            traversal_findings  TEXT,
            traversal_steps     SMALLINT        DEFAULT 0,
            errors              JSONB,
            started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
            completed_at        TIMESTAMP,
            duration_ms         NUMERIC(12, 2)
        );

        CREATE INDEX IF NOT EXISTS idx_queries_user_thread
            ON {_SCHEMA}.reporting_agent_queries (user_id, thread_id, started_at DESC);
    """

    # One row per generated chart. The Highcharts config (`chart_config`)
    # stays JSONB because Highcharts options are deeply nested and arbitrary,
    # but every metadata field that's worth filtering or joining on
    # (chart_id, query_id, user_id, thread_id, title, type, script,
    # description, insight) lives in its own column.
    # All TEXT fields are unbounded — nothing is truncated.
    create_charts = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_agent_charts (
            chart_id        VARCHAR(100)    PRIMARY KEY,
            query_id        VARCHAR(100)    NOT NULL,
            user_id         VARCHAR(100)    NOT NULL,
            thread_id       VARCHAR(100),
            chart_index     SMALLINT        NOT NULL DEFAULT 0,
            chart_type      VARCHAR(50),
            title           TEXT,
            description     TEXT,
            insight         TEXT,
            script          TEXT            NOT NULL,
            sql_index       SMALLINT,
            chart_config    JSONB           NOT NULL,
            created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_charts_user_created
            ON {_SCHEMA}.reporting_agent_charts (user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_charts_thread_created
            ON {_SCHEMA}.reporting_agent_charts (thread_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_charts_query
            ON {_SCHEMA}.reporting_agent_charts (query_id, chart_index);
    """

    create_threads = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_chat_threads (
            thread_id       VARCHAR(100)    PRIMARY KEY,
            user_id         VARCHAR(100)    NOT NULL,
            title           TEXT,
            project_type    VARCHAR(50),
            created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_threads_user_updated
            ON {_SCHEMA}.reporting_chat_threads (user_id, updated_at DESC);
    """

    create_chart_edits = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_chart_edits (
            edit_id         BIGSERIAL PRIMARY KEY,
            query_id        VARCHAR(100) NOT NULL,
            chart_id        VARCHAR(100) NOT NULL,
            instruction     TEXT         NOT NULL,
            patched_chart   JSONB        NOT NULL,
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_chart_edits_chart
            ON {_SCHEMA}.reporting_chart_edits (chart_id, created_at DESC);
    """

    # Canvas drafts are USER-scoped (not thread-scoped) — a draft can hold
    # charts the user gathered from any number of threads.
    create_canvas_drafts = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_canvas_drafts (
            draft_id        VARCHAR(100) PRIMARY KEY,
            user_id         VARCHAR(100) NOT NULL,
            name            TEXT         NOT NULL,
            project_type    VARCHAR(50),
            slots           JSONB        NOT NULL DEFAULT '[]'::jsonb,
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_canvas_drafts_user_updated
            ON {_SCHEMA}.reporting_canvas_drafts (user_id, updated_at DESC);
    """

    create_templates = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_templates (
            template_id       VARCHAR(100)    PRIMARY KEY,
            user_id           VARCHAR(100)    NOT NULL,
            thread_id         VARCHAR(100),
            source_draft_id   VARCHAR(100),
            title             TEXT            NOT NULL,
            project_type      VARCHAR(50),
            selections        JSONB           NOT NULL,
            last_rendered     JSONB,
            created_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
            last_run_at       TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_templates_user_created
            ON {_SCHEMA}.reporting_templates (user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_templates_source_draft
            ON {_SCHEMA}.reporting_templates (source_draft_id);
    """

    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_queries)
                cur.execute(create_charts)
                cur.execute(create_threads)
                cur.execute(create_chart_edits)
                cur.execute(create_canvas_drafts)
                cur.execute(create_templates)
        logger.info("reporting tables verified / created.")
    except Exception as exc:
        logger.error("ensure_tables failed: %s", exc)


def _exec(sql: str, params: tuple) -> None:
    """Execute a single DML statement. Logs and swallows all DB errors."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
    except Exception as exc:
        logger.error("DB write failed — %.80s | error=%s", sql, exc)


def _fetch_rows(sql: str, params: tuple) -> list[dict]:
    """Fetch all rows as a list of dicts. Returns [] on error or no results."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [desc[0] for desc in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("DB read failed — %.80s | error=%s", sql, exc)
        return []


def _fetch_row(sql: str, params: tuple) -> dict | None:
    """Fetch a single row as a dict. Returns None on error or no result."""
    rows = _fetch_rows(sql, params)
    return rows[0] if rows else None


# ─────────────────────────────────────────────
# Write operations
# ─────────────────────────────────────────────

def create_query(
    query_id: str,
    user_id: str,
    original_query: str,
    project_type: str,
    max_charts: int = 3,
    thread_id: str | None = None,
) -> None:
    """Insert a new query row with status=running at the moment of receipt."""
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_agent_queries
            (query_id, user_id, thread_id, original_query, project_type,
             max_charts, started_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'running')
        """,
        (query_id, user_id, thread_id, original_query, project_type, max_charts),
    )


_CHART_PERSIST_KEYS = {
    "chart_id", "chart", "title", "subtitle", "series", "xAxis", "yAxis",
    "plotOptions", "legend", "tooltip", "colors", "description", "insight",
    "script", "sql_index",
}


def _chart_for_storage(chart: dict) -> dict:
    """Build the slim chart object that goes into `chart_config` JSONB.

    Drops the bulky `evidence` blob (raw row dump used only for debugging),
    lifts `evidence.code` → top-level `script`, and lifts `evidence.sql_index`
    → top-level `sql_index`. Result mirrors what /report/stream emits.
    """
    out = {k: v for k, v in chart.items() if k in _CHART_PERSIST_KEYS}
    ev = chart.get("evidence")
    if isinstance(ev, dict):
        if not out.get("script"):
            out["script"] = ev.get("code", "")
        if out.get("sql_index") is None:
            out["sql_index"] = ev.get("sql_index")
    return out


def _extract_chart_meta(chart: dict, fallback_index: int) -> dict:
    """Pull queryable metadata out of a chart object so each field lands in
    its own column. Anything missing falls back to safe defaults."""
    ch = chart.get("chart") if isinstance(chart.get("chart"), dict) else {}
    title_obj = chart.get("title") if isinstance(chart.get("title"), dict) else {}
    ev = chart.get("evidence") if isinstance(chart.get("evidence"), dict) else {}
    return {
        "chart_id":    chart.get("chart_id") or "",
        "chart_index": int(chart.get("chart_index", fallback_index) or fallback_index),
        "chart_type":  (ch.get("type") if isinstance(ch, dict) else None) or chart.get("type"),
        "title":       (title_obj.get("text") if isinstance(title_obj, dict) else None)
                       or (chart.get("title") if isinstance(chart.get("title"), str) else None),
        "description": chart.get("description"),
        "insight":     chart.get("insight"),
        "script":      chart.get("script") or ev.get("code", ""),
        "sql_index":   chart.get("sql_index") or chart.get("evidence_sql_index") or ev.get("sql_index"),
    }


def save_chart(
    query_id: str,
    user_id: str,
    thread_id: str | None,
    chart: dict,
    fallback_index: int = 0,
) -> bool:
    """Insert (or upsert) one chart row keyed by chart_id.

    The full Highcharts config is preserved as `chart_config` JSONB; queryable
    metadata is extracted into proper columns. Nothing is truncated.
    """
    meta = _extract_chart_meta(chart, fallback_index)
    if not meta["chart_id"]:
        logger.warning("save_chart skipped — chart has no chart_id (query_id=%s)", query_id)
        return False
    if not meta["script"]:
        logger.warning("save_chart: chart_id=%s has no script — re-run will fail later",
                       meta["chart_id"])

    # Persist a slim chart_config — the bulky evidence-rows blob stays in
    # memory only. The re-run reads `script` straight from chart_config.
    storage_chart = _chart_for_storage(chart)
    try:
        _exec(
            f"""
            INSERT INTO {_SCHEMA}.reporting_agent_charts
                (chart_id, query_id, user_id, thread_id,
                 chart_index, chart_type, title, description, insight,
                 script, sql_index, chart_config, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (chart_id) DO UPDATE SET
                chart_type   = EXCLUDED.chart_type,
                title        = EXCLUDED.title,
                description  = EXCLUDED.description,
                insight      = EXCLUDED.insight,
                script       = EXCLUDED.script,
                sql_index    = EXCLUDED.sql_index,
                chart_config = EXCLUDED.chart_config,
                updated_at   = NOW()
            """,
            (
                meta["chart_id"], query_id, user_id, thread_id,
                meta["chart_index"], meta["chart_type"], meta["title"],
                meta["description"], meta["insight"],
                meta["script"], meta["sql_index"],
                json.dumps(sanitize_for_json(storage_chart), default=str),
            ),
        )
        return True
    except Exception as exc:
        logger.error("save_chart failed for chart_id=%s: %s", meta["chart_id"], exc)
        return False


def update_query_complete(
    query_id: str,
    charts: list[dict],
    rationale: str,
    traversal_findings: str,
    traversal_steps: int,
    duration_ms: float,
    errors: list[str] | None = None,
    evidence: list[dict] | None = None,  # accepted but no longer persisted
) -> None:
    """Finalize a completed query.

    Updates the query metadata row, then inserts one row per chart into
    reporting_agent_charts. The `evidence` parameter is accepted for caller
    compatibility but no longer stored — evidence.code now lives as `script`
    on each chart row.
    """
    del evidence  # accepted for caller compat, no longer persisted

    # 1) Update the query metadata row.
    _exec(
        f"""
        UPDATE {_SCHEMA}.reporting_agent_queries SET
            status              = 'complete',
            rationale           = %s,
            traversal_findings  = %s,
            traversal_steps     = %s,
            errors              = %s,
            completed_at        = NOW(),
            duration_ms         = %s
        WHERE query_id = %s
        """,
        (
            rationale,
            traversal_findings,
            traversal_steps,
            json.dumps(errors) if errors else None,
            duration_ms,
            query_id,
        ),
    )
    # 2) Persist each chart as its own row. user_id+thread_id come from the
    #    queries row we just updated.
    ctx = _fetch_row(
        f"SELECT user_id, thread_id FROM {_SCHEMA}.reporting_agent_queries WHERE query_id = %s",
        (query_id,),
    )
    if not ctx:
        logger.warning("update_query_complete: query_id %s not found, charts not saved", query_id)
        return
    for i, c in enumerate(charts or []):
        save_chart(
            query_id=query_id,
            user_id=ctx["user_id"],
            thread_id=ctx.get("thread_id"),
            chart=c,
            fallback_index=i,
        )


def update_query_error(
    query_id: str,
    duration_ms: float,
    errors: list[str] | None = None,
    traversal_findings: str = "",
    traversal_steps: int = 0,
) -> None:
    """Mark query as errored with the elapsed duration."""
    _exec(
        f"""
        UPDATE {_SCHEMA}.reporting_agent_queries SET
            status              = 'error',
            traversal_findings  = %s,
            traversal_steps     = %s,
            errors              = %s,
            completed_at        = NOW(),
            duration_ms         = %s
        WHERE query_id = %s
        """,
        (
            traversal_findings,
            traversal_steps,
            json.dumps(errors) if errors else None,
            duration_ms,
            query_id,
        ),
    )


# ─────────────────────────────────────────────
# Read operations
# ─────────────────────────────────────────────

def get_queries_by_user(user_id: str, limit: int = 50) -> list[dict]:
    """Return recent queries for a user, most recent first."""
    return _fetch_rows(
        f"""
        SELECT
            query_id, user_id, original_query, project_type,
            max_charts, status, rationale, traversal_findings,
            traversal_steps, errors, started_at, completed_at, duration_ms
        FROM {_SCHEMA}.reporting_agent_queries
        WHERE user_id = %s
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )


def get_query(query_id: str) -> dict | None:
    """Return a single query row by its ID."""
    return _fetch_row(
        f"""
        SELECT
            query_id, user_id, original_query, project_type,
            max_charts, status, rationale, traversal_findings,
            traversal_steps, errors, started_at, completed_at, duration_ms
        FROM {_SCHEMA}.reporting_agent_queries
        WHERE query_id = %s
        """,
        (query_id,),
    )


def get_all_queries(limit: int = 100) -> list[dict]:
    """Return recent queries across all users."""
    return _fetch_rows(
        f"""
        SELECT
            query_id, user_id, original_query, project_type,
            max_charts, status, rationale, traversal_findings,
            traversal_steps, errors, started_at, completed_at, duration_ms
        FROM {_SCHEMA}.reporting_agent_queries
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (limit,),
    )


# ─────────────────────────────────────────────
# Chat threads
# ─────────────────────────────────────────────

def ensure_thread(
    thread_id: str,
    user_id: str,
    project_type: str = "",
    title: str | None = None,
) -> None:
    """Insert a chat thread row if it does not exist, else bump updated_at."""
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_chat_threads
            (thread_id, user_id, title, project_type, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (thread_id) DO UPDATE
            SET updated_at = NOW(),
                title = COALESCE(
                    NULLIF({_SCHEMA}.reporting_chat_threads.title, ''),
                    EXCLUDED.title
                )
        """,
        (thread_id, user_id, title, project_type),
    )


def get_threads_by_user(user_id: str, limit: int = 50) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT thread_id, user_id, title, project_type,
               created_at, updated_at
        FROM {_SCHEMA}.reporting_chat_threads
        WHERE user_id = %s
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )


def get_queries_for_thread(thread_id: str, limit: int = 50) -> list[dict]:
    """Return query rows for a thread. Charts are NOT inlined here — pull
    them via `get_charts_for_query(query_id)` when you actually need them."""
    return _fetch_rows(
        f"""
        SELECT
            query_id, user_id, thread_id, original_query, project_type,
            max_charts, status, rationale,
            traversal_findings, traversal_steps, errors,
            started_at, completed_at, duration_ms
        FROM {_SCHEMA}.reporting_agent_queries
        WHERE thread_id = %s
        ORDER BY started_at ASC
        LIMIT %s
        """,
        (thread_id, limit),
    )


# ─────────────────────────────────────────────
# Report templates (finalized multi-chart reports)
# ─────────────────────────────────────────────

def create_template(
    template_id: str,
    user_id: str,
    thread_id: str | None,
    title: str,
    project_type: str,
    selections: list[dict],
    last_rendered: dict | None = None,
    source_draft_id: str | None = None,
) -> None:
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_templates
            (template_id, user_id, thread_id, title, project_type,
             source_draft_id,
             selections, last_rendered, created_at, last_run_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """,
        (
            template_id,
            user_id,
            thread_id,
            title,
            project_type,
            source_draft_id,
            json.dumps(sanitize_for_json(selections), default=str),
            json.dumps(sanitize_for_json(last_rendered), default=str) if last_rendered else None,
        ),
    )


def update_template(
    template_id: str,
    title: str | None = None,
    project_type: str | None = None,
    selections: list[dict] | None = None,
    last_rendered: dict | None = None,
    source_draft_id: str | None = None,
) -> bool:
    """Partial update — pass only the fields you want to change.

    Returns True if a row was updated (i.e. the template exists), False otherwise.
    `last_run_at` is bumped if `selections` or `last_rendered` was supplied so
    the audit trail tracks meaningful changes.
    """
    sets: list[str] = []
    params: list = []
    if title is not None:
        sets.append("title = %s")
        params.append(title)
    if project_type is not None:
        sets.append("project_type = %s")
        params.append(project_type)
    if selections is not None:
        sets.append("selections = %s::jsonb")
        params.append(json.dumps(sanitize_for_json(selections), default=str))
    if last_rendered is not None:
        sets.append("last_rendered = %s::jsonb")
        params.append(json.dumps(sanitize_for_json(last_rendered), default=str))
    if source_draft_id is not None:
        sets.append("source_draft_id = %s")
        params.append(source_draft_id)
    if selections is not None or last_rendered is not None:
        sets.append("last_run_at = NOW()")
    if not sets:
        return True  # nothing to update; treat as a no-op success
    params.append(template_id)
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {_SCHEMA}.reporting_templates "
                    f"SET {', '.join(sets)} WHERE template_id = %s",
                    tuple(params),
                )
                return cur.rowcount > 0
    except Exception as exc:
        logger.error("update_template failed: %s", exc)
        return False


def find_template_by_draft(source_draft_id: str) -> dict | None:
    """Return the most recently created template linked to a canvas draft,
    or None if none exists. Used for upsert-by-draft semantics."""
    return _fetch_row(
        f"""
        SELECT template_id, user_id, thread_id, source_draft_id,
               title, project_type, selections, last_rendered,
               created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates
        WHERE source_draft_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (source_draft_id,),
    )


def update_template_render(template_id: str, last_rendered: dict) -> None:
    _exec(
        f"""
        UPDATE {_SCHEMA}.reporting_templates
            SET last_rendered = %s, last_run_at = NOW()
            WHERE template_id = %s
        """,
        (json.dumps(sanitize_for_json(last_rendered), default=str), template_id),
    )


def get_template(template_id: str) -> dict | None:
    return _fetch_row(
        f"""
        SELECT template_id, user_id, thread_id, title, project_type,
               selections, last_rendered, created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates
        WHERE template_id = %s
        """,
        (template_id,),
    )


def get_templates_by_user(user_id: str, limit: int = 50) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT template_id, user_id, thread_id, title, project_type,
               selections, last_rendered, created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )


def propagate_chart_content(chart_id: str, patched_chart: dict) -> dict[str, int]:
    """When a chart is edited, write the new chart content into every canvas
    draft and every template that carries a slot/selection whose
    `chart.chart_id` matches. Layout fields stay intact — only the `chart`
    payload is replaced.

    Returns {"drafts": n_drafts, "templates": n_templates}.
    """
    counts = {"drafts": 0, "templates": 0}
    if patched_chart is None or not chart_id:
        return counts

    try:
        chart_json = json.dumps(sanitize_for_json(patched_chart), default=str)
    except Exception as exc:
        logger.error("propagate_chart_content: chart JSON dump failed: %s", exc)
        return counts

    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                # Drafts: load any whose slots JSONB mentions this chart_id
                cur.execute(
                    f"""
                    SELECT draft_id, slots
                    FROM {_SCHEMA}.reporting_canvas_drafts
                    WHERE slots::text LIKE %s
                    """,
                    (f'%"chart_id": "{chart_id}"%',),
                )
                for did, slots in list(cur.fetchall()):
                    if isinstance(slots, str):
                        try:
                            slots = json.loads(slots)
                        except Exception:
                            continue
                    changed = False
                    for s in (slots or []):
                        ch = s.get("chart") if isinstance(s, dict) else None
                        if isinstance(ch, dict) and ch.get("chart_id") == chart_id:
                            s["chart"] = json.loads(chart_json)
                            changed = True
                    if changed:
                        cur.execute(
                            f"""
                            UPDATE {_SCHEMA}.reporting_canvas_drafts
                               SET slots = %s::jsonb, updated_at = NOW()
                             WHERE draft_id = %s
                            """,
                            (json.dumps(sanitize_for_json(slots), default=str), did),
                        )
                        counts["drafts"] += 1

                # Templates: same pattern against selections
                cur.execute(
                    f"""
                    SELECT template_id, selections
                    FROM {_SCHEMA}.reporting_templates
                    WHERE selections::text LIKE %s
                    """,
                    (f'%"chart_id": "{chart_id}"%',),
                )
                for tid, sels in list(cur.fetchall()):
                    if isinstance(sels, str):
                        try:
                            sels = json.loads(sels)
                        except Exception:
                            continue
                    changed = False
                    for s in (sels or []):
                        ch = s.get("chart") if isinstance(s, dict) else None
                        if isinstance(ch, dict) and ch.get("chart_id") == chart_id:
                            s["chart"] = json.loads(chart_json)
                            changed = True
                    if changed:
                        cur.execute(
                            f"""
                            UPDATE {_SCHEMA}.reporting_templates
                               SET selections = %s::jsonb
                             WHERE template_id = %s
                            """,
                            (json.dumps(sanitize_for_json(sels), default=str), tid),
                        )
                        counts["templates"] += 1
    except Exception as exc:
        logger.error("propagate_chart_content failed: %s", exc)

    return counts


def sync_draft_to_template(draft_id: str, slots: list[dict]) -> int:
    """Live-mirror the canvas draft into any template linked to it.

    The template's `selections` are overwritten with the draft's `slots`,
    1:1. This handles every kind of canvas change in one go:
      * chart added or removed   → selection added/removed
      * chart moved or resized   → x/y/w/h carried over
      * chart edited via NL       → updated chart object carried over

    Returns the number of templates that were actually updated.
    """
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT template_id
                    FROM {_SCHEMA}.reporting_templates
                    WHERE source_draft_id = %s
                    """,
                    (draft_id,),
                )
                template_ids = [r[0] for r in cur.fetchall()]
                if not template_ids:
                    return 0

                payload = json.dumps(sanitize_for_json(slots or []), default=str)
                updated = 0
                for tid in template_ids:
                    cur.execute(
                        f"""
                        UPDATE {_SCHEMA}.reporting_templates
                           SET selections = %s::jsonb,
                               last_run_at = NOW()
                         WHERE template_id = %s
                        """,
                        (payload, tid),
                    )
                    updated += cur.rowcount
                return updated
    except Exception as exc:
        logger.error("sync_draft_to_template failed: %s", exc)
        return 0


def delete_template(template_id: str) -> bool:
    """Delete a template. Returns True if a row was deleted."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {_SCHEMA}.reporting_templates WHERE template_id = %s",
                    (template_id,),
                )
                return cur.rowcount > 0
    except Exception as exc:
        logger.error("delete_template failed: %s", exc)
        return False


# ─────────────────────────────────────────────
# Chart reads (one row per chart in reporting_agent_charts)
# ─────────────────────────────────────────────

_CHART_COLS = (
    "chart_id, query_id, user_id, thread_id, chart_index, chart_type, "
    "title, description, insight, script, sql_index, chart_config, "
    "created_at, updated_at"
)


def _hydrate_chart_row(row: dict) -> dict:
    """Inline `chart_config` JSONB into the row's `chart` field so callers
    get the same shape /report/stream emits."""
    cfg = row.get("chart_config")
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = None
    row["chart"] = cfg if isinstance(cfg, dict) else {}
    return row


def get_chart(chart_id: str) -> dict | None:
    """Return one chart by id (with `chart_config` inlined as `chart`)."""
    row = _fetch_row(
        f"SELECT {_CHART_COLS} FROM {_SCHEMA}.reporting_agent_charts WHERE chart_id = %s",
        (chart_id,),
    )
    return _hydrate_chart_row(row) if row else None


def get_charts_for_query(query_id: str) -> list[dict]:
    """Return every chart belonging to a query, ordered by chart_index."""
    rows = _fetch_rows(
        f"""
        SELECT {_CHART_COLS}
        FROM {_SCHEMA}.reporting_agent_charts
        WHERE query_id = %s
        ORDER BY chart_index ASC
        """,
        (query_id,),
    )
    return [_hydrate_chart_row(r) for r in rows]


def get_charts_by_user(user_id: str, limit: int = 100) -> list[dict]:
    """Return the most recent charts for a user, newest first."""
    rows = _fetch_rows(
        f"""
        SELECT {_CHART_COLS}
        FROM {_SCHEMA}.reporting_agent_charts
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )
    return [_hydrate_chart_row(r) for r in rows]


def get_charts_by_thread(thread_id: str, limit: int = 100) -> list[dict]:
    """Return the most recent charts in a chat thread, newest first."""
    rows = _fetch_rows(
        f"""
        SELECT {_CHART_COLS}
        FROM {_SCHEMA}.reporting_agent_charts
        WHERE thread_id = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (thread_id, limit),
    )
    return [_hydrate_chart_row(r) for r in rows]


# ─────────────────────────────────────────────
# Chart edits (per-chart natural-language patches)
# ─────────────────────────────────────────────

def update_chart_by_id(chart_id: str, patched_chart: dict) -> bool:
    """Replace the saved chart's slim chart_config + extracted metadata.
    Returns True if a row was updated.
    """
    meta = _extract_chart_meta(patched_chart, fallback_index=0)
    storage_chart = _chart_for_storage(patched_chart)
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {_SCHEMA}.reporting_agent_charts SET
                        chart_type   = %s,
                        title        = %s,
                        description  = %s,
                        insight      = %s,
                        script       = COALESCE(NULLIF(%s, ''), script),
                        sql_index    = COALESCE(%s, sql_index),
                        chart_config = %s::jsonb,
                        updated_at   = NOW()
                    WHERE chart_id = %s
                    """,
                    (
                        meta["chart_type"], meta["title"],
                        meta["description"], meta["insight"],
                        meta["script"], meta["sql_index"],
                        json.dumps(sanitize_for_json(storage_chart), default=str),
                        chart_id,
                    ),
                )
                return cur.rowcount > 0
    except Exception as exc:
        logger.error("update_chart_by_id failed: %s", exc)
        return False


def log_chart_edit(query_id: str, chart_id: str, instruction: str, patched_chart: dict) -> None:
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_chart_edits
            (query_id, chart_id, instruction, patched_chart, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        """,
        (query_id, chart_id, instruction, json.dumps(patched_chart, default=str)),
    )


def get_chart_edit_history(chart_id: str, limit: int = 20) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT edit_id, query_id, chart_id, instruction, patched_chart, created_at
        FROM {_SCHEMA}.reporting_chart_edits
        WHERE chart_id = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (chart_id, limit),
    )


# ─────────────────────────────────────────────
# Canvas drafts (persisted multi-chart work-in-progress)
# ─────────────────────────────────────────────

def create_canvas_draft(
    draft_id: str,
    user_id: str,
    name: str,
    project_type: str = "",
) -> None:
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_canvas_drafts
            (draft_id, user_id, name, project_type, slots, created_at, updated_at)
        VALUES (%s, %s, %s, %s, '[]'::jsonb, NOW(), NOW())
        """,
        (draft_id, user_id, name, project_type),
    )


def update_canvas_draft(
    draft_id: str,
    name: str | None = None,
    slots: list[dict] | None = None,
) -> None:
    """Replace name, slots, or both for a draft. Unspecified fields are kept."""
    sets: list[str] = []
    params: list = []
    if name is not None:
        sets.append("name = %s")
        params.append(name)
    if slots is not None:
        sets.append("slots = %s::jsonb")
        params.append(json.dumps(sanitize_for_json(slots), default=str))
    if not sets:
        return
    sets.append("updated_at = NOW()")
    params.append(draft_id)
    _exec(
        f"""
        UPDATE {_SCHEMA}.reporting_canvas_drafts
           SET {', '.join(sets)}
         WHERE draft_id = %s
        """,
        tuple(params),
    )


def delete_canvas_draft(draft_id: str) -> bool:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {_SCHEMA}.reporting_canvas_drafts WHERE draft_id = %s",
                    (draft_id,),
                )
                return cur.rowcount > 0
    except Exception as exc:
        logger.error("delete_canvas_draft failed: %s", exc)
        return False


def get_canvas_draft(draft_id: str) -> dict | None:
    return _fetch_row(
        f"""
        SELECT draft_id, user_id, name, project_type,
               slots, created_at, updated_at
        FROM {_SCHEMA}.reporting_canvas_drafts
        WHERE draft_id = %s
        """,
        (draft_id,),
    )


def list_canvas_drafts(user_id: str, limit: int = 50) -> list[dict]:
    """All canvas drafts for a user (canvas is user-scoped, not thread-scoped)."""
    return _fetch_rows(
        f"""
        SELECT draft_id, user_id, name, project_type,
               slots, created_at, updated_at
        FROM {_SCHEMA}.reporting_canvas_drafts
        WHERE user_id = %s
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )
