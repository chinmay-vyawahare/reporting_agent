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
import uuid

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
    # Base table for queries — kept minimal so it succeeds on fresh installs.
    create_queries = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_agent_queries (
            query_id            VARCHAR(100)    PRIMARY KEY,
            user_id             VARCHAR(100)    NOT NULL,
            username            VARCHAR(255)    NOT NULL,
            original_query      TEXT            NOT NULL,
            project_type        VARCHAR(50)     NOT NULL,
            max_charts          SMALLINT        NOT NULL DEFAULT 3,
            status              VARCHAR(20)     NOT NULL DEFAULT 'running',
            charts              JSONB,
            rationale           TEXT,
            traversal_findings  TEXT,
            traversal_steps     SMALLINT        DEFAULT 0,
            errors              JSONB,
            started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
            completed_at        TIMESTAMP,
            duration_ms         NUMERIC(12, 2)
        );
    """

    # Add any new columns on top of the base (idempotent).
    migration_ddl = f"""
        ALTER TABLE {_SCHEMA}.reporting_agent_queries
            ADD COLUMN IF NOT EXISTS thread_id VARCHAR(100);
        ALTER TABLE {_SCHEMA}.reporting_agent_queries
            ADD COLUMN IF NOT EXISTS evidence JSONB;

        CREATE INDEX IF NOT EXISTS idx_queries_user_thread
            ON {_SCHEMA}.reporting_agent_queries (user_id, thread_id, started_at DESC);
    """

    # Template linkage to source draft (for live layout propagation).
    template_migration_ddl = f"""
        ALTER TABLE {_SCHEMA}.reporting_templates
            ADD COLUMN IF NOT EXISTS source_draft_id VARCHAR(100);

        CREATE INDEX IF NOT EXISTS idx_templates_source_draft
            ON {_SCHEMA}.reporting_templates (source_draft_id);
    """

    # New tables.
    create_threads = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_chat_threads (
            thread_id       VARCHAR(100)    PRIMARY KEY,
            user_id         VARCHAR(100)    NOT NULL,
            username        VARCHAR(255)    NOT NULL,
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
            chart_index     SMALLINT     NOT NULL,
            instruction     TEXT         NOT NULL,
            patched_chart   JSONB        NOT NULL,
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_chart_edits_query
            ON {_SCHEMA}.reporting_chart_edits (query_id, chart_index, created_at DESC);
    """

    create_canvas_drafts = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_canvas_drafts (
            draft_id        VARCHAR(100) PRIMARY KEY,
            user_id         VARCHAR(100) NOT NULL,
            username        VARCHAR(255) NOT NULL,
            thread_id       VARCHAR(100),
            name            TEXT         NOT NULL,
            project_type    VARCHAR(50),
            slots           JSONB        NOT NULL DEFAULT '[]'::jsonb,
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_canvas_drafts_user_thread
            ON {_SCHEMA}.reporting_canvas_drafts (user_id, thread_id, updated_at DESC);
    """

    create_templates = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_templates (
            template_id       VARCHAR(100)    PRIMARY KEY,
            user_id           VARCHAR(100)    NOT NULL,
            username          VARCHAR(255)    NOT NULL,
            thread_id         VARCHAR(100),
            title             TEXT            NOT NULL,
            project_type      VARCHAR(50),
            selections        JSONB           NOT NULL,
            last_rendered     JSONB,
            created_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
            last_run_at       TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_templates_user_created
            ON {_SCHEMA}.reporting_templates (user_id, created_at DESC);
    """

    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_queries)
                cur.execute(migration_ddl)
                cur.execute(create_threads)
                cur.execute(create_chart_edits)
                cur.execute(create_canvas_drafts)
                cur.execute(create_templates)
                cur.execute(template_migration_ddl)
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
    username: str,
    original_query: str,
    project_type: str,
    max_charts: int = 3,
    thread_id: str | None = None,
) -> None:
    """Insert a new query row with status=running at the moment of receipt."""
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_agent_queries
            (query_id, user_id, username, thread_id, original_query, project_type,
             max_charts, started_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), 'running')
        """,
        (query_id, user_id, username, thread_id, original_query, project_type, max_charts),
    )


def update_query_complete(
    query_id: str,
    charts: list[dict],
    rationale: str,
    traversal_findings: str,
    traversal_steps: int,
    duration_ms: float,
    errors: list[str] | None = None,
    evidence: list[dict] | None = None,
) -> None:
    """Finalize a completed query with chart results."""
    _exec(
        f"""
        UPDATE {_SCHEMA}.reporting_agent_queries SET
            status              = 'complete',
            charts              = %s,
            rationale           = %s,
            evidence            = %s,
            traversal_findings  = %s,
            traversal_steps     = %s,
            errors              = %s,
            completed_at        = NOW(),
            duration_ms         = %s
        WHERE query_id = %s
        """,
        (
            json.dumps(sanitize_for_json(charts), default=str),
            rationale,
            # Always store evidence as a JSON array (never NULL) — the UI's
            # rehydration + downstream re-run both read `evidence[*].code`.
            json.dumps(sanitize_for_json(evidence or []), default=str),
            traversal_findings,
            traversal_steps,
            json.dumps(errors) if errors else None,
            duration_ms,
            query_id,
        ),
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
            query_id, user_id, username, original_query, project_type,
            max_charts, status, charts, rationale, traversal_findings,
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
            query_id, user_id, username, original_query, project_type,
            max_charts, status, charts, rationale, traversal_findings,
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
            query_id, user_id, username, original_query, project_type,
            max_charts, status, charts, rationale, traversal_findings,
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
    username: str,
    project_type: str = "",
    title: str | None = None,
) -> None:
    """Insert a chat thread row if it does not exist, else bump updated_at."""
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_chat_threads
            (thread_id, user_id, username, title, project_type, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (thread_id) DO UPDATE
            SET updated_at = NOW(),
                title = COALESCE(
                    NULLIF({_SCHEMA}.reporting_chat_threads.title, ''),
                    EXCLUDED.title
                )
        """,
        (thread_id, user_id, username, title, project_type),
    )


def get_threads_by_user(user_id: str, limit: int = 50) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT thread_id, user_id, username, title, project_type,
               created_at, updated_at
        FROM {_SCHEMA}.reporting_chat_threads
        WHERE user_id = %s
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )


def get_queries_for_thread(thread_id: str, limit: int = 50) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT
            query_id, user_id, username, thread_id, original_query, project_type,
            max_charts, status, charts, rationale, evidence,
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
    username: str,
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
            (template_id, user_id, username, thread_id, title, project_type,
             source_draft_id,
             selections, last_rendered, created_at, last_run_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """,
        (
            template_id,
            user_id,
            username,
            thread_id,
            title,
            project_type,
            source_draft_id,
            json.dumps(sanitize_for_json(selections), default=str),
            json.dumps(sanitize_for_json(last_rendered), default=str) if last_rendered else None,
        ),
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
        SELECT template_id, user_id, username, thread_id, title, project_type,
               selections, last_rendered, created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates
        WHERE template_id = %s
        """,
        (template_id,),
    )


def get_templates_by_user(user_id: str, limit: int = 50) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT template_id, user_id, username, thread_id, title, project_type,
               selections, last_rendered, created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )


def propagate_draft_layout(draft_id: str, slots: list[dict]) -> int:
    """Update the layout (x, y, w, h) on every template linked to this draft
    so moves on the canvas flow through to the saved template without a
    re-finalize.

    Matching rule: per template, for each selection whose
    (query_id, chart_index) equals a slot's, copy x/y/w/h over.
    `position` is re-derived from (y, x) so list-view order stays consistent.

    Unlinked selections are left untouched. Returns the number of templates
    that were actually updated.
    """
    try:
        # Build a lookup from (query_id, chart_index) → layout
        layout_by_key: dict[tuple[str, int], dict[str, int]] = {}
        for s in slots or []:
            qid = s.get("query_id")
            cidx = s.get("chart_index")
            if qid is None or cidx is None:
                continue
            layout_by_key[(str(qid), int(cidx))] = {
                "x": int(s.get("x", 0) or 0),
                "y": int(s.get("y", 0) or 0),
                "w": int(s.get("w", 6) or 6),
                "h": int(s.get("h", 4) or 4),
            }

        if not layout_by_key:
            return 0

        updated = 0
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT template_id, selections, last_rendered
                    FROM {_SCHEMA}.reporting_templates
                    WHERE source_draft_id = %s
                    """,
                    (draft_id,),
                )
                rows = cur.fetchall()
                for tid, sels, last_rendered in rows:
                    if isinstance(sels, str):
                        try:
                            sels = json.loads(sels)
                        except Exception:
                            continue
                    if not isinstance(sels, list):
                        continue

                    changed = False
                    for sel in sels:
                        key = (str(sel.get("query_id")), int(sel.get("chart_index")))
                        layout = layout_by_key.get(key)
                        if layout is None:
                            continue
                        for k in ("x", "y", "w", "h"):
                            if sel.get(k) != layout[k]:
                                sel[k] = layout[k]
                                changed = True

                    if not changed:
                        continue

                    # Re-derive position from 2D order (top-to-bottom, left-to-right)
                    sels.sort(key=lambda s: (int(s.get("y", 0)), int(s.get("x", 0))))
                    for i, s in enumerate(sels):
                        s["position"] = i

                    # Also propagate into last_rendered.charts if it exists, so
                    # the Template View tab picks up the new layout without a
                    # re-run. We only mutate x/y/w/h style keys on the chart
                    # dict if they happen to be stored there; most of the
                    # layout lives in `selections`.
                    cur.execute(
                        f"""
                        UPDATE {_SCHEMA}.reporting_templates
                           SET selections = %s::jsonb
                         WHERE template_id = %s
                        """,
                        (json.dumps(sanitize_for_json(sels), default=str), tid),
                    )
                    updated += 1
        return updated
    except Exception as exc:
        logger.error("propagate_draft_layout failed: %s", exc)
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
# Chart edits (per-chart natural-language patches)
# ─────────────────────────────────────────────

def get_query_charts(query_id: str) -> list[dict] | None:
    """Return the current `charts` JSON array for a query, or None if missing."""
    row = _fetch_row(
        f"SELECT charts FROM {_SCHEMA}.reporting_agent_queries WHERE query_id = %s",
        (query_id,),
    )
    if not row:
        return None
    charts = row.get("charts")
    if isinstance(charts, str):
        try:
            charts = json.loads(charts)
        except Exception:
            return None
    return charts if isinstance(charts, list) else None


def update_query_chart_at(query_id: str, chart_index: int, patched_chart: dict) -> bool:
    """
    Overwrite one chart inside reporting_agent_queries.charts[chart_index].
    Returns True on success, False otherwise.
    """
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {_SCHEMA}.reporting_agent_queries
                       SET charts = jsonb_set(COALESCE(charts, '[]'::jsonb),
                                              %s,
                                              %s::jsonb,
                                              false)
                     WHERE query_id = %s
                    """,
                    (
                        "{" + str(int(chart_index)) + "}",
                        json.dumps(patched_chart, default=str),
                        query_id,
                    ),
                )
        return True
    except Exception as exc:
        logger.error("update_query_chart_at failed: %s", exc)
        return False


def log_chart_edit(query_id: str, chart_index: int, instruction: str, patched_chart: dict) -> None:
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_chart_edits
            (query_id, chart_index, instruction, patched_chart, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        """,
        (query_id, chart_index, instruction, json.dumps(patched_chart, default=str)),
    )


def get_chart_edit_history(query_id: str, chart_index: int, limit: int = 20) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT edit_id, query_id, chart_index, instruction, patched_chart, created_at
        FROM {_SCHEMA}.reporting_chart_edits
        WHERE query_id = %s AND chart_index = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (query_id, chart_index, limit),
    )


# ─────────────────────────────────────────────
# Canvas drafts (persisted multi-chart work-in-progress)
# ─────────────────────────────────────────────

def create_canvas_draft(
    draft_id: str,
    user_id: str,
    username: str,
    thread_id: str | None,
    name: str,
    project_type: str = "",
) -> None:
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_canvas_drafts
            (draft_id, user_id, username, thread_id, name, project_type, slots, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, '[]'::jsonb, NOW(), NOW())
        """,
        (draft_id, user_id, username, thread_id, name, project_type),
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
        SELECT draft_id, user_id, username, thread_id, name, project_type,
               slots, created_at, updated_at
        FROM {_SCHEMA}.reporting_canvas_drafts
        WHERE draft_id = %s
        """,
        (draft_id,),
    )


def list_canvas_drafts(
    user_id: str,
    thread_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    if thread_id:
        return _fetch_rows(
            f"""
            SELECT draft_id, user_id, username, thread_id, name, project_type,
                   slots, created_at, updated_at
            FROM {_SCHEMA}.reporting_canvas_drafts
            WHERE user_id = %s AND thread_id = %s
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (user_id, thread_id, limit),
        )
    return _fetch_rows(
        f"""
        SELECT draft_id, user_id, username, thread_id, name, project_type,
               slots, created_at, updated_at
        FROM {_SCHEMA}.reporting_canvas_drafts
        WHERE user_id = %s
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (user_id, limit),
    )
