"""
DB Service — handles all persistence to pwc_agent_utility_schema.

Connections are checked out from a shared `ThreadedConnectionPool` (see
`services.db_pool`) — under load this is 50-200ms faster per call than the
old per-call `psycopg2.connect()`.

Design rules:
  - Every function uses `with _conn() as conn:` — the context manager commits
    on clean exit, rolls back on exception, and always returns the connection
    to the pool.
  - DB errors are logged but NEVER raised — DB failures must not block
    the agent from returning a response to the user.
"""
from __future__ import annotations

import logging

from services.db_pool import get_conn as _conn  # noqa: F401 — re-exported

logger = logging.getLogger(__name__)

_SCHEMA = "pwc_agent_utility_schema"


def ensure_tables() -> None:
    """Create all reporting tables in pwc_agent_utility_schema. Zero JSONB.

    Schema:
      reporting_agent_queries           query metadata
      reporting_agent_charts            decomposed Highcharts metadata (no JSONB)
      reporting_chart_cartesian_series  one row per series for cartesian charts
      reporting_chart_pie_slices        one row per slice for pie charts
      reporting_chart_edits             audit log (no patched-chart blob)
      reporting_canvas_drafts           draft metadata
      reporting_canvas_slots            one row per slot
      reporting_templates               template metadata
      reporting_template_selections     one row per selection
      reporting_chat_threads            chat threads
    """
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
            errors              TEXT,
            traversal_findings  TEXT,
            traversal_steps     SMALLINT        DEFAULT 0,
            started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
            completed_at        TIMESTAMP,
            duration_ms         NUMERIC(12, 2)
        );

        CREATE INDEX IF NOT EXISTS idx_queries_user_thread
            ON {_SCHEMA}.reporting_agent_queries (user_id, thread_id, started_at DESC);
    """

    # Each Highcharts chart is decomposed into proper columns. `colors` and
    # `x_axis_categories` use Postgres array types (TEXT[]) — these are real
    # relational types, not JSONB. Per-type variable parts (cartesian series
    # data, pie slices) live in dedicated child tables.
    create_charts = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_agent_charts (
            chart_id              VARCHAR(100) PRIMARY KEY,
            query_id              VARCHAR(100) NOT NULL,
            user_id               VARCHAR(100) NOT NULL,
            thread_id             VARCHAR(100),
            chart_index           SMALLINT     NOT NULL DEFAULT 0,
            chart_type            VARCHAR(50)  NOT NULL,

            title_text            TEXT,
            subtitle_text         TEXT,
            description           TEXT,
            insight               TEXT,
            script                TEXT         NOT NULL,
            sql_index             SMALLINT,

            colors                TEXT[]       NOT NULL DEFAULT ARRAY[]::TEXT[],
            x_axis_title          TEXT,
            x_axis_categories     TEXT[],
            y_axis_title          TEXT,
            tooltip_value_suffix  TEXT,
            data_labels_enabled   BOOLEAN      NOT NULL DEFAULT TRUE,
            legend_enabled        BOOLEAN      NOT NULL DEFAULT TRUE,

            created_at            TIMESTAMP    NOT NULL DEFAULT NOW(),
            updated_at            TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_charts_user_created
            ON {_SCHEMA}.reporting_agent_charts (user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_charts_thread_created
            ON {_SCHEMA}.reporting_agent_charts (thread_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_charts_query
            ON {_SCHEMA}.reporting_agent_charts (query_id, chart_index);
    """

    create_cart_series = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_chart_cartesian_series (
            series_id   BIGSERIAL    PRIMARY KEY,
            chart_id    VARCHAR(100) NOT NULL REFERENCES {_SCHEMA}.reporting_agent_charts(chart_id) ON DELETE CASCADE,
            position    SMALLINT     NOT NULL,
            name        TEXT         NOT NULL,
            color       TEXT,
            data        NUMERIC[]    NOT NULL DEFAULT ARRAY[]::NUMERIC[]
        );

        CREATE INDEX IF NOT EXISTS idx_cart_series_chart
            ON {_SCHEMA}.reporting_chart_cartesian_series (chart_id, position);
    """

    create_pie_slices = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_chart_pie_slices (
            slice_id    BIGSERIAL    PRIMARY KEY,
            chart_id    VARCHAR(100) NOT NULL REFERENCES {_SCHEMA}.reporting_agent_charts(chart_id) ON DELETE CASCADE,
            position    SMALLINT     NOT NULL,
            name        TEXT         NOT NULL,
            y           NUMERIC      NOT NULL,
            color       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_pie_slices_chart
            ON {_SCHEMA}.reporting_chart_pie_slices (chart_id, position);
    """

    create_threads = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_chat_threads (
            thread_id       VARCHAR(100) PRIMARY KEY,
            user_id         VARCHAR(100) NOT NULL,
            title           TEXT,
            project_type    VARCHAR(50),
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_threads_user_updated
            ON {_SCHEMA}.reporting_chat_threads (user_id, updated_at DESC);
    """

    # Audit log: just instruction + chart_id + when. The current chart state
    # is always in reporting_agent_charts; we don't need a per-edit snapshot.
    create_chart_edits = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_chart_edits (
            edit_id         BIGSERIAL    PRIMARY KEY,
            chart_id        VARCHAR(100) NOT NULL,
            instruction     TEXT         NOT NULL,
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_chart_edits_chart
            ON {_SCHEMA}.reporting_chart_edits (chart_id, created_at DESC);
    """

    create_canvas_drafts = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_canvas_drafts (
            draft_id        VARCHAR(100) PRIMARY KEY,
            user_id         VARCHAR(100) NOT NULL,
            name            TEXT         NOT NULL,
            project_type    VARCHAR(50),
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_canvas_drafts_user_updated
            ON {_SCHEMA}.reporting_canvas_drafts (user_id, updated_at DESC);
    """

    create_canvas_slots = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_canvas_slots (
            slot_id         VARCHAR(100) PRIMARY KEY,
            draft_id        VARCHAR(100) NOT NULL REFERENCES {_SCHEMA}.reporting_canvas_drafts(draft_id) ON DELETE CASCADE,
            chart_id        VARCHAR(100) NOT NULL REFERENCES {_SCHEMA}.reporting_agent_charts(chart_id) ON DELETE CASCADE,
            query_id        VARCHAR(100) NOT NULL,
            original_query  TEXT,
            x               SMALLINT     NOT NULL,
            y               SMALLINT     NOT NULL,
            w               SMALLINT     NOT NULL,
            h               SMALLINT     NOT NULL,
            position        SMALLINT     NOT NULL,
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_canvas_slots_draft
            ON {_SCHEMA}.reporting_canvas_slots (draft_id, position);
        CREATE INDEX IF NOT EXISTS idx_canvas_slots_chart
            ON {_SCHEMA}.reporting_canvas_slots (chart_id);
    """

    create_templates = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_templates (
            template_id       VARCHAR(100) PRIMARY KEY,
            user_id           VARCHAR(100) NOT NULL,
            source_draft_id   VARCHAR(100),
            title             TEXT         NOT NULL,
            project_type      VARCHAR(50),
            created_at        TIMESTAMP    NOT NULL DEFAULT NOW(),
            last_run_at       TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_templates_user_created
            ON {_SCHEMA}.reporting_templates (user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_templates_source_draft
            ON {_SCHEMA}.reporting_templates (source_draft_id);
    """

    create_template_selections = f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA}.reporting_template_selections (
            selection_id    VARCHAR(100) PRIMARY KEY,
            template_id     VARCHAR(100) NOT NULL REFERENCES {_SCHEMA}.reporting_templates(template_id) ON DELETE CASCADE,
            chart_id        VARCHAR(100) NOT NULL REFERENCES {_SCHEMA}.reporting_agent_charts(chart_id) ON DELETE CASCADE,
            query_id        VARCHAR(100) NOT NULL,
            original_query  TEXT,
            x               SMALLINT     NOT NULL,
            y               SMALLINT     NOT NULL,
            w               SMALLINT     NOT NULL,
            h               SMALLINT     NOT NULL,
            position        SMALLINT     NOT NULL,
            created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_tmpl_selections_template
            ON {_SCHEMA}.reporting_template_selections (template_id, position);
        CREATE INDEX IF NOT EXISTS idx_tmpl_selections_chart
            ON {_SCHEMA}.reporting_template_selections (chart_id);
    """

    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_queries)
                cur.execute(create_charts)
                cur.execute(create_cart_series)
                cur.execute(create_pie_slices)
                cur.execute(create_threads)
                cur.execute(create_chart_edits)
                cur.execute(create_canvas_drafts)
                cur.execute(create_canvas_slots)
                cur.execute(create_templates)
                cur.execute(create_template_selections)
        logger.info("reporting tables verified / created (zero JSONB).")
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


_CARTESIAN_TYPES = ("column", "bar", "line", "area", "spline", "areaspline", "scatter")
_PIE_TYPES = ("pie", "donut")


def _decompose_chart(chart: dict, fallback_index: int) -> dict | None:
    """Pull a chart dict apart into per-column values + per-table child rows.

    Returns a dict with everything `save_chart` needs:
      meta: dict (one row in reporting_agent_charts)
      cartesian_series: list[dict] (rows for reporting_chart_cartesian_series)
      pie_slices: list[dict] (rows for reporting_chart_pie_slices)

    Returns None if the chart is missing required fields.
    """
    cid = chart.get("chart_id") or ""
    if not cid:
        return None
    inner = chart.get("chart") if isinstance(chart.get("chart"), dict) else {}
    chart_type = (inner.get("type") if isinstance(inner, dict) else None) or chart.get("type") or ""
    chart_type = str(chart_type).lower()

    title = chart.get("title") if isinstance(chart.get("title"), dict) else {}
    subtitle = chart.get("subtitle") if isinstance(chart.get("subtitle"), dict) else {}
    xaxis = chart.get("xAxis") if isinstance(chart.get("xAxis"), dict) else {}
    yaxis = chart.get("yAxis") if isinstance(chart.get("yAxis"), dict) else {}
    tooltip = chart.get("tooltip") if isinstance(chart.get("tooltip"), dict) else {}
    legend = chart.get("legend") if isinstance(chart.get("legend"), dict) else {}
    plotopts = chart.get("plotOptions") if isinstance(chart.get("plotOptions"), dict) else {}
    type_plotopts = plotopts.get(chart_type) if isinstance(plotopts.get(chart_type), dict) else {}
    data_labels = type_plotopts.get("dataLabels") if isinstance(type_plotopts.get("dataLabels"), dict) else {}

    ev = chart.get("evidence") if isinstance(chart.get("evidence"), dict) else {}

    meta = {
        "chart_id":             cid,
        "chart_index":          int(chart.get("chart_index", fallback_index) or fallback_index),
        "chart_type":           chart_type,
        "title_text":           (title.get("text") if isinstance(title, dict) else None) or "",
        "subtitle_text":        (subtitle.get("text") if isinstance(subtitle, dict) else None),
        "description":          chart.get("description") or "",
        "insight":              chart.get("insight") or "",
        "script":               chart.get("script") or ev.get("code", "") or "",
        "sql_index":            chart.get("sql_index") or chart.get("evidence_sql_index") or ev.get("sql_index"),
        "colors":               chart.get("colors") or [],
        "x_axis_title":         ((xaxis.get("title") or {}).get("text") if isinstance(xaxis.get("title"), dict) else None) if xaxis else None,
        "x_axis_categories":    xaxis.get("categories") if isinstance(xaxis.get("categories"), list) else None,
        "y_axis_title":         ((yaxis.get("title") or {}).get("text") if isinstance(yaxis.get("title"), dict) else None) if yaxis else None,
        "tooltip_value_suffix": tooltip.get("valueSuffix"),
        "data_labels_enabled":  bool(data_labels.get("enabled", True)),
        "legend_enabled":       bool(legend.get("enabled", True)),
    }

    cartesian_series: list[dict] = []
    pie_slices: list[dict] = []
    series_list = chart.get("series") if isinstance(chart.get("series"), list) else []

    if chart_type in _CARTESIAN_TYPES:
        for i, s in enumerate(series_list):
            if not isinstance(s, dict):
                continue
            data_vals = [v for v in (s.get("data") or [])]
            cartesian_series.append({
                "position": i,
                "name":     s.get("name") or f"Series {i+1}",
                "color":    s.get("color"),
                "data":     data_vals,
            })
    elif chart_type in _PIE_TYPES:
        # Exactly one series; iterate its data slices
        if series_list and isinstance(series_list[0], dict):
            for i, slice_obj in enumerate(series_list[0].get("data") or []):
                if not isinstance(slice_obj, dict):
                    continue
                pie_slices.append({
                    "position": i,
                    "name":     slice_obj.get("name") or f"Slice {i+1}",
                    "y":       slice_obj.get("y") or 0,
                    "color":    slice_obj.get("color"),
                })

    return {"meta": meta, "cartesian_series": cartesian_series, "pie_slices": pie_slices}


def save_chart(
    query_id: str,
    user_id: str,
    thread_id: str | None,
    chart: dict,
    fallback_index: int = 0,
) -> bool:
    """Insert/upsert one chart row + its decomposed series/slice rows.

    Writes to up to three tables in one transaction:
      * reporting_agent_charts             — base chart row
      * reporting_chart_cartesian_series   — only if chart is cartesian
      * reporting_chart_pie_slices         — only if chart is pie / donut

    Old child rows for this chart_id are deleted before the new ones are
    inserted, so the chart shape always matches the latest dataset.
    """
    decomposed = _decompose_chart(chart, fallback_index)
    if decomposed is None:
        logger.warning("save_chart skipped — chart has no chart_id (query_id=%s)", query_id)
        return False
    meta = decomposed["meta"]
    if not meta["script"]:
        logger.warning("save_chart: chart_id=%s has no script — re-run will fail later", meta["chart_id"])

    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                # 1) Upsert the chart row.
                cur.execute(
                    f"""
                    INSERT INTO {_SCHEMA}.reporting_agent_charts
                        (chart_id, query_id, user_id, thread_id,
                         chart_index, chart_type,
                         title_text, subtitle_text, description, insight,
                         script, sql_index, colors,
                         x_axis_title, x_axis_categories, y_axis_title,
                         tooltip_value_suffix, data_labels_enabled, legend_enabled,
                         created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (chart_id) DO UPDATE SET
                        chart_type           = EXCLUDED.chart_type,
                        title_text           = EXCLUDED.title_text,
                        subtitle_text        = EXCLUDED.subtitle_text,
                        description          = EXCLUDED.description,
                        insight              = EXCLUDED.insight,
                        script               = EXCLUDED.script,
                        sql_index            = EXCLUDED.sql_index,
                        colors               = EXCLUDED.colors,
                        x_axis_title         = EXCLUDED.x_axis_title,
                        x_axis_categories    = EXCLUDED.x_axis_categories,
                        y_axis_title         = EXCLUDED.y_axis_title,
                        tooltip_value_suffix = EXCLUDED.tooltip_value_suffix,
                        data_labels_enabled  = EXCLUDED.data_labels_enabled,
                        legend_enabled       = EXCLUDED.legend_enabled,
                        updated_at           = NOW()
                    """,
                    (
                        meta["chart_id"], query_id, user_id, thread_id,
                        meta["chart_index"], meta["chart_type"],
                        meta["title_text"], meta["subtitle_text"],
                        meta["description"], meta["insight"],
                        meta["script"], meta["sql_index"], meta["colors"],
                        meta["x_axis_title"], meta["x_axis_categories"], meta["y_axis_title"],
                        meta["tooltip_value_suffix"], meta["data_labels_enabled"], meta["legend_enabled"],
                    ),
                )

                # 2) Replace child rows.
                cur.execute(
                    f"DELETE FROM {_SCHEMA}.reporting_chart_cartesian_series WHERE chart_id = %s",
                    (meta["chart_id"],),
                )
                cur.execute(
                    f"DELETE FROM {_SCHEMA}.reporting_chart_pie_slices WHERE chart_id = %s",
                    (meta["chart_id"],),
                )
                for s in decomposed["cartesian_series"]:
                    cur.execute(
                        f"""
                        INSERT INTO {_SCHEMA}.reporting_chart_cartesian_series
                            (chart_id, position, name, color, data)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (meta["chart_id"], s["position"], s["name"], s["color"], s["data"]),
                    )
                for sl in decomposed["pie_slices"]:
                    cur.execute(
                        f"""
                        INSERT INTO {_SCHEMA}.reporting_chart_pie_slices
                            (chart_id, position, name, y, color)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (meta["chart_id"], sl["position"], sl["name"], sl["y"], sl["color"]),
                    )
        return True
    except Exception as exc:
        logger.error("save_chart failed for chart_id=%s: %s", meta["chart_id"], exc)
        return False


def clone_chart(source_chart_id: str, new_chart_id: str) -> bool:
    """Make a complete copy of a chart row (and its child series/slice rows)
    under a new chart_id. Used when a chart is added to a canvas — the slot
    gets its own chart row so subsequent edits don't leak back into the chat.

    Returns True on success.
    """
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                # Copy the base row
                cur.execute(
                    f"""
                    INSERT INTO {_SCHEMA}.reporting_agent_charts (
                        chart_id, query_id, user_id, thread_id, chart_index, chart_type,
                        title_text, subtitle_text, description, insight,
                        script, sql_index, colors,
                        x_axis_title, x_axis_categories, y_axis_title,
                        tooltip_value_suffix, data_labels_enabled, legend_enabled,
                        created_at, updated_at
                    )
                    SELECT %s, query_id, user_id, thread_id, chart_index, chart_type,
                           title_text, subtitle_text, description, insight,
                           script, sql_index, colors,
                           x_axis_title, x_axis_categories, y_axis_title,
                           tooltip_value_suffix, data_labels_enabled, legend_enabled,
                           NOW(), NOW()
                    FROM {_SCHEMA}.reporting_agent_charts
                    WHERE chart_id = %s
                    """,
                    (new_chart_id, source_chart_id),
                )
                if cur.rowcount == 0:
                    return False
                # Copy cartesian series
                cur.execute(
                    f"""
                    INSERT INTO {_SCHEMA}.reporting_chart_cartesian_series
                        (chart_id, position, name, color, data)
                    SELECT %s, position, name, color, data
                    FROM {_SCHEMA}.reporting_chart_cartesian_series
                    WHERE chart_id = %s
                    """,
                    (new_chart_id, source_chart_id),
                )
                # Copy pie slices
                cur.execute(
                    f"""
                    INSERT INTO {_SCHEMA}.reporting_chart_pie_slices
                        (chart_id, position, name, y, color)
                    SELECT %s, position, name, y, color
                    FROM {_SCHEMA}.reporting_chart_pie_slices
                    WHERE chart_id = %s
                    """,
                    (new_chart_id, source_chart_id),
                )
        return True
    except Exception as exc:
        logger.error("clone_chart failed (%s → %s): %s", source_chart_id, new_chart_id, exc)
        return False


def _row_to_chart_dict(row: dict) -> dict:
    """Reconstruct the API-shaped chart dict from a base row + its children.

    The base row is from `reporting_agent_charts`. Series / slices are loaded
    here. Output matches the same shape /report/stream emits, so callers
    don't care that storage is decomposed.
    """
    if not row:
        return {}
    cid = row["chart_id"]
    ctype = (row.get("chart_type") or "").lower()
    out: dict = {
        "chart_id":    cid,
        "chart":       {"type": ctype} if ctype else {},
        "title":       {"text": row.get("title_text") or ""},
        "description": row.get("description") or "",
        "insight":     row.get("insight") or "",
        "script":      row.get("script") or "",
        "sql_index":   row.get("sql_index"),
        "colors":      list(row.get("colors") or []),
    }
    if row.get("subtitle_text"):
        out["subtitle"] = {"text": row["subtitle_text"]}

    # Visual extras with sensible defaults
    if row.get("legend_enabled") is not None:
        out["legend"] = {"enabled": bool(row["legend_enabled"])}
    if row.get("tooltip_value_suffix"):
        out["tooltip"] = {"valueSuffix": row["tooltip_value_suffix"]}
    if ctype:
        out["plotOptions"] = {ctype: {"dataLabels": {"enabled": bool(row.get("data_labels_enabled", True))}}}

    if ctype in _CARTESIAN_TYPES:
        xaxis: dict = {}
        if row.get("x_axis_categories") is not None:
            xaxis["categories"] = list(row["x_axis_categories"])
        if row.get("x_axis_title"):
            xaxis["title"] = {"text": row["x_axis_title"]}
        if xaxis:
            out["xAxis"] = xaxis
        if row.get("y_axis_title"):
            out["yAxis"] = {"title": {"text": row["y_axis_title"]}}
        # Series
        series_rows = _fetch_rows(
            f"""
            SELECT position, name, color, data
            FROM {_SCHEMA}.reporting_chart_cartesian_series
            WHERE chart_id = %s ORDER BY position
            """,
            (cid,),
        )
        out["series"] = [
            {
                "name": s["name"],
                "data": [float(v) if v is not None else None for v in (s.get("data") or [])],
                **({"color": s["color"]} if s.get("color") else {}),
            }
            for s in series_rows
        ]
    elif ctype in _PIE_TYPES:
        slice_rows = _fetch_rows(
            f"""
            SELECT position, name, y, color
            FROM {_SCHEMA}.reporting_chart_pie_slices
            WHERE chart_id = %s ORDER BY position
            """,
            (cid,),
        )
        out["series"] = [{
            "name": (row.get("title_text") or "Distribution"),
            "data": [
                {
                    "name": s["name"],
                    "y":    float(s["y"]) if s.get("y") is not None else 0,
                    **({"color": s["color"]} if s.get("color") else {}),
                }
                for s in slice_rows
            ],
        }]
    return out


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

    # 1) Update the query metadata row. `errors` is plain TEXT now (not JSONB);
    # we join the list with newlines so SELECTs read cleanly.
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
            "\n".join(errors) if errors else None,
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
            "\n".join(errors) if errors else None,
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

# ─────────────────────────────────────────────
# Chart reads — reconstruct the API-shaped chart dict from decomposed rows
# ─────────────────────────────────────────────

_CHART_BASE_COLS = (
    "chart_id, query_id, user_id, thread_id, chart_index, chart_type, "
    "title_text, subtitle_text, description, insight, script, sql_index, colors, "
    "x_axis_title, x_axis_categories, y_axis_title, "
    "tooltip_value_suffix, data_labels_enabled, legend_enabled, "
    "created_at, updated_at"
)


def get_chart(chart_id: str) -> dict | None:
    """Return one chart with its `chart` config rebuilt from decomposed rows."""
    row = _fetch_row(
        f"SELECT {_CHART_BASE_COLS} FROM {_SCHEMA}.reporting_agent_charts WHERE chart_id = %s",
        (chart_id,),
    )
    if not row:
        return None
    row["chart"] = _row_to_chart_dict(row)
    return row


def get_charts_for_query(query_id: str) -> list[dict]:
    rows = _fetch_rows(
        f"""
        SELECT {_CHART_BASE_COLS}
        FROM {_SCHEMA}.reporting_agent_charts
        WHERE query_id = %s ORDER BY chart_index ASC
        """,
        (query_id,),
    )
    for r in rows:
        r["chart"] = _row_to_chart_dict(r)
    return rows


def get_charts_by_user(user_id: str, limit: int = 100) -> list[dict]:
    rows = _fetch_rows(
        f"""
        SELECT {_CHART_BASE_COLS}
        FROM {_SCHEMA}.reporting_agent_charts
        WHERE user_id = %s ORDER BY created_at DESC LIMIT %s
        """,
        (user_id, limit),
    )
    for r in rows:
        r["chart"] = _row_to_chart_dict(r)
    return rows


def get_charts_by_thread(thread_id: str, limit: int = 100) -> list[dict]:
    rows = _fetch_rows(
        f"""
        SELECT {_CHART_BASE_COLS}
        FROM {_SCHEMA}.reporting_agent_charts
        WHERE thread_id = %s ORDER BY created_at DESC LIMIT %s
        """,
        (thread_id, limit),
    )
    for r in rows:
        r["chart"] = _row_to_chart_dict(r)
    return rows


# ─────────────────────────────────────────────
# Chart edits (chart_id is the only handle; chart-edit is canvas-scoped via
# the slot's per-canvas chart_id clone — see canvas endpoint)
# ─────────────────────────────────────────────

def update_chart_by_id(chart_id: str, patched_chart: dict) -> bool:
    """Re-save a chart in place. Just delegates to save_chart with the same
    chart_id, which UPSERTs the base row and re-creates the child rows.
    """
    base = _fetch_row(
        f"SELECT query_id, user_id, thread_id FROM {_SCHEMA}.reporting_agent_charts WHERE chart_id = %s",
        (chart_id,),
    )
    if not base:
        return False
    patched_chart["chart_id"] = chart_id
    return save_chart(
        query_id=base["query_id"],
        user_id=base["user_id"],
        thread_id=base.get("thread_id"),
        chart=patched_chart,
        fallback_index=0,
    )


def log_chart_edit(chart_id: str, instruction: str) -> None:
    """Audit log only — instruction + chart_id + timestamp. The current chart
    state is in reporting_agent_charts, so we don't snapshot the patched chart.
    """
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_chart_edits (chart_id, instruction)
        VALUES (%s, %s)
        """,
        (chart_id, instruction),
    )


def get_chart_edit_history(chart_id: str, limit: int = 20) -> list[dict]:
    return _fetch_rows(
        f"""
        SELECT edit_id, chart_id, instruction, created_at
        FROM {_SCHEMA}.reporting_chart_edits
        WHERE chart_id = %s ORDER BY created_at DESC LIMIT %s
        """,
        (chart_id, limit),
    )


# ─────────────────────────────────────────────
# Canvas drafts (metadata only) + canvas slots (their own table)
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
            (draft_id, user_id, name, project_type)
        VALUES (%s, %s, %s, %s)
        """,
        (draft_id, user_id, name, project_type),
    )


def rename_canvas_draft(draft_id: str, name: str) -> None:
    _exec(
        f"""
        UPDATE {_SCHEMA}.reporting_canvas_drafts
           SET name = %s, updated_at = NOW()
         WHERE draft_id = %s
        """,
        (name, draft_id),
    )


def delete_canvas_draft(draft_id: str) -> bool:
    """Cascade-deletes its slots via the FK ON DELETE CASCADE."""
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


def _draft_meta(draft_id: str) -> dict | None:
    return _fetch_row(
        f"""
        SELECT draft_id, user_id, name, project_type, created_at, updated_at
        FROM {_SCHEMA}.reporting_canvas_drafts WHERE draft_id = %s
        """,
        (draft_id,),
    )


def list_canvas_slots(draft_id: str) -> list[dict]:
    """Return slot rows for a draft, each with its chart fully reconstructed."""
    rows = _fetch_rows(
        f"""
        SELECT slot_id, draft_id, chart_id, query_id, original_query,
               x, y, w, h, position, created_at
        FROM {_SCHEMA}.reporting_canvas_slots
        WHERE draft_id = %s ORDER BY position ASC
        """,
        (draft_id,),
    )
    for r in rows:
        ch = get_chart(r["chart_id"])
        r["chart"] = ch.get("chart") if ch else {}
    return rows


def get_canvas_draft(draft_id: str) -> dict | None:
    """Return draft metadata + its slots (each with the chart reconstructed)."""
    meta = _draft_meta(draft_id)
    if not meta:
        return None
    meta["slots"] = list_canvas_slots(draft_id)
    return meta


def list_canvas_drafts(user_id: str, limit: int = 50) -> list[dict]:
    """All drafts for a user. Slot lists are NOT inlined for list calls
    (cheap row-only listing); call `get_canvas_draft(draft_id)` for slots."""
    return _fetch_rows(
        f"""
        SELECT draft_id, user_id, name, project_type, created_at, updated_at
        FROM {_SCHEMA}.reporting_canvas_drafts
        WHERE user_id = %s ORDER BY updated_at DESC LIMIT %s
        """,
        (user_id, limit),
    )


def replace_canvas_slots(draft_id: str, slot_specs: list[dict]) -> list[dict]:
    """Wipe all slots for a draft and insert the given list (one row each).

    Each `slot_spec` carries:
      slot_id (server-assigned if missing), chart_id, query_id, original_query,
      x, y, w, h, position
    Returns the freshly-saved slot list (with reconstructed charts).
    """
    import uuid as _uuid
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {_SCHEMA}.reporting_canvas_slots WHERE draft_id = %s",
                    (draft_id,),
                )
                for s in slot_specs:
                    sid = s.get("slot_id") or str(_uuid.uuid4())
                    cur.execute(
                        f"""
                        INSERT INTO {_SCHEMA}.reporting_canvas_slots
                            (slot_id, draft_id, chart_id, query_id, original_query,
                             x, y, w, h, position)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            sid, draft_id, s["chart_id"],
                            s.get("query_id") or "",
                            s.get("original_query") or "",
                            int(s["x"]), int(s["y"]), int(s["w"]), int(s["h"]),
                            int(s["position"]),
                        ),
                    )
                cur.execute(
                    f"UPDATE {_SCHEMA}.reporting_canvas_drafts SET updated_at = NOW() WHERE draft_id = %s",
                    (draft_id,),
                )
    except Exception as exc:
        logger.error("replace_canvas_slots failed for draft %s: %s", draft_id, exc)
    return list_canvas_slots(draft_id)


# ─────────────────────────────────────────────
# Templates (metadata only) + template selections (their own table)
# ─────────────────────────────────────────────

def create_template(
    template_id: str,
    user_id: str,
    title: str,
    project_type: str,
    source_draft_id: str | None = None,
) -> None:
    _exec(
        f"""
        INSERT INTO {_SCHEMA}.reporting_templates
            (template_id, user_id, title, project_type, source_draft_id, last_run_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        """,
        (template_id, user_id, title, project_type, source_draft_id),
    )


def update_template_meta(
    template_id: str,
    title: str | None = None,
    project_type: str | None = None,
    source_draft_id: str | None = None,
) -> bool:
    """Update template metadata fields. Selections live in their own table —
    use `replace_template_selections` for those.
    """
    sets: list[str] = []
    params: list = []
    if title is not None:
        sets.append("title = %s"); params.append(title)
    if project_type is not None:
        sets.append("project_type = %s"); params.append(project_type)
    if source_draft_id is not None:
        sets.append("source_draft_id = %s"); params.append(source_draft_id)
    if not sets:
        return True
    params.append(template_id)
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {_SCHEMA}.reporting_templates SET {', '.join(sets)} WHERE template_id = %s",
                    tuple(params),
                )
                return cur.rowcount > 0
    except Exception as exc:
        logger.error("update_template_meta failed: %s", exc)
        return False


def find_template_by_draft(source_draft_id: str) -> dict | None:
    """Most recent template linked to this draft. Used for upsert semantics."""
    return _fetch_row(
        f"""
        SELECT template_id, user_id, source_draft_id, title, project_type,
               created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates
        WHERE source_draft_id = %s ORDER BY created_at DESC LIMIT 1
        """,
        (source_draft_id,),
    )


def bump_template_last_run(template_id: str) -> None:
    _exec(
        f"UPDATE {_SCHEMA}.reporting_templates SET last_run_at = NOW() WHERE template_id = %s",
        (template_id,),
    )


def list_template_selections(template_id: str) -> list[dict]:
    rows = _fetch_rows(
        f"""
        SELECT selection_id, template_id, chart_id, query_id, original_query,
               x, y, w, h, position, created_at
        FROM {_SCHEMA}.reporting_template_selections
        WHERE template_id = %s ORDER BY position ASC
        """,
        (template_id,),
    )
    for r in rows:
        ch = get_chart(r["chart_id"])
        r["chart"] = ch.get("chart") if ch else {}
    return rows


def get_template(template_id: str) -> dict | None:
    meta = _fetch_row(
        f"""
        SELECT template_id, user_id, source_draft_id, title, project_type,
               created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates WHERE template_id = %s
        """,
        (template_id,),
    )
    if not meta:
        return None
    meta["selections"] = list_template_selections(template_id)
    return meta


def get_templates_by_user(user_id: str, limit: int = 50) -> list[dict]:
    """Metadata only — no selections inlined. Call get_template for that."""
    return _fetch_rows(
        f"""
        SELECT template_id, user_id, source_draft_id, title, project_type,
               created_at, last_run_at
        FROM {_SCHEMA}.reporting_templates
        WHERE user_id = %s ORDER BY created_at DESC LIMIT %s
        """,
        (user_id, limit),
    )


def replace_template_selections(template_id: str, selection_specs: list[dict]) -> list[dict]:
    """Wipe all selections for a template and insert the given list."""
    import uuid as _uuid
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {_SCHEMA}.reporting_template_selections WHERE template_id = %s",
                    (template_id,),
                )
                for s in selection_specs:
                    sid = s.get("selection_id") or str(_uuid.uuid4())
                    cur.execute(
                        f"""
                        INSERT INTO {_SCHEMA}.reporting_template_selections
                            (selection_id, template_id, chart_id, query_id, original_query,
                             x, y, w, h, position)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            sid, template_id, s["chart_id"],
                            s.get("query_id") or "",
                            s.get("original_query") or "",
                            int(s["x"]), int(s["y"]), int(s["w"]), int(s["h"]),
                            int(s["position"]),
                        ),
                    )
                cur.execute(
                    f"UPDATE {_SCHEMA}.reporting_templates SET last_run_at = NOW() WHERE template_id = %s",
                    (template_id,),
                )
    except Exception as exc:
        logger.error("replace_template_selections failed for template %s: %s", template_id, exc)
    return list_template_selections(template_id)


def sync_draft_to_template(draft_id: str) -> int:
    """Mirror the canvas draft's slots into the linked template's selections.

    Each slot's `chart_id` is used directly — no per-template chart clone.
    Returns the number of templates synced (0 or more).
    """
    # Find linked templates
    rows = _fetch_rows(
        f"SELECT template_id FROM {_SCHEMA}.reporting_templates WHERE source_draft_id = %s",
        (draft_id,),
    )
    if not rows:
        return 0
    slots = list_canvas_slots(draft_id)
    selection_specs = [{
        "chart_id":       s["chart_id"],
        "query_id":       s.get("query_id") or "",
        "original_query": s.get("original_query") or "",
        "x": s["x"], "y": s["y"], "w": s["w"], "h": s["h"], "position": s["position"],
    } for s in slots]
    n = 0
    for r in rows:
        replace_template_selections(r["template_id"], selection_specs)
        n += 1
    return n


def delete_template(template_id: str) -> bool:
    """Cascade-deletes its selections via FK ON DELETE CASCADE."""
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
