"""
Report templates API — kept intentionally small.

Endpoints:
  POST   /templates                       create a template from a canvas selection
  GET    /templates?user_id=              list a user's templates
  DELETE /templates/{template_id}         delete a template
  POST   /templates/{template_id}/run     re-run the template's scripts with today's data

The chat-threads endpoints stay in this file (separate concern from templates),
but every "extras" endpoint (PATCH, GET-single, draft-upsert) was removed in
favor of these four.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from services import db_service
from services.insight_refresh import refresh_insight
from tools.python_sandbox import PythonSandbox
from agents.graph_agent import _round_floats
# Re-use the canvas slot example so the docs show one consistent shape.
# `Chart` is the strict per-type discriminated union from models.chart_types.
from models.chart_types import Chart
from api.v1.endpoints.canvas import _EXAMPLE_CHART, _EXAMPLE_SLOT  # noqa: F401 (kept for reference)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Templates"])

MAX_CHARTS_PER_TEMPLATE = 6


# ── Models ──────────────────────────────────────────────────────────────────

class TemplateSelectionIn(BaseModel):
    """One chart selected for a saved template.

    Identical shape to `CanvasSlot` — when finalizing, copy each draft slot
    straight into `selections`. The chart object contains its own `script`
    (what powers `POST /templates/{id}/run`).

    Required (mirrored from /report/stream):
      * `query_id` — the SSE stream_started.query_id
      * `chart`    — the full chart object from complete.charts[i]
                     (its `chart.script` is the runnable code)

    Optional: `original_query`, layout (`x` / `y` / `w` / `h` / `position`).
    """
    query_id: str = Field(..., description="From SSE stream_started.query_id")
    chart:    Chart = Field(..., description="Full chart object — strict per-type schema (cartesian or pie). chart_id, script, insight, colors are required.")

    original_query: str = Field(default="", description="The NL question that produced the chart")

    x:        int | None = Field(default=None, ge=0, le=11, description="Column offset 0..11 on the 12-col grid")
    y:        int | None = Field(default=None, ge=0,        description="Row offset (≥0)")
    w:        int | None = Field(default=None, ge=1, le=12, description="Width in cols 1..12")
    h:        int | None = Field(default=None, ge=1,        description="Height in rows ≥1")
    position: int | None = Field(default=None, ge=0,        description="Legacy list order; server re-derives from (y, x)")

    model_config = ConfigDict(
        extra="ignore",                         # → no `additionalProp1` in Swagger
        json_schema_extra={"example": _EXAMPLE_SLOT},
    )


_EXAMPLE_TEMPLATE: dict[str, Any] = {
    "user_id":         "u_123",
    "title":           "Q1 NTM weekly run-rate",
    "project_type":    "NTM",
    "source_draft_id": "14dfd449-d9b6-484e-858a-e3d7b36a353a",
    "selections":      [_EXAMPLE_SLOT],
}


class TemplateIn(BaseModel):
    user_id:      str = Field(..., description="User identifier")
    title:        str = Field(..., description="Display title for the template")
    project_type: str = Field(default="", description="NTM | AHLOB Modernization | Both")
    source_draft_id: str | None = Field(
        default=None,
        description=(
            "If set, links this template to a canvas draft. POST is then an UPSERT: "
            "if a template already exists for this draft_id it is UPDATED in place "
            "(never duplicated). Subsequent canvas changes are auto-mirrored."
        ),
    )
    selections:   list[TemplateSelectionIn] = Field(
        ..., min_length=1, max_length=MAX_CHARTS_PER_TEMPLATE,
        description=f"1..{MAX_CHARTS_PER_TEMPLATE} charts to save in this template",
    )

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={"example": _EXAMPLE_TEMPLATE},
    )


# ── Helpers ─────────────────────────────────────────────────────────────────

def _selection_script(sel: dict[str, Any]) -> str:
    """Pull the Python+SQL code from a saved selection.

    Source of truth is `chart.script` — that's where the SSE response puts
    it and where the canvas/template payloads carry it. Falls back to
    legacy shapes (top-level `script`, `evidence.code`) for templates
    saved before the slot model was simplified.
    """
    chart = sel.get("chart") or {}
    return (
        (chart.get("script") if isinstance(chart, dict) else "")
        or sel.get("script")
        or ((sel.get("evidence") or {}).get("code", "") if isinstance(sel.get("evidence"), dict) else "")
        or ""
    )


def _pick_label_and_value_cols(sample_row: dict) -> tuple[str | None, str | None]:
    """Find a string label column and a numeric value column in a result row."""
    keys = list(sample_row.keys())
    val_col = None
    for k in keys:
        v = sample_row.get(k)
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)) and v is not None:
            val_col = k
            break
    if val_col is None:
        return None, None
    label_col = None
    for k in keys:
        if k == val_col:
            continue
        if isinstance(sample_row.get(k), str):
            label_col = k
            break
    if label_col is None:
        for k in keys:
            if k != val_col:
                label_col = k
                break
    return label_col, val_col


def _rebuild_chart_with_fresh_data(chart: dict[str, Any], fresh: dict[str, Any]) -> dict[str, Any]:
    """Swap the saved chart's data for freshly-fetched rows.

    The chart's shape (type, title, colors, axes) is preserved; only series
    data and xAxis categories (or pie slice array) get replaced. Floats are
    rounded to 2 decimals, matching the chart agent's rule.
    """
    new_chart = json.loads(json.dumps(chart, default=str))
    new_chart.setdefault("subtitle", {})
    if not isinstance(new_chart.get("subtitle"), dict):
        new_chart["subtitle"] = {"text": str(new_chart.get("subtitle", ""))}
    new_chart["subtitle"]["text"] = (
        (new_chart["subtitle"].get("text", "") or "") + "  (refreshed)"
    ).strip()

    result = fresh.get("result") if isinstance(fresh, dict) else None
    rows = None
    if isinstance(result, dict):
        for k in ("chart_data", "rows", "data", "series", "detail_rows"):
            if isinstance(result.get(k), list):
                rows = result[k]
                break
    elif isinstance(result, list):
        rows = result

    if not (rows and isinstance(rows[0], dict)):
        _round_floats(new_chart)
        return new_chart

    label_col, val_col = _pick_label_and_value_cols(rows[0])
    if label_col is None or val_col is None:
        _round_floats(new_chart)
        return new_chart

    chart_type = (
        (new_chart.get("chart") or {}).get("type")
        or new_chart.get("type") or ""
    ).lower()
    series = new_chart.get("series") or []

    if chart_type in ("pie", "donut") and series and isinstance(series[0], dict):
        series[0]["data"] = [
            {"name": str(r.get(label_col, "")), "y": r.get(val_col)} for r in rows
        ]
        new_chart["series"] = series
        _round_floats(new_chart)
        return new_chart

    categories = [str(r.get(label_col, "")) for r in rows]
    values = [r.get(val_col) for r in rows]
    if isinstance(new_chart.get("xAxis"), dict):
        new_chart["xAxis"]["categories"] = categories
    elif isinstance(new_chart.get("xAxis"), list) and new_chart["xAxis"]:
        if isinstance(new_chart["xAxis"][0], dict):
            new_chart["xAxis"][0]["categories"] = categories

    if series and isinstance(series[0], dict):
        series[0]["data"] = values
        new_chart["series"] = series

    _round_floats(new_chart)
    return new_chart


# ── Threads (unchanged — separate concern from templates) ───────────────────

@router.get("/threads", summary="List chat threads for a user")
def list_threads(user_id: str = Query(...), limit: int = Query(default=50, le=200)):
    return {"threads": db_service.get_threads_by_user(user_id, limit=limit)}


@router.get("/threads/{thread_id}/queries", summary="List queries in a thread")
def list_thread_queries(thread_id: str, limit: int = Query(default=100, le=500)):
    return {"thread_id": thread_id, "queries": db_service.get_queries_for_thread(thread_id, limit=limit)}


# ── Templates (4 endpoints, no extras) ──────────────────────────────────────

def _selection_specs(selections: list) -> list[dict]:
    """Convert validated `TemplateSelectionIn` objects into the dict shape
    `db_service.replace_template_selections` consumes.

    The chart object inside each selection MUST already have its `chart_id`
    pointing at a row in `reporting_agent_charts` (the canvas-flow saves the
    cloned chart there before it gets here).
    """
    out: list[dict] = []
    for i, s in enumerate(selections):
        d = s.model_dump()
        chart_obj = d.get("chart") or {}
        cid = chart_obj.get("chart_id")
        if not cid:
            raise HTTPException(status_code=422, detail="selection.chart.chart_id is required")
        out.append({
            "chart_id":       cid,
            "query_id":       d.get("query_id") or "",
            "original_query": d.get("original_query") or "",
            "x":        int(d.get("x") or 0),
            "y":        int(d.get("y") or 0),
            "w":        int(d.get("w") or 6),
            "h":        int(d.get("h") or 4),
            "position": int(d.get("position") if d.get("position") is not None else i),
        })
    return out


@router.post("/templates", summary="Create OR update (upsert by source_draft_id) a template")
def create_template(payload: TemplateIn):
    """Save the canvas as a template. Upsert by `source_draft_id`."""
    if not payload.selections:
        raise HTTPException(status_code=400, detail="At least one chart selection is required.")
    if len(payload.selections) > MAX_CHARTS_PER_TEMPLATE:
        raise HTTPException(
            status_code=400,
            detail=f"Max {MAX_CHARTS_PER_TEMPLATE} charts per template, got {len(payload.selections)}.",
        )

    title       = payload.title.strip() or "Untitled report"
    sel_specs   = _selection_specs(payload.selections)

    # Upsert by source_draft_id — same draft-to-template never duplicates.
    if payload.source_draft_id:
        existing = db_service.find_template_by_draft(payload.source_draft_id)
        if existing:
            template_id = existing["template_id"]
            db_service.update_template_meta(
                template_id=template_id,
                title=title,
                project_type=payload.project_type,
                source_draft_id=payload.source_draft_id,
            )
            db_service.replace_template_selections(template_id, sel_specs)
            return {"template_id": template_id, "action": "updated"}

    template_id = str(uuid.uuid4())
    db_service.create_template(
        template_id=template_id,
        user_id=payload.user_id,
        title=title,
        project_type=payload.project_type,
        source_draft_id=payload.source_draft_id,
    )
    db_service.replace_template_selections(template_id, sel_specs)
    return {"template_id": template_id, "action": "created"}


@router.get("/templates", summary="List a user's templates (metadata only — no selections inlined)")
def list_templates(user_id: str = Query(...), limit: int = Query(default=50, le=200)):
    return {"templates": db_service.get_templates_by_user(user_id, limit=limit)}


@router.get("/templates/{template_id}", summary="Fetch one template, with its selections inlined")
def get_template(template_id: str):
    """Returns the template plus every selection (with the chart fully
    reconstructed from `reporting_agent_charts`). Use this when opening a
    template view; the list endpoint is metadata-only for cheap listing."""
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail="Template not found")
    return row


@router.delete("/templates/{template_id}", summary="Delete a template")
def delete_template(template_id: str):
    if not db_service.get_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")
    if not db_service.delete_template(template_id):
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"template_id": template_id, "deleted": True}


@router.post("/templates/{template_id}/run", summary="Re-run the template with today's data")
def run_template(template_id: str):
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail="Template not found")

    # Selections come as proper rows from the dedicated table — no JSONB parse.
    selections = row.get("selections") or []
    sandbox = PythonSandbox()
    rendered_selections: list[dict[str, Any]] = []
    script_reports: list[dict[str, Any]] = []

    for sel in selections:
        t0 = time.perf_counter()
        chart = sel.get("chart") or {}
        code = _selection_script(sel)

        if not code.strip():
            fresh = {"status": "error", "error": "Empty script — cannot re-run", "result": None}
        else:
            try:
                fresh = sandbox.execute(code, 45)
            except Exception as e:
                fresh = {"status": "error", "error": str(e), "result": None}

        elapsed_ms = (time.perf_counter() - t0) * 1000
        status = fresh.get("status", "success") if isinstance(fresh, dict) else "success"
        script_reports.append({
            "query_id": sel.get("query_id"),
            "chart_id": (chart.get("chart_id") if isinstance(chart, dict) else None),
            "status": status,
            "error": fresh.get("error", "") if isinstance(fresh, dict) else "",
            "elapsed_ms": round(elapsed_ms, 2),
        })

        if status == "success":
            rebuilt = _rebuild_chart_with_fresh_data(chart, fresh)
            original_insight = chart.get("insight") if isinstance(chart, dict) else ""
            if original_insight:
                title = ""
                if isinstance(rebuilt.get("title"), dict):
                    title = rebuilt["title"].get("text", "") or ""
                elif isinstance(rebuilt.get("title"), str):
                    title = rebuilt["title"]
                rebuilt["insight"] = refresh_insight(
                    title, original_insight, fresh.get("result"),
                )
            new_chart = rebuilt
        else:
            new_chart = json.loads(json.dumps(chart, default=str))
            sub = new_chart.get("subtitle") if isinstance(new_chart.get("subtitle"), dict) else {}
            sub["text"] = ((sub.get("text", "") or "") + "  (refresh failed — showing saved data)").strip()
            new_chart["subtitle"] = sub

        # Validate the rebuilt chart against the strict per-type Chart union
        # before returning. If reshape produced something off-shape, log it
        # but still return — the UI's Chart-typed parser will catch it too.
        try:
            from models.chart_types import parse_chart as _parse_chart
            _parse_chart(new_chart)
        except Exception as ve:
            logger.warning(
                "rebuilt chart failed Chart validation (chart_id=%s): %s",
                new_chart.get("chart_id"), ve,
            )

        # Return the WHOLE selection back so the UI can render in the same
        # spot it was saved. Layout (x/y/w/h/position) is carried verbatim
        # from the saved selection — only `chart` is the freshly rebuilt one.
        rendered_selections.append({
            "query_id":       sel.get("query_id"),
            "chart_id":       new_chart.get("chart_id"),
            "original_query": sel.get("original_query", ""),
            "x":              sel.get("x"),
            "y":              sel.get("y"),
            "w":              sel.get("w"),
            "h":              sel.get("h"),
            "position":       sel.get("position"),
            "chart":          new_chart,
        })

    # Bump just the `last_run_at` timestamp — the heavy `last_rendered`
    # blob has been removed (the UI re-runs on demand instead of caching
    # a stale snapshot).
    db_service.bump_template_last_run(template_id)
    return {
        "template_id":    template_id,
        "selections":     rendered_selections,
        "charts":         [s["chart"] for s in rendered_selections],
        "script_reports": script_reports,
        "rendered_at":    time.time(),
    }
