"""
Chat threads + report templates API.

Endpoints:
  GET  /threads?user_id=                 list a user's chat threads
  GET  /threads/{thread_id}/queries      ordered queries in a thread
  POST /templates                         create a template (selected charts + evidence)
  GET  /templates?user_id=               list templates for a user
  GET  /templates/{template_id}          fetch one template
  POST /templates/{template_id}/run      re-run the template's scripts with fresh data
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from services import db_service
from services.insight_refresh import refresh_insight
from tools.python_sandbox import PythonSandbox

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Templates"])


# ── Models ──────────────────────────────────────────────────────────────────

class TemplateSelectionIn(BaseModel):
    """One chart selected for inclusion in a finalized report.

    Layout fields are carried verbatim from the canvas draft so a finalized
    template reproduces the exact 2D positioning the user arranged.

    * `x`, `y`, `w`, `h` — 12-column grid coordinates + size.
    * `position`         — legacy list index (kept for back-compat, server
                           re-derives from y/x on write).
    """
    position:       int | None = Field(default=None, ge=0)
    x:              int | None = Field(default=None, ge=0, le=11)
    y:              int | None = Field(default=None, ge=0)
    w:              int | None = Field(default=None, ge=1, le=12)
    h:              int | None = Field(default=None, ge=1)
    query_id:       str
    chart_index:    int = Field(..., ge=0)
    chart:          dict[str, Any]
    evidence:       dict[str, Any] | None = None
    original_query: str = ""

    class Config:
        extra = "allow"


class TemplateIn(BaseModel):
    user_id: str
    username: str
    thread_id: str | None = None
    title: str
    project_type: str = ""
    # Link back to the canvas draft this template was finalized from. Present
    # when the UI finalizes a draft; absent if a template is built directly
    # via the API. When present, layout changes to that draft propagate into
    # this template's selections automatically (see
    # db_service.propagate_draft_layout).
    source_draft_id: str | None = None
    selections: list[TemplateSelectionIn]


# ── Thread routes ───────────────────────────────────────────────────────────

@router.get("/threads", summary="List chat threads for a user")
def list_threads(user_id: str = Query(...), limit: int = Query(default=50, le=200)):
    return {"threads": db_service.get_threads_by_user(user_id, limit=limit)}


@router.get("/threads/{thread_id}/queries", summary="List queries in a thread")
def list_thread_queries(thread_id: str, limit: int = Query(default=100, le=500)):
    rows = db_service.get_queries_for_thread(thread_id, limit=limit)
    return {"thread_id": thread_id, "queries": rows}


# ── Template routes ─────────────────────────────────────────────────────────

MAX_CHARTS_PER_TEMPLATE = 6


@router.post("/templates", summary="Create a finalized report template from selected charts")
def create_template(payload: TemplateIn):
    if not payload.selections:
        raise HTTPException(status_code=400, detail="At least one chart selection is required.")
    if len(payload.selections) > MAX_CHARTS_PER_TEMPLATE:
        raise HTTPException(
            status_code=400,
            detail=f"Max {MAX_CHARTS_PER_TEMPLATE} charts per template, got {len(payload.selections)}.",
        )

    template_id = str(uuid.uuid4())
    raw_selections = [s.dict() for s in payload.selections]
    # Reuse the canvas layout normaliser so templates respect the exact 2D
    # positioning from the draft. This assigns x/y/w/h for any slots that
    # arrive without them and re-derives `position` from the 2D order.
    from api.v1.endpoints.canvas import _normalise_positions as _norm_layout
    raw_selections = _norm_layout(raw_selections)

    db_service.create_template(
        template_id=template_id,
        user_id=payload.user_id,
        username=payload.username,
        thread_id=payload.thread_id,
        title=payload.title,
        project_type=payload.project_type,
        selections=raw_selections,
        last_rendered={"charts": [s["chart"] for s in raw_selections]},
        source_draft_id=payload.source_draft_id,
    )

    return {"template_id": template_id}


@router.get("/templates", summary="List templates for a user")
def list_templates(user_id: str = Query(...), limit: int = Query(default=50, le=200)):
    return {"templates": db_service.get_templates_by_user(user_id, limit=limit)}


@router.get("/templates/{template_id}", summary="Fetch one template")
def get_template(template_id: str):
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail="Template not found")
    return row


@router.delete("/templates/{template_id}", summary="Delete a template")
def delete_template(template_id: str):
    if not db_service.get_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")
    ok = db_service.delete_template(template_id)
    if not ok:
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"template_id": template_id, "deleted": True}


def _run_one_script(code: str, timeout_seconds: int = 45) -> dict[str, Any]:
    """Re-run a single saved Python+SQL script in the sandbox. Returns {status, result, error}."""
    if not code or not code.strip():
        return {"status": "error", "error": "Empty script", "result": None}
    sandbox = PythonSandbox()
    return sandbox.execute(code, timeout_seconds)


def _pick_label_and_value_cols(sample_row: dict) -> tuple[str | None, str | None]:
    """Find a string-ish 'label' column and a numeric 'value' column.

    Rules:
      - `value` = the first column whose sample value is numeric (int/float, non-bool)
      - `label` = the first column whose sample value is NOT the value column
                  and is either a string OR not numeric
      - If no numeric column exists, we return (None, None) — caller falls back.
    """
    keys = list(sample_row.keys())
    val_col = None
    for k in keys:
        v = sample_row.get(k)
        if isinstance(v, bool):   # bool is a subclass of int — skip
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
        v = sample_row.get(k)
        if isinstance(v, str):
            label_col = k
            break
    if label_col is None:
        # No explicit string column — fall back to the first non-value column.
        for k in keys:
            if k != val_col:
                label_col = k
                break
    return label_col, val_col


def _rebuild_chart_with_fresh_data(chart: dict[str, Any], fresh: dict[str, Any]) -> dict[str, Any]:
    """Replace the chart's series data with freshly-fetched rows.

    Preserves the saved chart's shape (type, title, colors, axes). Only
    `series[0].data` and `xAxis.categories` (or pie slice shape) get swapped
    for the new rows. If we cannot find sensible label + value columns in the
    fresh result, the chart is returned unchanged and flagged in the subtitle.
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
        return new_chart

    label_col, val_col = _pick_label_and_value_cols(rows[0])
    if label_col is None or val_col is None:
        return new_chart

    # Normalize the chart.type — it can live under `chart.type` or top-level `type`.
    chart_type = (
        (new_chart.get("chart") or {}).get("type")
        or new_chart.get("type")
        or ""
    ).lower()
    series = new_chart.get("series") or []

    # Pie / donut charts need [{name, y}, ...] — check this FIRST, before the
    # generic branch would overwrite series[0].data with a bare number array.
    if chart_type in ("pie", "donut") and series and isinstance(series[0], dict):
        series[0]["data"] = [
            {"name": str(r.get(label_col, "")), "y": r.get(val_col)}
            for r in rows
        ]
        new_chart["series"] = series
        return new_chart

    # Cartesian charts (bar, column, line, area, spline, scatter…): update
    # xAxis.categories and series[0].data in lockstep.
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

    return new_chart


@router.post("/templates/{template_id}/run", summary="Re-run all scripts and re-render charts")
def run_template(template_id: str):
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail="Template not found")

    selections = row.get("selections") or []
    if isinstance(selections, str):
        try:
            selections = json.loads(selections)
        except Exception:
            selections = []

    # Respect the stored drag-drop order. Fall back to list index if a
    # selection predates the `position` field so older templates still work.
    selections = sorted(
        selections,
        key=lambda s: int(s.get("position", 0) if s.get("position") is not None
                          else selections.index(s)),
    )

    rendered_charts: list[dict[str, Any]] = []
    script_reports: list[dict[str, Any]] = []

    for sel in selections:
        t0 = time.perf_counter()
        chart = sel.get("chart", {}) or {}
        evidence = sel.get("evidence") or {}
        code = evidence.get("code", "") if isinstance(evidence, dict) else ""

        try:
            fresh = _run_one_script(code)
        except Exception as e:
            fresh = {"status": "error", "error": str(e), "result": None}

        elapsed_ms = (time.perf_counter() - t0) * 1000

        status = fresh.get("status", "success") if isinstance(fresh, dict) else "success"
        script_reports.append({
            "query_id": sel.get("query_id"),
            "chart_index": sel.get("chart_index"),
            "status": status,
            "error": fresh.get("error", "") if isinstance(fresh, dict) else "",
            "elapsed_ms": round(elapsed_ms, 2),
        })

        if status == "success":
            rebuilt = _rebuild_chart_with_fresh_data(chart, fresh)

            # Refresh the insight block using the original as a style template.
            original_insight = chart.get("insight") if isinstance(chart, dict) else ""
            if original_insight:
                chart_title = ""
                if isinstance(rebuilt.get("title"), dict):
                    chart_title = rebuilt["title"].get("text", "") or ""
                elif isinstance(rebuilt.get("title"), str):
                    chart_title = rebuilt["title"]
                fresh_result = fresh.get("result") if isinstance(fresh, dict) else None
                new_insight = refresh_insight(chart_title, original_insight, fresh_result)
                if new_insight:
                    rebuilt["insight"] = new_insight
                    history = rebuilt.get("_insight_history") or []
                    history.append({
                        "original_insight": original_insight,
                        "refreshed_at": time.time(),
                    })
                    rebuilt["_insight_history"] = history

            rendered_charts.append(rebuilt)
        else:
            # Fallback to the saved chart; mark it in the subtitle so the UI can flag it.
            stale = json.loads(json.dumps(chart, default=str))
            sub = stale.get("subtitle") if isinstance(stale.get("subtitle"), dict) else {}
            sub["text"] = ((sub.get("text", "") or "") + "  (refresh failed — showing saved data)").strip()
            stale["subtitle"] = sub
            rendered_charts.append(stale)

    rendered = {
        "charts": rendered_charts,
        "script_reports": script_reports,
        "rendered_at": time.time(),
    }
    db_service.update_template_render(template_id, rendered)
    return {"template_id": template_id, **rendered}
