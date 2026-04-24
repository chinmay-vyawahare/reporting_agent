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
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from services import db_service
from services.insight_refresh import refresh_insight
from services.canvas_export import render_canvas_pdf, _safe_filename
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

_EXAMPLE_TEMPLATE: dict[str, Any] = {
    "user_id":  "u_123",
    "draft_id": "14dfd449-d9b6-484e-858a-e3d7b36a353a",
    "title":    "Q1 NTM weekly run-rate",          # optional — defaults to draft.name
}


class TemplateIn(BaseModel):
    """Save a canvas as a template — by reference, no chart payload duplication.

    Workflow:
      1. User builds a canvas draft (POST /canvas/drafts, PATCH /canvas/drafts/{id})
      2. User finalizes → POST /templates with just {user_id, draft_id}
      3. Server pulls the draft's slots, copies them as the template's
         selections (by chart_id reference), and saves the template.

    Upsert: if a template already exists for this draft_id it is updated in
    place (no duplicates). Same draft → same template, always.
    """
    user_id:      str = Field(..., description="Owner — must match draft.user_id")
    draft_id:     str = Field(..., description="The canvas draft to save as a template")
    title:        str | None = Field(default=None, description="Display title (defaults to draft.name)")
    project_type: str | None = Field(default=None, description="NTM | AHLOB Modernization | Both (defaults to draft.project_type)")

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


# Threads endpoints live in their own router — see api/v1/endpoints/threads.py


# ── Templates ───────────────────────────────────────────────────────────────

def _selection_specs_from_canvas_slots(slots: list[dict]) -> list[dict]:
    """Project canvas slot rows onto the shape `replace_template_selections`
    consumes. Slots carry chart_id (FK to reporting_agent_charts) and layout —
    that's all a selection needs. Provenance lives on the chart row."""
    out: list[dict] = []
    for i, s in enumerate(slots):
        out.append({
            "chart_id": s["chart_id"],
            "x":        float(s.get("x") if s.get("x") is not None else 0.0),
            "y":        float(s.get("y") if s.get("y") is not None else 0.0),
            "w":        float(s.get("w") if s.get("w") is not None else 0.5),
            "h":        float(s.get("h") if s.get("h") is not None else 0.4),
            "position": int(s.get("position") if s.get("position") is not None else i),
        })
    return out


@router.post(
    "/templates",
    summary="Save a canvas as a template (by reference — upsert by draft_id, no chart payload duplication)",
)
def create_template(payload: TemplateIn):
    """Save the canvas as a template — pass just `{user_id, draft_id}`.

    Server steps:
      1. Look up the draft. 404 if missing.
      2. Verify draft.user_id == payload.user_id. 403 if mismatch.
      3. Read the draft's slots. 400 if empty.
      4. Upsert by draft_id: if a template already linked to this draft
         exists, update its title/project_type and replace its selections.
         Otherwise create a new one.
      5. Selections are copied by chart_id reference (the chart rows are
         already persisted from the canvas's clone-on-add step).
    """
    draft = db_service.get_canvas_draft(payload.draft_id)
    if not draft:
        raise HTTPException(status_code=404, detail=f"Draft {payload.draft_id} not found")
    if draft.get("user_id") != payload.user_id:
        raise HTTPException(
            status_code=403,
            detail=f"Draft {payload.draft_id} does not belong to user {payload.user_id}",
        )

    slots = draft.get("slots") or []
    if not slots:
        raise HTTPException(
            status_code=400,
            detail="Cannot create a template from an empty canvas — add at least one chart first.",
        )
    if len(slots) > MAX_CHARTS_PER_TEMPLATE:
        raise HTTPException(
            status_code=400,
            detail=f"Max {MAX_CHARTS_PER_TEMPLATE} charts per template, draft has {len(slots)}.",
        )

    title        = (payload.title or draft.get("name") or "").strip() or "Untitled report"
    project_type = payload.project_type if payload.project_type is not None else (draft.get("project_type") or "")
    sel_specs    = _selection_specs_from_canvas_slots(slots)

    existing = db_service.find_template_by_draft(payload.draft_id)
    if existing:
        template_id = existing["template_id"]
        db_service.update_template_meta(
            template_id=template_id,
            title=title,
            project_type=project_type,
            source_draft_id=payload.draft_id,
        )
        db_service.replace_template_selections(template_id, sel_specs)
        return {"template_id": template_id, "action": "updated"}

    template_id = str(uuid.uuid4())
    db_service.create_template(
        template_id=template_id,
        user_id=payload.user_id,
        title=title,
        project_type=project_type,
        source_draft_id=payload.draft_id,
    )
    db_service.replace_template_selections(template_id, sel_specs)
    return {"template_id": template_id, "action": "created"}


@router.get(
    "/templates",
    summary="List a user's templates — id + title only (use GET /templates/{id} for full)",
)
def list_templates(user_id: str = Query(...), limit: int = Query(default=50, le=200)):
    """Lightweight picker list — returns just `template_id` and `title` per row.
    Open a single template via `GET /templates/{id}?user_id=...` to fetch its
    selections + reconstructed charts.
    """
    rows = db_service.get_templates_by_user(user_id, limit=limit)
    return {
        "templates": [
            {"template_id": r["template_id"], "title": r.get("title") or ""}
            for r in rows
        ],
    }


@router.get("/templates/{template_id}", summary="Fetch one template (ownership-checked) — selections + reconstructed charts inlined")
def get_template(template_id: str, user_id: str = Query(..., description="Owner — server verifies the template belongs to this user")):
    """Returns the template plus every selection (with the chart fully
    reconstructed from `reporting_agent_charts`).

    Ownership is enforced — `user_id` must match `template.user_id`,
    otherwise 403. Use this when opening a template view.
    """
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail="Template not found")
    if row.get("user_id") != user_id:
        raise HTTPException(
            status_code=403,
            detail=f"Template {template_id} does not belong to user {user_id}",
        )
    return row


@router.delete("/templates/{template_id}", summary="Delete a template")
def delete_template(template_id: str):
    if not db_service.get_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")
    if not db_service.delete_template(template_id):
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"template_id": template_id, "deleted": True}


def _rerun_template_selections(template_row: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Re-run every selection's script in the sandbox and rebuild each chart
    with today's rows. Returns `(rendered_selections, script_reports)` in the
    same shape `POST /templates/{id}/run` returns. Shared by the JSON rerun
    endpoint and the PDF/HTML download endpoints so the refresh logic lives
    in one place.

    Each rendered selection carries layout (x/y/w/h/position) from the saved
    selection plus a `chart` rebuilt from fresh data; the chart's `insight`
    is regenerated by the LLM against the new rows.
    """
    selections = template_row.get("selections") or []
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
            "chart_id":   (chart.get("chart_id") if isinstance(chart, dict) else None),
            "status":     status,
            "error":      fresh.get("error", "") if isinstance(fresh, dict) else "",
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
            "chart_id": new_chart.get("chart_id"),
            "x":        sel.get("x"),
            "y":        sel.get("y"),
            "w":        sel.get("w"),
            "h":        sel.get("h"),
            "position": sel.get("position"),
            "chart":    new_chart,
        })

    return rendered_selections, script_reports


@router.post("/templates/{template_id}/run", summary="Re-run the template with today's data")
def run_template(template_id: str):
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail="Template not found")

    rendered_selections, script_reports = _rerun_template_selections(row)
    db_service.bump_template_last_run(template_id)
    return {
        "template_id":    template_id,
        "selections":     rendered_selections,
        "charts":         [s["chart"] for s in rendered_selections],
        "script_reports": script_reports,
        "rendered_at":    time.time(),
    }


@router.get(
    "/templates/{template_id}/download.pdf",
    summary="Re-run the template with today's data and download the result as a PDF",
    response_class=Response,
)
def download_template_pdf(
    template_id: str,
    user_id: str = Query(..., description="Owner — server verifies the template belongs to this user"),
):
    """Re-runs the template's scripts (fresh data + refreshed insights),
    then renders the result to a PDF preserving each selection's
    (x/y/w/h) free-form position. Returns the PDF bytes directly with a
    `Content-Disposition: attachment` header.

    This is the equivalent of `GET /canvas/drafts/{id}/download.pdf` for
    a saved template — same look-and-feel, but with TODAY'S data instead
    of the snapshot the template was saved with.
    """
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Template {template_id} not found")
    if row.get("user_id") != user_id:
        raise HTTPException(
            status_code=403,
            detail=f"Template {template_id} does not belong to user {user_id}",
        )

    rendered_selections, _ = _rerun_template_selections(row)
    db_service.bump_template_last_run(template_id)

    # The PDF renderer takes the same `slots` shape as a canvas — each
    # rendered selection already has chart + (x/y/w/h), which matches.
    title = row.get("title") or "Template Report"
    pdf   = render_canvas_pdf(title, rendered_selections)
    fname = f"{_safe_filename(title)}.pdf"
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get(
    "/templates/{template_id}/base64",
    summary="Same refreshed PDF as /download.pdf but returned as a base64-encoded JSON payload",
)
def template_pdf_base64(
    template_id: str,
    user_id: str = Query(..., description="Owner — server verifies the template belongs to this user"),
):
    """Identical pipeline as `/templates/{id}/download.pdf` — re-runs the
    template's scripts with today's data, rebuilds each chart + refreshed
    insight, renders to PDF preserving saved positions.

    Differs only in transport: returns a JSON payload with the PDF
    base64-encoded inside, for callers that prefer to decode client-side.

    Response shape:
      {
        "template_id":    "<uuid>",
        "filename":       "<title>.pdf",
        "mime_type":      "application/pdf",
        "content_base64": "<base64 of the pdf bytes>"
      }
    """
    import base64 as _b64
    row = db_service.get_template(template_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Template {template_id} not found")
    if row.get("user_id") != user_id:
        raise HTTPException(
            status_code=403,
            detail=f"Template {template_id} does not belong to user {user_id}",
        )

    rendered_selections, _ = _rerun_template_selections(row)
    db_service.bump_template_last_run(template_id)

    title = row.get("title") or "Template Report"
    pdf   = render_canvas_pdf(title, rendered_selections)
    return {
        "template_id":    template_id,
        "filename":       f"{_safe_filename(title)}.pdf",
        "mime_type":      "application/pdf",
        "content_base64": _b64.b64encode(pdf).decode("ascii"),
    }
