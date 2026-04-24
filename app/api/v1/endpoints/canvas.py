"""
Canvas draft persistence API.

Canvas drafts are work-in-progress selections of charts that a user is
assembling before clicking Finalize. They live in Postgres so a browser
refresh (or switching machines) doesn't blow them away.

Each draft's `slots` is a JSONB array, each slot carrying an explicit
`position` (0-indexed) so the order is never ambiguous for drag-and-drop.

Endpoints:
  GET    /api/v1/canvas/drafts?user_id=&thread_id=      list drafts (optionally scoped to a thread)
  POST   /api/v1/canvas/drafts                          create a new empty draft
  GET    /api/v1/canvas/drafts/{draft_id}               fetch one draft
  PATCH  /api/v1/canvas/drafts/{draft_id}               rename, or replace the slots array
  DELETE /api/v1/canvas/drafts/{draft_id}               delete a draft
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from models.chart_types import Chart
from services import db_service
from services.canvas_export import _safe_filename, render_canvas_pdf

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Canvas"])

MAX_SLOTS_PER_DRAFT = 6


# ── Models ──────────────────────────────────────────────────────────────────

# A single example used in BOTH the Swagger preview and the runtime example.
# Edit it once here and Swagger picks the change up everywhere.
_EXAMPLE_CHART: dict[str, Any] = {
    "chart_id": "0c7e9b4a-c3d6-4d2c-9c8a-7b1d5d8f9e10",
    "chart":    {"type": "column"},
    "colors":   ["#2E86AB", "#F18F01", "#A23B72", "#3B8EA5", "#C73E1D", "#6A994E"],
    "title":    {"text": "Weekly GC Run Rate by Region"},
    "subtitle": {"text": "NTM Projects, Last 12 Weeks"},
    "xAxis":    {"categories": ["CENTRAL", "NORTHEAST", "SOUTH"], "title": {"text": "Region"}},
    "yAxis":    {"title": {"text": "Sites / week"}},
    "series":   [{"name": "Run rate", "data": [0.25, 0.08, 0.08]}],
    "legend":   {"enabled": True},
    "tooltip":  {"valueSuffix": " sites/week"},
    "plotOptions": {"column": {"dataLabels": {"enabled": True}}},
    "description": "Central region leads at 0.25 sites/week.",
    "insight":  ("Central leads with 0.25 sites/week, 3x the 0.08 rate of "
                 "Northeast and South. Run rate has held steady for the trailing "
                 "12-week window."),
    "script":   "sql = '''SELECT rgn_region, COUNT(*) FROM ...'''\nresult = run_sql(sql)",
    "sql_index": 1,
}

_EXAMPLE_SLOT: dict[str, Any] = {
    # Minimum payload — only chart + free-form (x, y, w, h) as fractions
    # of the canvas viewport. A slot is just "this chart at this spot on
    # the canvas".  Provenance (which query produced the chart, the
    # original NL question) lives ON the chart row, not on the slot.
    "chart": _EXAMPLE_CHART,
    "x": 0.0, "y": 0.0, "w": 0.5, "h": 0.4,
}


class CanvasSlot(BaseModel):
    """One chart slot inside a canvas draft.

    Coordinates are FREE-FORM — whatever the UI sends is stored verbatim.
    No bounds, no clamping, no rounding. Conventions the UI is expected
    to follow (but server does NOT enforce):

      * `x`, `y` — top-left of the tile (fractions of the canvas, pixels,
                   whatever scale the UI uses; server doesn't care)
      * `w`, `h` — width / height of the tile, same scale as x/y

    The ONLY required field is `chart`. If layout is omitted the server
    drops the slot at (0, max_y_so_far) with default size 0.5 × 0.4.
    """
    chart:    Chart = Field(..., description="Full chart object — strict per-type schema (cartesian or pie). chart_id, script, insight, colors are required.")

    x:        float | None = Field(default=None, description="Left edge of the tile (UI's coordinate system)")
    y:        float | None = Field(default=None, description="Top edge of the tile")
    w:        float | None = Field(default=None, description="Tile width")
    h:        float | None = Field(default=None, description="Tile height")
    position: int   | None = Field(default=None, description="Legacy list order; server re-derives from (y, x)")

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={"example": _EXAMPLE_SLOT},
    )


class CanvasDraftCreate(BaseModel):
    user_id:      str
    name:         str
    project_type: str = ""


class CanvasDraftPatch(BaseModel):
    """Partial update — send any subset of `name` / `slots`.

    `name` is OPTIONAL: omit it (or send null) to keep the draft's existing
    name. Only include it when you're actually renaming.

    `slots` is OPTIONAL: omit it (or send null) to keep the existing slots.
    When provided, it REPLACES the entire slot list (slots not in the
    payload are removed).
    """
    name:  str | None = Field(
        default=None,
        description="Omit to keep the existing name. Only set when renaming.",
    )
    slots: list[CanvasSlot] | None = Field(
        default=None,
        description="Omit to keep existing slots. When set, REPLACES the slot list (max 6).",
    )

    model_config = ConfigDict(
        extra="ignore",
        # Swagger example: only `slots` — the most common PATCH (add/move tiles).
        # Renaming alone is rare; the docstring covers it.
        json_schema_extra={"example": {"slots": [_EXAMPLE_SLOT]}},
    )


# ── Helpers ─────────────────────────────────────────────────────────────────
#
# Free-form layout — server stores x/y/w/h verbatim. No clamping, no bounds
# checks. The UI is fully in charge of layout.

DEFAULT_W = 0.5     # only used when the UI omits a slot's size entirely
DEFAULT_H = 0.4


def _has_layout(s: dict) -> bool:
    return all(s.get(k) is not None for k in ("x", "y", "w", "h"))


def _assign_default_layout(slot: dict, already_placed: list[dict]) -> dict:
    """Fallback when the UI sends a slot with no x/y/w/h — drop it at
    (0, max_y) so it sits below everything that's already placed."""
    slot["w"] = float(slot.get("w") if slot.get("w") is not None else DEFAULT_W)
    slot["h"] = float(slot.get("h") if slot.get("h") is not None else DEFAULT_H)
    slot["x"] = float(slot.get("x") if slot.get("x") is not None else 0.0)
    if slot.get("y") is None:
        max_y = max(
            (float(p["y"]) + float(p["h"]) for p in already_placed),
            default=0.0,
        )
        slot["y"] = max_y
    else:
        slot["y"] = float(slot["y"])
    return slot


def _normalise_positions(slots: list[dict]) -> list[dict]:
    """Coerce x/y/w/h to floats and renumber `position` from 2D order
    (top-to-bottom, left-to-right). NO clamping — values pass through
    exactly as the UI sent them."""
    placed: list[dict] = []
    unplaced: list[dict] = []

    for s in slots:
        if _has_layout(s):
            s["x"] = float(s["x"]); s["y"] = float(s["y"])
            s["w"] = float(s["w"]); s["h"] = float(s["h"])
            placed.append(s)
        else:
            unplaced.append(s)

    for s in unplaced:
        _assign_default_layout(s, placed)
        placed.append(s)

    placed.sort(key=lambda s: (float(s["y"]), float(s["x"])))
    for i, s in enumerate(placed):
        s["position"] = i
    return placed


# ── Endpoints ───────────────────────────────────────────────────────────────

@router.get(
    "/canvas/drafts",
    summary="List canvas drafts for a user (each with slots + reconstructed charts inlined)",
)
def list_drafts(
    user_id: str = Query(..., description="Owner of the drafts"),
    limit:   int = Query(default=50, le=200),
):
    """One round-trip — every draft plus every slot (with the chart fully
    reconstructed from `reporting_agent_charts`). Canvas drafts are
    USER-scoped — a single canvas can mix charts the user collected from
    any number of chat threads. There is no thread filter."""
    drafts = db_service.list_canvas_drafts(user_id, limit=limit)
    for d in drafts:
        d["slots"] = db_service.list_canvas_slots(d["draft_id"])
    return {"drafts": drafts}


@router.post("/canvas/drafts", summary="Create a new empty canvas draft")
def create_draft(payload: CanvasDraftCreate):
    draft_id = str(uuid.uuid4())
    db_service.create_canvas_draft(
        draft_id=draft_id,
        user_id=payload.user_id,
        name=payload.name.strip() or "Untitled report",
        project_type=payload.project_type,
    )
    row = db_service.get_canvas_draft(draft_id)
    if not row:
        raise HTTPException(status_code=500, detail="Draft insert succeeded but readback failed.")
    return row


@router.get("/canvas/drafts/{draft_id}", summary="Fetch one canvas draft")
def get_draft(draft_id: str):
    row = db_service.get_canvas_draft(draft_id)
    if not row:
        raise HTTPException(status_code=404, detail="Draft not found")
    return row


@router.patch("/canvas/drafts/{draft_id}", summary="Rename and/or replace slots")
def patch_draft(draft_id: str, payload: CanvasDraftPatch):
    """Rename and/or replace the slot list. Each incoming slot's chart is
    CLONED into its own chart_id row at add-time so subsequent NL edits
    on the canvas don't leak back into the chat or other canvases.
    """
    existing = db_service.get_canvas_draft(draft_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Draft not found")

    if payload.name is not None:
        db_service.rename_canvas_draft(draft_id, payload.name.strip() or "Untitled report")

    if payload.slots is not None:
        if len(payload.slots) > MAX_SLOTS_PER_DRAFT:
            raise HTTPException(
                status_code=400,
                detail=f"Max {MAX_SLOTS_PER_DRAFT} slots per draft.",
            )

        # Build the slot specs the DB layer wants. Auto-place layout for
        # any slot the client didn't position, renumber `position` from the
        # 2D order so list and grid views agree.
        raw = [s.model_dump() for s in payload.slots]
        normalised = _normalise_positions(raw)

        # CLONE chart_ids for any slot pointing at a chart_id we don't already
        # own. The first time the user adds a chart from chat → canvas, the
        # original chart_id is replaced with a fresh clone so edits on this
        # canvas only touch the clone (not the chat or another canvas).
        existing_slots = db_service.list_canvas_slots(draft_id)
        owned_chart_ids = {s["chart_id"] for s in existing_slots}
        slot_specs: list[dict] = []
        for s in normalised:
            chart_obj = s.get("chart") or {}
            src_chart_id = chart_obj.get("chart_id")
            if not src_chart_id:
                raise HTTPException(status_code=422, detail="slot.chart.chart_id is required")

            # If this chart_id is already a slot in this draft, reuse it
            # (move/resize, no clone). Otherwise clone-on-add.
            if src_chart_id in owned_chart_ids:
                slot_chart_id = src_chart_id
            else:
                slot_chart_id = str(uuid.uuid4())
                if not db_service.clone_chart(src_chart_id, slot_chart_id):
                    # Source chart not in DB yet (test path / direct API use).
                    # Fall back to reusing the source id and persisting it via save_chart.
                    slot_chart_id = src_chart_id

            slot_specs.append({
                "chart_id": slot_chart_id,
                "x": s["x"], "y": s["y"], "w": s["w"], "h": s["h"], "position": s["position"],
            })

        db_service.replace_canvas_slots(draft_id, slot_specs)

        # Mirror into any linked template (selections == slots, by chart_id).
        try:
            n = db_service.sync_draft_to_template(draft_id)
            if n:
                logger.info("synced canvas → %d template(s) linked to draft %s", n, draft_id)
        except Exception as e:
            logger.warning("template sync failed for draft %s: %s", draft_id, e)

    return db_service.get_canvas_draft(draft_id)


@router.delete("/canvas/drafts/{draft_id}", summary="Delete a canvas draft")
def delete_draft(draft_id: str):
    if not db_service.get_canvas_draft(draft_id):
        raise HTTPException(status_code=404, detail="Draft not found")
    ok = db_service.delete_canvas_draft(draft_id)
    if not ok:
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"draft_id": draft_id, "deleted": True}


def _load_owned_draft(draft_id: str, user_id: str) -> dict:
    """Fetch a draft (with slots) and verify it belongs to user_id.

    Raises 404 if the draft doesn't exist; 403 if it belongs to someone else.
    """
    draft = db_service.get_canvas_draft(draft_id)
    if not draft:
        raise HTTPException(status_code=404, detail=f"Draft {draft_id} not found")
    if draft.get("user_id") != user_id:
        raise HTTPException(
            status_code=403,
            detail=f"Draft {draft_id} does not belong to user {user_id}",
        )
    return draft


@router.get(
    "/canvas/drafts/{draft_id}/download.pdf",
    summary="Download the canvas as a PDF (browser saves the file directly)",
    response_class=Response,
)
def download_canvas_pdf(
    draft_id: str,
    user_id: str = Query(..., description="Owner — server verifies the draft belongs to this user"),
):
    """Render the draft as a single PDF page (landscape A4) preserving the
    saved (x/y/w/h) tile layout. Charts drawn server-side (matplotlib);
    each tile shows the chart with the insight underneath.

    Returns the PDF bytes directly with a `Content-Disposition: attachment`
    header — the browser downloads it as a normal file. No base64 wrapping.
    """
    draft = _load_owned_draft(draft_id, user_id)
    title = draft.get("name") or "Canvas Report"
    pdf   = render_canvas_pdf(title, draft.get("slots") or [])
    fname = f"{_safe_filename(title)}.pdf"
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get(
    "/canvas/drafts/{draft_id}/base64",
    summary="Same PDF as /download.pdf but returned as a base64-encoded JSON payload",
)
def canvas_pdf_base64(
    draft_id: str,
    user_id: str = Query(..., description="Owner — server verifies the draft belongs to this user"),
):
    """Identical PDF pipeline as `/download.pdf` (one chart per page,
    matplotlib-rendered charts, insight callout per chart). Differs only
    in the transport: returns a JSON payload with the PDF base64-encoded
    inside, for callers that prefer to decode client-side (e.g. embed in
    a data-URL or persist to a CMS that expects base64 blobs).

    Response shape:
      {
        "draft_id":       "<uuid>",
        "filename":       "<title>.pdf",
        "mime_type":      "application/pdf",
        "content_base64": "<base64 of the pdf bytes>"
      }
    """
    import base64 as _b64
    draft = _load_owned_draft(draft_id, user_id)
    title = draft.get("name") or "Canvas Report"
    pdf   = render_canvas_pdf(title, draft.get("slots") or [])
    return {
        "draft_id":       draft_id,
        "filename":       f"{_safe_filename(title)}.pdf",
        "mime_type":      "application/pdf",
        "content_base64": _b64.b64encode(pdf).decode("ascii"),
    }
