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
from pydantic import BaseModel, ConfigDict, Field

from models.chart_types import Chart
from services import db_service

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
    "query_id":       "23237b8e-a963-459e-920a-3ffbafa1f015",
    "chart":          _EXAMPLE_CHART,   # chart.script is the script — no duplicate at slot level
    "original_query": "give the GC run rate region wise",
    "x": 0, "y": 0, "w": 6, "h": 4,
}


class CanvasSlot(BaseModel):
    """One chart slot inside a canvas draft.

    Copy each chart from the `/report/stream` complete event into a slot:

      * `query_id` ← `stream_started.query_id`
      * `chart`    ← `complete.charts[i]`   (the whole object — paste it as-is.
                                              `chart.script` is what powers re-run.)

    Layout (`x`, `y`, `w`, `h`, `position`) is optional — the server auto-places
    new tiles in the first free spot on the 12-column grid.
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
        extra="ignore",                      # → no `additionalProp1` in Swagger
        json_schema_extra={"example": _EXAMPLE_SLOT},
    )


class CanvasDraftCreate(BaseModel):
    user_id:      str
    name:         str
    project_type: str = ""


class CanvasDraftPatch(BaseModel):
    """Partial update. Send any subset of `name` / `slots`."""
    name:  str | None = None
    slots: list[CanvasSlot] | None = None


# ── Helpers ─────────────────────────────────────────────────────────────────

GRID_COLS = 12
DEFAULT_W = 6    # half-width tile (2 per row)
DEFAULT_H = 4    # ~320 px tall at 80 px row height


def _has_layout(s: dict) -> bool:
    return all(s.get(k) is not None for k in ("x", "y", "w", "h"))


def _assign_default_layout(slot: dict, already_placed: list[dict]) -> dict:
    """Pack an un-laid-out slot into the first available slot on the grid.

    We walk existing placements row-by-row and drop the new tile into the
    first gap big enough to hold DEFAULT_W × DEFAULT_H. Falling back to
    appending at the bottom if no gap fits.
    """
    slot["w"] = slot.get("w") or DEFAULT_W
    slot["h"] = slot.get("h") or DEFAULT_H
    slot["w"] = min(GRID_COLS, max(1, slot["w"]))
    slot["h"] = max(1, slot["h"])

    # Try to fit into each row from top, scanning columns.
    if already_placed:
        max_y = max((int(p["y"]) + int(p["h"])) for p in already_placed) + slot["h"] + 1
    else:
        max_y = slot["h"] + 1

    for y in range(max_y):
        for x in range(GRID_COLS - slot["w"] + 1):
            collides = False
            for p in already_placed:
                px, py, pw, ph = int(p["x"]), int(p["y"]), int(p["w"]), int(p["h"])
                if (x < px + pw and x + slot["w"] > px
                        and y < py + ph and y + slot["h"] > py):
                    collides = True
                    break
            if not collides:
                slot["x"], slot["y"] = x, y
                return slot

    # Should never reach — but fall back to bottom-left.
    slot["x"] = 0
    slot["y"] = max_y
    return slot


def _normalise_positions(slots: list[dict]) -> list[dict]:
    """Ensure every slot has an x/y/w/h layout, then renumber `position`
    from the 2D order (top-to-bottom, left-to-right) so list and grid views
    agree."""
    placed: list[dict] = []

    # First pass: accept existing layouts, defer un-placed ones.
    unplaced: list[dict] = []
    for s in slots:
        if _has_layout(s):
            # Defensive clamp
            s["x"] = max(0, min(GRID_COLS - 1, int(s["x"])))
            s["w"] = max(1, min(GRID_COLS, int(s["w"])))
            if s["x"] + s["w"] > GRID_COLS:
                s["w"] = GRID_COLS - s["x"]
            s["y"] = max(0, int(s["y"]))
            s["h"] = max(1, int(s["h"]))
            placed.append(s)
        else:
            unplaced.append(s)

    # Second pass: auto-place newcomers into the first free spot.
    for s in unplaced:
        _assign_default_layout(s, placed)
        placed.append(s)

    # Recompute `position` from 2D order (row-major, top-to-bottom).
    placed.sort(key=lambda s: (int(s["y"]), int(s["x"])))
    for i, s in enumerate(placed):
        s["position"] = i

    return placed


# ── Endpoints ───────────────────────────────────────────────────────────────

@router.get("/canvas/drafts", summary="List canvas drafts for a user")
def list_drafts(
    user_id: str = Query(..., description="Owner of the drafts"),
    limit:   int = Query(default=50, le=200),
):
    """Canvas drafts are USER-scoped — a single canvas can mix charts the
    user collected from any number of chat threads. There is no thread filter."""
    return {"drafts": db_service.list_canvas_drafts(user_id, limit=limit)}


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
                "chart_id":       slot_chart_id,
                "query_id":       s.get("query_id") or "",
                "original_query": s.get("original_query") or "",
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
