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
from pydantic import BaseModel, Field

from services import db_service

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Canvas"])

MAX_SLOTS_PER_DRAFT = 6


# ── Models ──────────────────────────────────────────────────────────────────

class CanvasSlot(BaseModel):
    """One chart slot inside a draft.

    Positioning (2D canvas):
      * `x`, `y`, `w`, `h` describe the chart's position on a 12-column grid.
        - `x`  column offset  (0..11)
        - `y`  row offset     (0..N)
        - `w`  width  in cols (1..12)
        - `h`  height in rows (1..N)
      * `position` is kept for legacy list ordering (0-indexed). The server
        re-derives it from y ascending → x ascending on every write so the
        two views stay consistent.
    """
    position:       int | None = Field(default=None, ge=0)
    x:              int | None = Field(default=None, ge=0, le=11)
    y:              int | None = Field(default=None, ge=0)
    w:              int | None = Field(default=None, ge=1, le=12)
    h:              int | None = Field(default=None, ge=1)
    query_id:       str
    chart_index:    int = Field(..., ge=0)
    original_query: str = ""
    chart:          dict[str, Any]
    evidence:       dict[str, Any] | None = None
    # Accept any extra fields the client may add (e.g. `source`) without
    # rejecting — they get persisted as-is inside the JSONB.
    class Config:
        extra = "allow"


class CanvasDraftCreate(BaseModel):
    user_id:      str
    username:     str
    thread_id:    str | None = None
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
    user_id: str = Query(...),
    thread_id: str | None = Query(default=None),
    limit: int = Query(default=50, le=200),
):
    rows = db_service.list_canvas_drafts(user_id, thread_id=thread_id, limit=limit)
    return {"drafts": rows}


@router.post("/canvas/drafts", summary="Create a new empty canvas draft")
def create_draft(payload: CanvasDraftCreate):
    draft_id = str(uuid.uuid4())
    db_service.create_canvas_draft(
        draft_id=draft_id,
        user_id=payload.user_id,
        username=payload.username,
        thread_id=payload.thread_id,
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
    existing = db_service.get_canvas_draft(draft_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Draft not found")

    slots_payload = None
    if payload.slots is not None:
        if len(payload.slots) > MAX_SLOTS_PER_DRAFT:
            raise HTTPException(
                status_code=400,
                detail=f"Max {MAX_SLOTS_PER_DRAFT} slots per draft.",
            )
        raw = [s.dict() for s in payload.slots]
        slots_payload = _normalise_positions(raw)

    db_service.update_canvas_draft(
        draft_id=draft_id,
        name=payload.name.strip() if payload.name is not None else None,
        slots=slots_payload,
    )

    # Live layout sync: any template finalized from this draft inherits the
    # new x/y/w/h for matching charts, so moves on the canvas flow into the
    # saved report without having to re-finalize.
    if slots_payload is not None:
        try:
            n = db_service.propagate_draft_layout(draft_id, slots_payload)
            if n:
                logger.info("propagated layout to %d template(s) linked to draft %s",
                            n, draft_id)
        except Exception as e:
            logger.warning("layout propagation failed for draft %s: %s", draft_id, e)

    return db_service.get_canvas_draft(draft_id)


@router.delete("/canvas/drafts/{draft_id}", summary="Delete a canvas draft")
def delete_draft(draft_id: str):
    if not db_service.get_canvas_draft(draft_id):
        raise HTTPException(status_code=404, detail="Draft not found")
    ok = db_service.delete_canvas_draft(draft_id)
    if not ok:
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"draft_id": draft_id, "deleted": True}
