"""
Chart edit API — apply a natural-language styling edit to a generated chart
and persist the result on the query row so the edit survives refresh / restart.

POST /api/v1/charts/edit
  body: { "query_id": "...", "chart_index": 0, "instruction": "make the bars red" }
  returns: { "query_id": "...", "chart_index": 0, "chart": {...patched} }

GET /api/v1/charts/edits?query_id=...&chart_index=0
  returns the edit history for that chart.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from services import db_service
from services.chart_edit import apply_chart_edit

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ChartEdits"])


class ChartEditIn(BaseModel):
    query_id: str
    chart_index: int = Field(..., ge=0)
    instruction: str


@router.post("/charts/edit", summary="Apply a natural-language edit to a generated chart")
def edit_chart(payload: ChartEditIn):
    charts = db_service.get_query_charts(payload.query_id)
    if charts is None:
        raise HTTPException(status_code=404, detail="Query not found")
    if payload.chart_index >= len(charts):
        raise HTTPException(
            status_code=400,
            detail=f"chart_index {payload.chart_index} out of range (have {len(charts)} charts)",
        )

    current_chart = charts[payload.chart_index] or {}

    try:
        patched = apply_chart_edit(current_chart, payload.instruction)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Chart edit failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Chart edit failed: {e}")

    ok = db_service.update_query_chart_at(payload.query_id, payload.chart_index, patched)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to persist edited chart")

    db_service.log_chart_edit(
        payload.query_id, payload.chart_index, payload.instruction, patched
    )

    # Propagate the new chart content into every canvas draft and every
    # finalized template that carries this chart, so edits made anywhere
    # (chat, canvas quick-edit, per-slot edit) flow through to all views
    # without a manual re-finalize.
    try:
        propagation = db_service.propagate_chart_content(
            payload.query_id, payload.chart_index, patched,
        )
        if propagation.get("drafts") or propagation.get("templates"):
            logger.info(
                "chart-edit propagated: drafts=%d templates=%d (query=%s, idx=%d)",
                propagation["drafts"], propagation["templates"],
                payload.query_id, payload.chart_index,
            )
    except Exception as e:
        logger.warning("chart-edit propagation failed (non-fatal): %s", e)
        propagation = {"drafts": 0, "templates": 0}

    return {
        "query_id": payload.query_id,
        "chart_index": payload.chart_index,
        "chart": patched,
        "propagation": propagation,
    }


@router.get("/charts/edits", summary="Fetch edit history for a chart")
def list_edits(
    query_id: str = Query(...),
    chart_index: int = Query(..., ge=0),
    limit: int = Query(default=20, le=100),
):
    return {
        "query_id": query_id,
        "chart_index": chart_index,
        "edits": db_service.get_chart_edit_history(query_id, chart_index, limit=limit),
    }
