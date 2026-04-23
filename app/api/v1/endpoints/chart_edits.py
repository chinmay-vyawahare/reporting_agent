"""
Chart edit API — apply a natural-language styling edit to a saved chart.

POST /api/v1/charts/edit
  body: { "chart_id": "...", "instruction": "make the bars red" }
  returns: { "chart_id": "...", "chart": {...patched} }

The edit is **canvas-scoped by construction**: every chart added to a canvas
gets its own cloned chart_id (see canvas PATCH endpoint), so editing a
chart_id only mutates that one row — it never leaks back into the chat or
into another canvas. The linked template's selection points at the SAME
chart_id, so canvas edits flow into the template automatically.

GET /api/v1/charts/edits?chart_id=...
  returns the edit history for that chart (audit log: instruction + when).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from services import db_service
from services.chart_edit import apply_chart_edit

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ChartEdits"])


class ChartEditIn(BaseModel):
    chart_id:    str = Field(..., description="Stable chart UUID from /report/stream complete.charts[i].chart_id")
    instruction: str = Field(..., min_length=1, description="Natural-language edit, e.g. 'make the bars red'")

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={"example": {
            "chart_id":    "0c7e9b4a-c3d6-4d2c-9c8a-7b1d5d8f9e10",
            "instruction": "make the bars red and add a 0.5 horizontal line at y=10",
        }},
    )


@router.get("/charts", summary="List charts (filter by query_id, user_id, or thread_id)")
def list_charts(
    query_id:  str | None = Query(default=None, description="Charts that belong to one query"),
    user_id:   str | None = Query(default=None, description="All charts a user has produced"),
    thread_id: str | None = Query(default=None, description="All charts in a chat thread"),
    limit:     int        = Query(default=100, le=500),
):
    """Return chart rows from `reporting_agent_charts`.

    Supply exactly one of `query_id`, `user_id`, or `thread_id`. The response
    inlines `chart_config` as `chart` so the shape matches /report/stream.
    """
    set_filters = sum(1 for x in (query_id, user_id, thread_id) if x)
    if set_filters != 1:
        raise HTTPException(
            status_code=400,
            detail="Pass exactly one of query_id, user_id, or thread_id.",
        )
    if query_id:
        return {"charts": db_service.get_charts_for_query(query_id)}
    if user_id:
        return {"charts": db_service.get_charts_by_user(user_id, limit=limit)}
    return {"charts": db_service.get_charts_by_thread(thread_id, limit=limit)}


@router.post("/charts/edit", summary="Apply a natural-language edit to a saved chart")
def edit_chart(payload: ChartEditIn):
    row = db_service.get_chart(payload.chart_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Chart {payload.chart_id} not found")

    current_chart = row.get("chart") or {}
    # Make sure the patched chart keeps its identity — chart_id stays stable.
    current_chart.setdefault("chart_id", payload.chart_id)

    try:
        patched = apply_chart_edit(current_chart, payload.instruction)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Chart edit failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Chart edit failed: {e}")

    # Belt-and-braces: the LLM patch must not strip identity or runnability.
    # Carry the chart_id and (if the LLM dropped it) the script forward.
    patched["chart_id"] = payload.chart_id
    if not patched.get("script") and current_chart.get("script"):
        patched["script"] = current_chart["script"]
    if patched.get("sql_index") is None and current_chart.get("sql_index") is not None:
        patched["sql_index"] = current_chart["sql_index"]

    if not db_service.update_chart_by_id(payload.chart_id, patched):
        raise HTTPException(status_code=500, detail="Failed to persist edited chart")

    db_service.log_chart_edit(payload.chart_id, payload.instruction)

    # No propagation — each canvas owns its own cloned chart_id, so editing
    # one chart_id only ever mutates that one row. The linked template's
    # selection points at the same chart_id, so canvas edits flow into the
    # template by reference, not by copying.
    return {
        "chart_id": payload.chart_id,
        "chart":    patched,
    }


@router.get("/charts/edits", summary="Fetch edit history for a chart")
def list_edits(
    chart_id: str = Query(..., description="Stable chart UUID"),
    limit: int = Query(default=20, le=100),
):
    return {
        "chart_id": chart_id,
        "edits":    db_service.get_chart_edit_history(chart_id, limit=limit),
    }


# IMPORTANT: this catch-all path-param route MUST be registered last.
# FastAPI matches in order, and `/charts/{chart_id}` would otherwise
# swallow `/charts/edit` and `/charts/edits` (treating "edit"/"edits"
# as a chart_id and 404-ing). Keep it at the bottom of the file.
@router.get("/charts/{chart_id}", summary="Fetch one chart by id")
def get_one_chart(chart_id: str):
    row = db_service.get_chart(chart_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Chart {chart_id} not found")
    return row
