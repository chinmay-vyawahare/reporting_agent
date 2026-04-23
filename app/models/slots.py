"""
Typed Pydantic models for canvas slots and template selections.

A slot or a selection is the same idea — one chart placed on a 12-column
grid at (x, y, w, h). The two shapes are kept as separate types only so
Swagger renders the right wording per endpoint.

Every slot/selection lives as ITS OWN ROW in Postgres
(`reporting_canvas_slots`, `reporting_template_selections`) — there is no
JSONB array column anywhere in the slot/selection path.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from models.chart_types import Chart


class _SlotBase(BaseModel):
    """Common shape for canvas slots and template selections.

    `chart` is a **strict** discriminated union (see models.chart_types.Chart).
    Layout fields are optional — the auto-place logic fills them when the
    client hasn't picked a position.
    """
    query_id: str = Field(..., description="From SSE stream_started.query_id")
    chart:    Chart = Field(..., description="Validated chart object")

    original_query: str = Field(default="", description="The NL question that produced the chart")

    x:        int | None = Field(default=None, ge=0, le=11)
    y:        int | None = Field(default=None, ge=0)
    w:        int | None = Field(default=None, ge=1, le=12)
    h:        int | None = Field(default=None, ge=1)
    position: int | None = Field(default=None, ge=0)

    model_config = ConfigDict(extra="ignore")


class CanvasSlotIn(_SlotBase):
    """Inbound slot for `PATCH /canvas/drafts/{id}` (entire slot list replace)."""


class TemplateSelectionIn(_SlotBase):
    """Inbound selection for `POST /templates`. Same shape as a CanvasSlotIn."""


class SlotRow(_SlotBase):
    """The row shape every slot/selection takes once it's persisted.

    Carries the server-assigned id and the FK back to its parent (draft or
    template). Layout fields are guaranteed populated (auto-placed if the
    caller didn't specify).
    """
    slot_id:   str = Field(..., description="Server-assigned UUID for this slot row")
    parent_id: str = Field(..., description="draft_id (canvas) or template_id (template)")
    chart_id:  str = Field(..., description="Denormalised from chart.chart_id for FK lookup speed")
    x: int = Field(..., ge=0, le=11)
    y: int = Field(..., ge=0)
    w: int = Field(..., ge=1, le=12)
    h: int = Field(..., ge=1)
    position: int = Field(..., ge=0)
