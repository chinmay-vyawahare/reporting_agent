"""
Pydantic view-models for the Streamlit UI.

These mirror the API's response shapes — using the same per-type chart union
the backend defines (`models.chart_types.Chart`) so a chart object that
passes the API server-side validation will pass here too. No hand-rolled dict
fiddling in the views.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from models.chart_types import Chart    # the strict per-type union (cartesian | pie)


# ── Chat / threads ──────────────────────────────────────────────────────────

class Thread(BaseModel):
    thread_id: str
    user_id:   str
    title:     str | None = None
    project_type: str | None = None
    created_at: Any | None = None
    updated_at: Any | None = None
    model_config = ConfigDict(extra="ignore")


class Query(BaseModel):
    query_id:       str
    user_id:        str
    thread_id:      str | None = None
    original_query: str
    project_type:   str | None = None
    rationale:      str | None = None
    started_at:     Any | None = None
    completed_at:   Any | None = None
    model_config = ConfigDict(extra="ignore")


# ── Chart row (from /charts?... endpoints — base columns + reconstructed chart) ──

class ChartRow(BaseModel):
    """One row from `/charts?query_id=` / `/charts/{id}`.

    The actual API-shaped chart object lives at `chart` — that's what the UI
    passes around (canvas slot, edits). The other columns are denormalised
    metadata for filtering.
    """
    chart_id:    str
    query_id:    str
    user_id:     str
    thread_id:   str | None = None
    chart_type:  str | None = None
    chart_index: int = 0
    title_text:  str | None = None
    subtitle_text: str | None = None
    description: str | None = None
    insight:     str | None = None
    script:      str
    sql_index:   int | None = None
    chart:       Chart           # validated against the strict per-type union
    created_at:  Any | None = None
    updated_at:  Any | None = None
    model_config = ConfigDict(extra="ignore")


# ── Canvas slot / draft ────────────────────────────────────────────────────

class CanvasSlot(BaseModel):
    """One slot inside a draft as the API returns it."""
    slot_id:        str | None = None
    draft_id:       str | None = None
    chart_id:       str
    query_id:       str | None = None
    original_query: str | None = None
    chart:          Chart        # strict per-type
    x:              int
    y:              int
    w:              int
    h:              int
    position:       int
    created_at:     Any | None = None
    model_config = ConfigDict(extra="ignore")


class CanvasDraft(BaseModel):
    draft_id:     str
    user_id:      str
    name:         str
    project_type: str | None = None
    slots:        list[CanvasSlot] = Field(default_factory=list)
    created_at:   Any | None = None
    updated_at:   Any | None = None
    model_config = ConfigDict(extra="ignore")


# ── Templates ──────────────────────────────────────────────────────────────

class TemplateSelection(BaseModel):
    selection_id:   str | None = None
    template_id:    str | None = None
    chart_id:       str
    query_id:       str | None = None
    original_query: str | None = None
    chart:          Chart
    x:              int
    y:              int
    w:              int
    h:              int
    position:       int
    created_at:     Any | None = None
    model_config = ConfigDict(extra="ignore")


class Template(BaseModel):
    template_id:     str
    user_id:         str
    title:           str
    project_type:    str | None = None
    source_draft_id: str | None = None
    selections:      list[TemplateSelection] = Field(default_factory=list)
    created_at:      Any | None = None
    last_run_at:     Any | None = None
    model_config = ConfigDict(extra="ignore")


class TemplateActionResponse(BaseModel):
    template_id: str
    action:      str         # "created" | "updated"


class TemplateRunResult(BaseModel):
    template_id:    str
    selections:     list[TemplateSelection] = Field(default_factory=list)
    script_reports: list[dict[str, Any]] = Field(default_factory=list)
    rendered_at:    float | None = None
    model_config = ConfigDict(extra="ignore")


class ChartEditResponse(BaseModel):
    chart_id: str
    chart:    Chart
