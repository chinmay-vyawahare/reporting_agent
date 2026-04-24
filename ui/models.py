"""
Pydantic view-models for the Streamlit UI.

These mirror the API's response shapes.

Read vs. write validation split (important for UI robustness):

  * READ-side `chart` fields are typed as `dict[str, Any]` — the UI accepts
    whatever the backend hands back. Reason: if a stored chart has slight
    schema drift (empty colors, missing insight, an older shape, a chart
    with 0 rebuilt series rows yet), the strict per-type `Chart` union
    would reject it. One bad chart then blows up the WHOLE draft/template
    fetch, so the UI silently renders as empty. That's the bug the user
    hit on refresh.

  * WRITE-side validation still uses the strict `Chart` union — enforced
    inside `ApiClient.patch_draft` / `ApiClient.upsert_template` before
    the payload leaves the UI. Bad data never reaches the server.

So reads are tolerant (survive schema drift), writes are strict (server
never sees a malformed chart).
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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
    """One Q&A turn in a chat thread.

    `charts` is the AI's chart response — fully reconstructed Highcharts
    configs in the same shape SSE `complete.charts[i]` emits. It comes
    inlined in `GET /threads/{tid}/queries` so the chat UI can render the
    whole thread in one round-trip.
    """
    query_id:       str
    user_id:        str
    thread_id:      str | None = None
    original_query: str
    project_type:   str | None = None
    rationale:      str | None = None
    charts:         list[dict[str, Any]] = Field(default_factory=list)
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
    chart:       dict[str, Any]  # tolerant on read — strict validation is on writes
    created_at:  Any | None = None
    updated_at:  Any | None = None
    model_config = ConfigDict(extra="ignore")


# ── Canvas slot / draft ────────────────────────────────────────────────────

class CanvasSlot(BaseModel):
    """One slot inside a draft as the API returns it.

    Slot = chart_id + free-form position. x/y/w/h are floats — fractions
    of the canvas viewport (or whatever scale the UI sends).
    """
    slot_id:    str | None = None
    draft_id:   str | None = None
    chart_id:   str
    chart:      dict[str, Any]   # tolerant on read
    x:          float
    y:          float
    w:          float
    h:          float
    position:   int
    created_at: Any | None = None
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
    """Same shape as CanvasSlot — chart_id + free-form position (floats)."""
    selection_id: str | None = None
    template_id:  str | None = None
    chart_id:     str
    chart:        dict[str, Any]   # tolerant on read
    x:            float
    y:            float
    w:            float
    h:            float
    position:     int
    created_at:   Any | None = None
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
    chart:    dict[str, Any]   # tolerant on read; write path validated separately
