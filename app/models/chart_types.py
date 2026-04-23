"""
Strict per-type chart schemas.

The graph agent / canvas / template endpoints all use the discriminated
union `Chart` defined here so:

  * Each chart type has a KNOWN, FIXED set of keys — the frontend never
    sees a surprise shape.
  * Pydantic validates every chart on the way in and on the way out.
  * If the LLM ever drops or renames a field, validation fails and the
    chart agent retries with a clear error message.

Two shapes today (covering 8 Highcharts types):

  * `CartesianChart` — column, bar, line, area, spline, areaspline, scatter
      → series.data is a flat list of numbers, xAxis.categories is the labels
  * `PieChart`       — pie, donut
      → series.data is a list of {name, y} objects, no xAxis

Add a new shape by:
  1. Adding a model below with its own `chart.type` Literal.
  2. Including it in the `Chart` union.
  3. Updating the graph agent prompt with a worked example for that type.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag
from pydantic.types import StringConstraints

# A non-empty trimmed string used wherever we accept any text.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


# ── Common building blocks (loose where Highcharts is genuinely arbitrary) ──

class TextBlock(BaseModel):
    """Highcharts-style `{"text": "..."}` wrapper used for title/subtitle."""
    text: str = Field(..., description="Display text")
    model_config = ConfigDict(extra="allow")


class AxisTitle(BaseModel):
    text: str | None = Field(default=None)
    model_config = ConfigDict(extra="allow")


class CategoricalXAxis(BaseModel):
    """X axis for cartesian charts — categorical labels."""
    categories: list[str] = Field(..., min_length=1, description="Tick labels in order")
    title: AxisTitle | None = Field(default=None)
    model_config = ConfigDict(extra="allow")


class NumericYAxis(BaseModel):
    title: AxisTitle | None = Field(default=None)
    min: float | int | None = Field(default=None)
    max: float | int | None = Field(default=None)
    model_config = ConfigDict(extra="allow")


# ── Series shapes ───────────────────────────────────────────────────────────

class CartesianSeries(BaseModel):
    """One data series for column/bar/line/area/spline/areaspline/scatter."""
    name: NonEmptyStr = Field(..., description="Series label shown in legend & tooltip")
    data: list[float | int | None] = Field(
        ..., min_length=1,
        description="Plain list of numeric values; one per xAxis category.",
    )
    color: str | None = Field(default=None)
    type: Literal["column", "bar", "line", "area", "spline", "areaspline", "scatter"] | None = Field(
        default=None, description="Per-series type override (rare). Keep null for normal use."
    )
    model_config = ConfigDict(extra="allow")


class PieSlice(BaseModel):
    """One slice in a pie/donut chart."""
    name: NonEmptyStr = Field(..., description="Slice label")
    y: float | int = Field(..., description="Slice value (sum determines proportions)")
    color: str | None = Field(default=None)
    model_config = ConfigDict(extra="allow")


class PieSeries(BaseModel):
    name: NonEmptyStr = Field(..., description="Series label (used in tooltip header)")
    data: list[PieSlice] = Field(..., min_length=1, description="Slice list")
    model_config = ConfigDict(extra="allow")


# ── Common chart-level fields (the metadata every chart carries) ────────────

class _ChartMixin(BaseModel):
    """Fields every chart object carries, regardless of type.

    Strict on the metadata (chart_id, script, insight, colors), tolerant on
    the visual extras (legend, tooltip, plotOptions) since Highcharts has
    hundreds of options nobody wants to enumerate.
    """
    chart_id:    NonEmptyStr = Field(..., description="Stable UUID assigned by the graph agent")
    title:       TextBlock = Field(..., description="`{\"text\": \"…\"}`")
    subtitle:    TextBlock | None = Field(default=None)
    colors:      list[str] = Field(..., min_length=1, description="Series color palette")
    description: str = Field(default="", description="One-line takeaway")
    insight:     str = Field(..., min_length=1, description="2-3 line plain-string insight")
    script:      NonEmptyStr = Field(..., description="Python+SQL that produced this chart")
    sql_index:   int | None = Field(default=None)

    # Loose visual extras — Highcharts options are arbitrary, so we don't
    # try to enumerate them. They round-trip unchanged.
    legend:      dict[str, Any] | None = Field(default=None)
    tooltip:     dict[str, Any] | None = Field(default=None)
    plotOptions: dict[str, Any] | None = Field(default=None)


# ── Chart-type bodies ───────────────────────────────────────────────────────

CARTESIAN_TYPES = ("column", "bar", "line", "area", "spline", "areaspline", "scatter")
PIE_TYPES = ("pie", "donut")


class _CartesianChartType(BaseModel):
    type: Literal["column", "bar", "line", "area", "spline", "areaspline", "scatter"]
    model_config = ConfigDict(extra="allow")


class _PieChartType(BaseModel):
    type: Literal["pie", "donut"]
    model_config = ConfigDict(extra="allow")


class CartesianChart(_ChartMixin):
    """column / bar / line / area / spline / areaspline / scatter."""
    chart:  _CartesianChartType = Field(..., description="`{\"type\": \"column\" | \"bar\" | …}`")
    xAxis:  CategoricalXAxis = Field(..., description="Categorical x axis")
    yAxis:  NumericYAxis | None = Field(default=None)
    series: list[CartesianSeries] = Field(..., min_length=1, description="One or more cartesian series")

    model_config = ConfigDict(extra="ignore")


class PieChart(_ChartMixin):
    """pie / donut — one series of {name, y} slices."""
    chart:  _PieChartType = Field(..., description="`{\"type\": \"pie\" | \"donut\"}`")
    series: list[PieSeries] = Field(..., min_length=1, max_length=1, description="Exactly one pie series")
    # No xAxis on a pie.

    model_config = ConfigDict(extra="ignore")


# ── The discriminated union the rest of the codebase depends on ─────────────
# Pydantic picks the right subtype based on `chart.type`:
#   "column"|"bar"|"line"|"area"|"spline"|"areaspline"|"scatter"  → CartesianChart
#   "pie" | "donut"                                                → PieChart
# Anything else fails with a clear field-level ValidationError.

def _chart_kind(v: Any) -> str | None:
    """Pull `chart.type` from either a raw dict or a parsed model."""
    if isinstance(v, dict):
        ch = v.get("chart")
        return ch.get("type") if isinstance(ch, dict) else None
    return getattr(getattr(v, "chart", None), "type", None)


def _chart_tag(v: Any) -> str | None:
    """Map chart.type → the union tag Pydantic dispatches on."""
    t = _chart_kind(v)
    if t in CARTESIAN_TYPES:
        return "cartesian"
    if t in PIE_TYPES:
        return "pie"
    return None


# THE chart type. Use this as the field type for any place a chart enters
# or leaves the system; Pydantic does the validation + dispatch for you.
Chart = Annotated[
    Union[
        Annotated[CartesianChart, Tag("cartesian")],
        Annotated[PieChart,       Tag("pie")],
    ],
    Discriminator(_chart_tag),
]


def parse_chart(payload: Any) -> CartesianChart | PieChart:
    """One-shot validate-and-dispatch helper for ad-hoc callers (e.g. the
    graph agent's own validator). Equivalent to `Chart`-typed Pydantic
    validation but raises `ValueError` rather than `ValidationError` so
    error messages stay short.
    """
    t = _chart_kind(payload)
    if t in CARTESIAN_TYPES:
        return CartesianChart.model_validate(payload)
    if t in PIE_TYPES:
        return PieChart.model_validate(payload)
    allowed = list(CARTESIAN_TYPES) + list(PIE_TYPES)
    raise ValueError(f"Unknown chart.type {t!r}. Allowed: {allowed}")
