"""
Canvas → Markdown / PDF report export.

Two renderers:

  * `render_canvas_markdown(title, slots)` — Markdown source string. Each
    chart becomes a section: `## title`, then a base64-embedded image,
    then a `> **Insight:** …` blockquote. Useful on its own AND used as
    the intermediate format for the PDF renderer.
  * `render_canvas_pdf(title, slots)`  — Markdown → HTML → PDF. The
    Markdown step gives clean section semantics; the HTML render via
    reportlab Platypus then handles flowing layout, page breaks, and
    typography automatically. Charts rendered server-side via matplotlib;
    one chart per page, full page width.
"""
from __future__ import annotations

import html as _html
from typing import Any


# ── PDF export ──────────────────────────────────────────────────────────────
#
# Server-side rendering: each chart is drawn as a PNG via matplotlib, then
# laid out on a PDF page via reportlab. Pure Python — no headless browser
# required. The layout reproduces the canvas's 12-column grid: each tile is
# placed using its (x, y, w, h), and the chart's insight is printed under
# the chart image.

def _safe_filename(title: str | None) -> str:
    safe = "".join(c if c.isalnum() or c in (" ", "-", "_") else "_" for c in (title or "canvas"))
    return safe.strip().replace(" ", "_") or "canvas"


def _render_chart_png(chart: dict[str, Any], width_in: float, height_in: float) -> bytes | None:
    """Render a single Highcharts-shaped chart dict to a PNG via matplotlib.

    The output PNG is exactly `width_in × height_in` inches — no
    `tight_layout` and no `bbox_inches="tight"`, both of which would
    re-crop / re-size and break the caller's tile-fitting math.

    Supports the strict per-type union: cartesian (column / bar / line / area /
    spline / areaspline / scatter) and pie (pie / donut). Returns None on
    unsupported shapes — caller substitutes a placeholder.
    """
    import io
    import matplotlib
    matplotlib.use("Agg")  # no display backend
    import matplotlib.pyplot as plt

    inner = chart.get("chart") or {}
    ctype = (inner.get("type") if isinstance(inner, dict) else None) or ""
    ctype = str(ctype).lower()
    series = chart.get("series") or []
    colors = chart.get("colors") or [
        "#2E86AB", "#F18F01", "#A23B72", "#3B8EA5", "#C73E1D", "#6A994E",
    ]
    title = (chart.get("title") or {}).get("text") if isinstance(chart.get("title"), dict) else (chart.get("title") or "")

    # Truncate over-long axis labels so they don't overflow the tight tile.
    def _shrink(s: str | None, lim: int = 30) -> str:
        s = (s or "").strip()
        return (s[: lim - 1] + "…") if len(s) > lim else s

    # 100 DPI keeps text sharp while keeping the image ~half the size of a
    # 140-DPI render. Combined with JPEG output below this roughly halves
    # the resulting PDF — essential for the /base64 endpoints that ship
    # the PDF inline in emails.
    fig, ax = plt.subplots(figsize=(width_in, height_in), dpi=100, facecolor="white")

    cartesian = ctype in ("column", "bar", "line", "area", "spline", "areaspline", "scatter")
    pie       = ctype in ("pie", "donut")

    def _fmt_num(v):
        """Format a numeric value for a data label — 2 decimals max, drop
        trailing zeros; integer-valued floats become integers."""
        try:
            f = float(v)
        except Exception:
            return str(v)
        if f != f:       # NaN
            return ""
        if f == int(f):  # 3.0 → "3"
            return f"{int(f)}"
        return f"{f:.2f}".rstrip("0").rstrip(".")

    if cartesian and series:
        xaxis = chart.get("xAxis") or {}
        cats  = xaxis.get("categories") or []
        # x positions per category, grouped per series
        n_series = len(series)
        n_cats   = len(cats)
        x = list(range(n_cats))

        # Data labels are ALWAYS shown — no hover in a PDF, so the value
        # has to be printed on the plot itself. Scale the font size with
        # marker count so labels still fit when there are many bars.
        total_markers = n_series * max(1, n_cats)
        if   total_markers <= 20:  label_fs = 8
        elif total_markers <= 40:  label_fs = 6
        elif total_markers <= 80:  label_fs = 5
        else:                      label_fs = 4
        # When there are many narrow bars, rotate the labels vertical so
        # they occupy the column width instead of overflowing sideways.
        label_rot = 90 if total_markers > 30 and ctype in ("column",) else 0

        if ctype in ("column", "bar"):
            bar_w = 0.8 / max(1, n_series)
            for i, s in enumerate(series):
                data = [v if v is not None else 0 for v in (s.get("data") or [])]
                offset = (i - (n_series - 1) / 2) * bar_w
                if ctype == "bar":
                    containers = ax.barh(
                        [xi + offset for xi in x], data, height=bar_w,
                        color=colors[i % len(colors)], label=s.get("name", f"Series {i+1}"),
                    )
                    ax.bar_label(
                        containers,
                        labels=[_fmt_num(v) for v in data],
                        padding=2, fontsize=label_fs, color="#111827",
                    )
                else:
                    containers = ax.bar(
                        [xi + offset for xi in x], data, width=bar_w,
                        color=colors[i % len(colors)], label=s.get("name", f"Series {i+1}"),
                    )
                    ax.bar_label(
                        containers,
                        labels=[_fmt_num(v) for v in data],
                        padding=2, fontsize=label_fs, color="#111827",
                        rotation=label_rot,
                    )
        else:
            for i, s in enumerate(series):
                data = [v if v is not None else 0 for v in (s.get("data") or [])]
                if ctype == "scatter":
                    ax.scatter(x[: len(data)], data, color=colors[i % len(colors)], label=s.get("name", f"Series {i+1}"))
                else:
                    ax.plot(x[: len(data)], data, color=colors[i % len(colors)], label=s.get("name", f"Series {i+1}"))
                    if ctype in ("area", "areaspline"):
                        ax.fill_between(x[: len(data)], data, alpha=0.25, color=colors[i % len(colors)])
                for xi, v in zip(x[: len(data)], data):
                    ax.annotate(
                        _fmt_num(v), (xi, v),
                        textcoords="offset points", xytext=(0, 4),
                        ha="center", fontsize=label_fs, color="#111827",
                    )
        # Category labels — scale gracefully no matter how many there are.
        # matplotlib otherwise crams every label together and produces the
        # unreadable wall the user reported.
        #
        # Policy:
        #   n_cats ≤ 12  → show every label, mild rotation (20°)
        #   13..24       → show every label, 45° rotation
        #   25..60       → show every Nth label (thinned), 90° rotation
        #   > 60         → drop tick labels entirely (the chart becomes a
        #                  density view) — otherwise they'd overprint into a
        #                  black smear that obscures the bars.
        cat_labels_full = [_shrink(c, 20) for c in cats]
        if n_cats <= 12:
            thin, rotation, fs = 1, 20, 7
        elif n_cats <= 24:
            thin, rotation, fs = 1, 45, 6
        elif n_cats <= 60:
            # Show ~20 ticks max so the axis is legible.
            thin = max(1, (n_cats + 19) // 20)
            rotation, fs = 90, 6
        else:
            thin, rotation, fs = 0, 90, 6   # 0 → no tick labels

        def _thinned(labels: list[str]) -> list[str]:
            if thin == 0:
                return ["" for _ in labels]
            return [lbl if (i % thin == 0) else "" for i, lbl in enumerate(labels)]

        if ctype == "bar":
            # Horizontal bar: labels are on the Y axis — rotation stays 0
            # but we still thin when there are too many.
            ax.set_yticks(x)
            ax.set_yticklabels(_thinned(cat_labels_full), fontsize=fs)
        else:
            ax.set_xticks(x)
            ax.set_xticklabels(
                _thinned(cat_labels_full),
                rotation=rotation,
                ha=("center" if rotation == 90 else "right"),
                fontsize=fs,
            )

        x_title = ((xaxis.get("title") or {}) if isinstance(xaxis.get("title"), dict) else {}).get("text") or ""
        y_title = (((chart.get("yAxis") or {}).get("title") or {}) if isinstance((chart.get("yAxis") or {}).get("title"), dict) else {}).get("text") or ""
        if x_title and ctype != "bar":
            # When tick labels are thinned/hidden, put a count hint in the
            # x-axis title so the reader knows how many categories there are.
            xt = _shrink(x_title)
            if n_cats > 24:
                xt = f"{xt}  (n={n_cats})"
            ax.set_xlabel(xt, fontsize=7)
        if y_title:
            ax.set_ylabel(_shrink(y_title), fontsize=7)
        if n_series > 1:
            ax.legend(fontsize=6, loc="best")
        ax.tick_params(axis="both", labelsize=fs)
        ax.grid(axis="y" if ctype != "bar" else "x", alpha=0.3)

    elif pie and series and isinstance(series[0], dict):
        slices = series[0].get("data") or []
        names  = [_shrink(s.get("name", f"Slice {i+1}"), 14) for i, s in enumerate(slices)]
        values = [s.get("y", 0) or 0 for s in slices]
        # Label each slice with BOTH the raw value and the percentage, so
        # the reader can tell the absolute size without hover.
        total = sum(v for v in values if isinstance(v, (int, float))) or 1
        def _pie_label(pct):
            # matplotlib's autopct is called with percentage only; recover
            # the underlying value via the total.
            val = total * pct / 100.0
            return f"{_fmt_num(val)}\n({pct:.1f}%)"
        wedges, _, autotexts = ax.pie(
            values, labels=names, autopct=_pie_label, startangle=90,
            colors=colors[: len(slices)],
            wedgeprops={"width": 0.45 if ctype == "donut" else None, "linewidth": 1, "edgecolor": "white"},
            textprops={"fontsize": 7},
        )
        for t in autotexts:
            t.set_fontsize(8)
        ax.axis("equal")
    else:
        plt.close(fig)
        return None

    if title:
        ax.set_title(_shrink(title, 50), fontsize=9, pad=4)

    # `tight_layout` auto-adjusts margins so axis titles, tick labels and
    # the figure title never get clipped — even at the small figsizes used
    # for grid-cell rendering. `bbox_inches="tight"` then crops the saved
    # image to its actual content, with a small uniform padding.
    #
    # JPEG (quality=78) instead of PNG: charts are solid-fill shapes with a
    # little text — JPEG encodes these in ~30–45% of the PNG bytes with no
    # visible quality loss at print DPI, and the PDF containing them shrinks
    # accordingly. White facecolor set above so the JPEG has no blotchy
    # backgrounds. `optimize=True` lets PIL re-pack the Huffman tables for
    # another few % savings.
    fig.tight_layout(pad=0.6)
    buf = io.BytesIO()
    fig.savefig(
        buf, format="jpg",
        bbox_inches="tight", pad_inches=0.05,
        pil_kwargs={"quality": 78, "optimize": True, "progressive": True},
    )
    plt.close(fig)
    return buf.getvalue()


# ── Markdown intermediate ───────────────────────────────────────────────────

import base64 as _b64
import datetime as _dt


def render_canvas_markdown(title: str, slots: list[dict[str, Any]]) -> str:
    """Build a Markdown report — ONE chart per section, full size.

    Each slot becomes:
      ## {chart title}
      _description (if any)_
      ![chart-{cid}](data:image/jpeg;base64,…)    ← rendered server-side
      > **Insight:** {insight}

    Slots are emitted in canvas position order (top-to-bottom by `y`,
    then left-to-right by `x`) so the report reads in the same order
    a viewer would scan the canvas — but each chart gets its own
    full-width section instead of being squeezed into a grid cell.
    """
    today = _dt.date.today().isoformat()
    parts: list[str] = [
        f"# {title or 'Canvas Report'}",
        "",
        f"_Generated {today} · {len(slots)} chart(s)_",
        "",
    ]

    if not slots:
        parts.append("_This canvas has no charts._")
        return "\n".join(parts)

    # Canvas position order (top-to-bottom, left-to-right).
    slots_sorted = sorted(
        slots,
        key=lambda s: (float(s.get("y", 0) or 0), float(s.get("x", 0) or 0)),
    )

    # Render each chart at a uniform large size — fills a landscape A4
    # page minus margins.  width_in × height_in determines aspect; the
    # PDF Image flowable will further fit it to the page.
    chart_w_in, chart_h_in = 9.5, 5.0

    for i, s in enumerate(slots_sorted, 1):
        chart   = s.get("chart") or {}
        ctitle  = (chart.get("title") or {}).get("text") if isinstance(chart.get("title"), dict) else (chart.get("title") or f"Chart {i}")
        cdesc   = (chart.get("description") or "").strip()
        insight = (chart.get("insight") or "").strip()
        cid     = (chart.get("chart_id") or f"chart-{i}")[:8]

        parts.append(f"## {ctitle}")
        if cdesc:
            parts.append(f"_{cdesc}_")
        parts.append("")

        img = _render_chart_png(chart, chart_w_in, chart_h_in)
        if img:
            # Chart is rendered as JPEG (smaller than PNG for solid-fill
            # bar/pie charts) — mime type in the data-URL matches.
            data_url = "data:image/jpeg;base64," + _b64.b64encode(img).decode("ascii")
            parts.append(f"![chart-{cid}]({data_url})")
        else:
            parts.append("> _(unsupported chart type — could not render)_")
        parts.append("")

        if insight:
            parts.append(f"> **Insight:** {insight}")
            parts.append("")

    return "\n".join(parts)


# ── Markdown → PDF ──────────────────────────────────────────────────────────
#
# The PDF flow is two stages, both inspectable independently:
#   1. canvas → Markdown        (render_canvas_markdown — text the user
#                                could open in any MD viewer)
#   2. Markdown → HTML → PDF    (render_canvas_pdf below — uses the `markdown`
#                                library to convert MD to HTML, then walks
#                                the HTML and emits reportlab Platypus
#                                flowables for clean typography + auto
#                                page breaks)
#
# Going through MD removes layout guesswork: each chart becomes a clean
# section (title + image + insight), Platypus handles flowing them across
# pages, and the typography (headings, blockquotes, italics) stays
# consistent regardless of how many charts the canvas contains.

def render_canvas_pdf(title: str, slots: list[dict[str, Any]]) -> bytes:
    """Markdown → HTML → PDF pipeline.

    Steps:
      1. `render_canvas_markdown(title, slots)` builds the MD source
         (sections per chart, inline base64 PNG, blockquoted insight).
      2. The `markdown` library parses it to HTML.
      3. We walk the HTML elements and emit reportlab Platypus flowables
         (`Paragraph`, `Image`, `Spacer`, `PageBreak`). Platypus then
         handles page layout, headers, page numbers, and overflow.
    """
    import io
    import xml.etree.ElementTree as ET
    import markdown as md
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak,
    )

    md_text = render_canvas_markdown(title, slots)
    html_text = md.markdown(md_text, extensions=["extra"])

    try:
        root = ET.fromstring(f"<root>{html_text}</root>")
    except ET.ParseError as e:
        root = ET.fromstring(f"<root><p>PDF render failed: {_html.escape(str(e))}</p></root>")

    # ── Styles ────────────────────────────────────────────────────────────
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("Heading1Custom", parent=styles["Heading1"],
                        fontSize=18, leading=22, spaceAfter=8, textColor=colors.HexColor("#0f172a"))
    h2 = ParagraphStyle("Heading2Custom", parent=styles["Heading2"],
                        fontSize=12, leading=15, spaceBefore=4, spaceAfter=2, textColor=colors.HexColor("#1f2937"))
    body = ParagraphStyle("BodyCustom", parent=styles["BodyText"],
                          fontSize=9, leading=12, alignment=TA_LEFT)
    italic = ParagraphStyle("ItalicCustom", parent=body, fontName="Helvetica-Oblique",
                            textColor=colors.HexColor("#6b7280"), spaceAfter=4)
    insight_style = ParagraphStyle(
        "Insight", parent=body,
        leftIndent=8, rightIndent=4,
        borderColor=colors.HexColor("#4CAF50"),
        borderPadding=(4, 6, 4, 6),
        backColor=colors.HexColor("#F0F8F0"),
        spaceBefore=4, spaceAfter=4,
        fontSize=8, leading=10,
    )

    page_w, page_h = landscape(A4)
    margin = 0.4 * inch
    page_inner_w = page_w - 2 * margin

    # ── ET element → reportlab inline-text helper ─────────────────────────
    def text_of(elem: ET.Element) -> str:
        """Inline text of a tag, with bold/italic preserved as <b>/<i>."""
        out: list[str] = []
        if elem.text:
            out.append(elem.text)
        for child in elem:
            tag = child.tag.lower()
            inner = text_of(child)
            if   tag in ("strong", "b"): out.append(f"<b>{inner}</b>")
            elif tag in ("em", "i"):     out.append(f"<i>{inner}</i>")
            elif tag == "code":          out.append(f"<font face='Courier'>{inner}</font>")
            elif tag == "br":            out.append("<br/>")
            else:                        out.append(inner)
            if child.tail:
                out.append(child.tail)
        return "".join(out)

    # ── Walk the top-level HTML elements ──────────────────────────────────
    #
    # Layout policy: ONE chart per page, full size. Each `<h2>` (chart
    # title) starts a new page, then we render: title → description →
    # chart image (sized to fill the page) → insight.
    flowables: list = []
    chart_count_seen = 0
    page_max_w = page_inner_w
    page_max_h = (page_h - 2 * margin) * 0.70   # leave room for h2 + insight

    for elem in root:
        tag = elem.tag.lower()
        if tag == "h1":
            flowables.append(Paragraph(text_of(elem), h1))
        elif tag == "h2":
            # Each chart on its own page — break before every h2 except
            # the very first (which can share the page with the doc title).
            if chart_count_seen > 0:
                flowables.append(PageBreak())
            chart_count_seen += 1
            flowables.append(Paragraph(text_of(elem), h2))
        elif tag == "p":
            img = elem.find("img")
            if img is not None:
                # Chart image — fill the page width while preserving aspect.
                src = img.get("src", "")
                if src.startswith("data:image/"):
                    try:
                        png = _b64.b64decode(src.split(",", 1)[1])
                        from PIL import Image as PILImage
                        with PILImage.open(io.BytesIO(png)) as pim:
                            iw, ih = pim.size
                        target_w = page_max_w
                        target_h = target_w * (ih / iw)
                        if target_h > page_max_h:
                            target_h = page_max_h
                            target_w = target_h * (iw / ih)
                        flowables.append(Image(io.BytesIO(png), width=target_w, height=target_h))
                        flowables.append(Spacer(1, 6))
                    except Exception as e:
                        flowables.append(Paragraph(f"<i>(image render failed: {_html.escape(str(e))})</i>", italic))
                else:
                    flowables.append(Paragraph("<i>(unsupported image src)</i>", italic))
            else:
                txt = text_of(elem).strip()
                if txt:
                    is_italic = (len(elem) == 1 and elem[0].tag.lower() in ("em", "i") and not elem.text)
                    flowables.append(Paragraph(txt, italic if is_italic else body))
        elif tag == "blockquote":
            inner = text_of(elem).strip()
            if inner:
                flowables.append(Paragraph(inner, insight_style))
        else:
            txt = text_of(elem).strip()
            if txt:
                flowables.append(Paragraph(txt, body))

    if not flowables:
        flowables.append(Paragraph("This canvas has no charts.", italic))

    # ── Build the document ───────────────────────────────────────────────
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=landscape(A4),
        leftMargin=margin, rightMargin=margin,
        topMargin=margin, bottomMargin=margin,
        title=title or "Canvas Report",
    )
    doc.build(flowables)
    return buf.getvalue()


