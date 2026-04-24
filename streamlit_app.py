"""
Streamlit UI for the Reporting Agent.

Layout:
  - LEFT  (60%): chat thread. Ask a question, get charts, edit charts, add
                 charts to the active canvas draft.
  - RIGHT (40%): canvas box — the active report draft. Drag-drop reorder,
                 rename, finalize to a template. DB-backed (no session state
                 for canvas state), so a browser refresh does not wipe it.
  - Sidebar:    user settings, draft picker, saved templates.
  - Saved template view is a collapsible bottom section.

Run: streamlit run streamlit_app.py
"""
from __future__ import annotations

import json
import math
import sys
import urllib.parse
import uuid
from pathlib import Path

_APP_DIR = str(Path(__file__).resolve().parent / "app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

import requests
import streamlit as st  # type: ignore[import-unresolved]
import streamlit.components.v1 as components  # type: ignore[import-unresolved]

# Typed API client — single source of truth for talking to the backend.
# Replaces the scattered `requests.*` calls and the requests-monkey-patch
# trace. The client owns: serialisation, response parsing into Pydantic
# models, structured ApiError, and a per-call trace ring for debugging.
from ui.api_client import ApiClient, ApiError

try:
    from streamlit_sortables import sort_items  # type: ignore[import-unresolved]
    _HAVE_SORTABLE = True
except Exception:
    _HAVE_SORTABLE = False


# ── Configuration ───────────────────────────────────────────────────────────

API_BASE = "http://localhost:8002/api/v1"
PROJECT_TYPES = ["NTM", "AHLOB Modernization", "Both"]
MAX_REPORT_CHARTS = 6


def get_client() -> ApiClient:
    """Fresh ApiClient per rerun — but its trace + HTTP session are
    module-level singletons inside `ui.api_client`, so they persist
    across reruns. We deliberately DON'T `@st.cache_resource` the
    instance: caching pinned the methods table from the moment the
    cache was warmed, so adding a new client method (`get_template`)
    needed a process restart to become visible. Re-instantiating each
    time is sub-microsecond and lets new methods land on hot reload.
    """
    return ApiClient(API_BASE)


api = get_client()


# ── JSON sanitizer (NaN/Inf → null) ─────────────────────────────────────────

def _sanitize_for_json(obj):
    if obj is None:
        return None
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {(k if isinstance(k, str) else str(k)): _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj


# ── Highcharts rendering ────────────────────────────────────────────────────

_HIGHCHARTS_JS_PATH = Path(__file__).resolve().parent / "static" / "highcharts.js"
_HIGHCHARTS_JS = ""
if _HIGHCHARTS_JS_PATH.exists():
    _HIGHCHARTS_JS = _HIGHCHARTS_JS_PATH.read_text(encoding="utf-8")


def render_insight(insight, header: str = "💡 Insight"):
    if not insight:
        return
    st.markdown(f"##### {header}")
    if isinstance(insight, dict):
        headline = (insight.get("headline") or "").strip()
        what = insight.get("what_the_data_shows") or []
        why = insight.get("why_it_matters") or []
        nxt = (insight.get("recommended_next_step") or "").strip()
        legacy_md = insight.get("_legacy_markdown") or ""
        if headline:
            st.markdown(
                f"<div style='padding:8px 12px;border-left:4px solid #4CAF50;"
                f"background:rgba(76,175,80,0.08);margin-bottom:8px;"
                f"font-size:1.02em;'><b>{headline}</b></div>",
                unsafe_allow_html=True,
            )
        if what:
            st.markdown("**What the data shows**")
            for b in what:
                st.markdown(f"- {b}")
        if why:
            st.markdown("**Why it matters**")
            for b in why:
                st.markdown(f"- {b}")
        if nxt:
            st.markdown("**Recommended next step**")
            st.markdown(f"- {nxt}")
        if legacy_md and not (headline or what or why or nxt):
            st.markdown(legacy_md)
        return
    if isinstance(insight, str):
        st.markdown(insight)


def render_readonly_grid(slots: list[dict], container_key: str,
                          height: int = 760, cell_height: int = 80) -> None:
    """Render slots at their saved x/y/w/h with drag/resize disabled — used
    by the template view so a re-run reproduces the exact layout the user
    arranged at finalize time."""
    tiles = []
    for s in slots:
        x = int(s.get("x", 0) or 0)
        y = int(s.get("y", 0) or 0)
        w = int(s.get("w", 6) or 6)
        h = int(s.get("h", 4) or 4)
        chart_json = json.dumps(s.get("chart") or {}, default=str)
        host_id = f"ro-chart-{container_key}-{x}-{y}-{w}-{h}"
        # Only the chart goes inside the tile — no title bar, no scripts visible.
        tiles.append(f"""
          <div class="grid-stack-item" gs-x="{x}" gs-y="{y}" gs-w="{w}" gs-h="{h}" gs-no-resize gs-no-move>
            <div class="grid-stack-item-content tile">
              <div class="chart-host" id="{host_id}"></div>
              <script type="application/json" class="chart-config" data-host="{host_id}">{chart_json}</script>
            </div>
          </div>
        """)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <link href="https://cdn.jsdelivr.net/npm/gridstack@10.1.2/dist/gridstack.min.css" rel="stylesheet"/>
      <script src="https://cdn.jsdelivr.net/npm/gridstack@10.1.2/dist/gridstack-all.js"></script>
      <script>{_HIGHCHARTS_JS}</script>
      <style>
        body {{ margin:0; padding:0; background:transparent;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }}
        .grid-stack {{ background: rgba(0,0,0,0.015); border-radius: 6px; }}
        .grid-stack-item-content.tile {{
          background: #fff; border: 1px solid #e5e7eb; border-radius: 8px;
          overflow: hidden;
        }}
        .chart-host {{ width: 100%; height: 100%; padding: 4px; box-sizing: border-box; }}
      </style>
    </head>
    <body>
      <div class="grid-stack">{''.join(tiles)}</div>
      <script>
        GridStack.init({{
          column: 12, cellHeight: {cell_height},
          margin: 6, float: true,
          disableDrag: true, disableResize: true, staticGrid: true,
        }});
        document.querySelectorAll('.chart-config').forEach(s => {{
          const hostId = s.dataset.host;
          try {{
            const cfg = JSON.parse(s.textContent);
            if (!cfg.chart) cfg.chart = {{}};
            cfg.chart.renderTo = hostId;
            if (!cfg.credits) cfg.credits = {{ enabled: false }};
            cfg.title = cfg.title || {{}};
            if (typeof cfg.title === 'string') cfg.title = {{ text: cfg.title }};
            cfg.title.style = Object.assign({{ fontSize: '13px' }}, cfg.title.style || {{}});
            Highcharts.chart(cfg);
          }} catch (e) {{
            const host = document.getElementById(hostId);
            if (host) host.innerText = '⚠ chart render failed';
          }}
        }});
      </script>
    </body>
    </html>
    """
    components.html(html, height=height, scrolling=True)


def render_gridstack_canvas(slots: list[dict], draft_id: str, api_base: str,
                             height: int = 760, cell_height: int = 80) -> None:
    """Render the active draft's slots as a GridStack 2D grid.

    Each tile is draggable and resizable at pixel precision. On every drag or
    resize the new layout (x, y, w, h per tile) is PATCHed straight to the
    FastAPI `/canvas/drafts/{draft_id}` endpoint from inside the iframe, so
    positions persist instantly without needing a Streamlit rerun.

    After dragging, click the "🔄 Pull latest layout" button below the canvas
    to have Streamlit re-fetch from the DB (updates other views — e.g., the
    finalize payload).
    """
    # Build one gridstack-item per slot. Only the chart goes into the tile —
    # no title bar, no data, no insight, no script. Highcharts already paints
    # its own chart.title inside the chart, so a redundant header would just
    # eat vertical space.
    items_html = []
    for s in slots:
        x = int(s.get("x", 0) or 0)
        y = int(s.get("y", 0) or 0)
        w = int(s.get("w", 6) or 6)
        h = int(s.get("h", 4) or 4)
        chart_obj = s.get("chart") or {}
        cid = chart_obj.get("chart_id") or s.get("source") or ""
        chart_json = json.dumps(chart_obj, default=str)
        items_html.append(f"""
          <div class="grid-stack-item"
               gs-x="{x}" gs-y="{y}" gs-w="{w}" gs-h="{h}"
               gs-id="{cid}">
            <div class="grid-stack-item-content tile">
              <button class="tile-rm" data-source="{cid}" title="Remove from canvas">✕</button>
              <div class="chart-host" id="chart-{cid}"></div>
              <script type="application/json" class="chart-config">{chart_json}</script>
            </div>
          </div>
        """)

    slots_payload_json = json.dumps(slots, default=str)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <link href="https://cdn.jsdelivr.net/npm/gridstack@10.1.2/dist/gridstack.min.css" rel="stylesheet"/>
      <script src="https://cdn.jsdelivr.net/npm/gridstack@10.1.2/dist/gridstack-all.js"></script>
      <script>{_HIGHCHARTS_JS}</script>
      <style>
        body {{ margin:0; padding:0; background:transparent;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }}
        .grid-stack {{ background: rgba(0,0,0,0.015); border-radius: 6px; min-height: {cell_height * 6}px; }}
        .grid-stack-item-content.tile {{
          background: #fff; border: 1px solid #e5e7eb; border-radius: 8px;
          box-shadow: 0 1px 3px rgba(0,0,0,.04); overflow: hidden;
          cursor: move; position: relative;
        }}
        /* The chart fills the whole tile — no title bar, no script preview,
           no data dump. Just the graph. */
        .chart-host {{ width: 100%; height: 100%; padding: 4px; box-sizing: border-box; }}
        /* Floating remove button in the top-right corner (only on hover so
           it doesn't compete with the chart). */
        .tile-rm {{
          position: absolute; top: 4px; right: 6px;
          border: 0; background: rgba(255,255,255,.85); cursor: pointer;
          color: #991b1b; font-size: 13px; padding: 2px 6px; border-radius: 4px;
          opacity: 0; transition: opacity .15s ease; z-index: 5;
          line-height: 1;
        }}
        .grid-stack-item-content.tile:hover .tile-rm {{ opacity: 1; }}
        .tile-rm:hover {{ color: #dc2626; background: #fff; }}
        .status {{ font-size: 12px; color: #6b7280; padding: 4px 2px; }}
        .status.saving {{ color: #2563eb; }}
        .status.saved  {{ color: #16a34a; }}
        .status.error  {{ color: #dc2626; }}
      </style>
    </head>
    <body>
      <div id="status" class="status">Ready.</div>
      <div class="grid-stack">
        {''.join(items_html)}
      </div>

      <script>
        const DRAFT_ID = {json.dumps(draft_id)};
        const API_BASE = {json.dumps(api_base)};
        const STATUS = document.getElementById('status');
        // Snapshot of the slots as currently in the DB, keyed by source.
        const BASE_SLOTS = {slots_payload_json};
        const SLOT_BY_SRC = {{}};
        BASE_SLOTS.forEach(s => {{
          const src = (s.chart && s.chart.chart_id) || s.source || '';
          SLOT_BY_SRC[src] = s;
        }});

        // Render every chart into its host element.
        document.querySelectorAll('.grid-stack-item').forEach(el => {{
          const cfgScript = el.querySelector('.chart-config');
          const host = el.querySelector('.chart-host');
          if (!cfgScript || !host) return;
          try {{
            const cfg = JSON.parse(cfgScript.textContent);
            if (!cfg.chart) cfg.chart = {{}};
            cfg.chart.renderTo = host.id;
            if (!cfg.credits) cfg.credits = {{ enabled: false }};
            // Shrink font slightly so things fit inside tiles.
            cfg.title = cfg.title || {{}};
            if (typeof cfg.title === 'string') cfg.title = {{ text: cfg.title }};
            cfg.title.style = Object.assign({{ fontSize: '13px' }}, cfg.title.style || {{}});
            Highcharts.chart(cfg);
          }} catch (e) {{
            host.innerText = '⚠ chart render failed';
          }}
        }});

        // Initialise GridStack with a 12-col grid.
        const grid = GridStack.init({{
          column: 12, cellHeight: {cell_height},
          margin: 6, float: true, animate: true,
        }});

        async function persistLayout() {{
          // Serialise current positions
          const positions = grid.save(false);  // [{{id, x, y, w, h}}...]
          const byId = Object.fromEntries(positions.map(p => [p.id, p]));
          const newSlots = Object.values(SLOT_BY_SRC).map(s => {{
            const src = (s.chart && s.chart.chart_id) || s.source || '';
            const pos = byId[src];
            if (!pos) return s;  // removed
            return Object.assign({{}}, s, {{
              x: pos.x, y: pos.y, w: pos.w, h: pos.h,
            }});
          }}).filter(s => {{
            const src = (s.chart && s.chart.chart_id) || s.source || '';
            return byId[src] != null;
          }});

          STATUS.textContent = 'Saving layout…';
          STATUS.className = 'status saving';
          try {{
            const res = await fetch(`${{API_BASE}}/canvas/drafts/${{DRAFT_ID}}`, {{
              method: 'PATCH',
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify({{ slots: newSlots }}),
            }});
            if (!res.ok) throw new Error('HTTP ' + res.status);
            STATUS.textContent = 'Layout saved · ' + new Date().toLocaleTimeString();
            STATUS.className = 'status saved';
          }} catch (e) {{
            STATUS.textContent = 'Save failed: ' + e.message;
            STATUS.className = 'status error';
          }}
        }}

        // Persist after drag or resize stops.
        grid.on('change', () => {{ persistLayout(); }});

        // Tile-level remove button.
        document.querySelectorAll('.tile-rm').forEach(btn => {{
          btn.addEventListener('click', async (e) => {{
            e.stopPropagation();
            const src = btn.dataset.source;
            delete SLOT_BY_SRC[src];
            const el = document.querySelector(`.grid-stack-item[gs-id="${{src}}"]`);
            if (el) grid.removeWidget(el);
            await persistLayout();
          }});
        }});
      </script>
    </body>
    </html>
    """
    components.html(html, height=height, scrolling=True)


def render_highchart(chart_config: dict, container_key: str, height: int = 380):
    config_json = json.dumps(chart_config, default=str)
    container_id = f"chart-{container_key}"
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ margin: 0; padding: 0; background: transparent; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }}
            #{container_id} {{ width: 100%; height: {height - 20}px; }}
        </style>
        <script>{_HIGHCHARTS_JS}</script>
    </head>
    <body>
        <div id="{container_id}"></div>
        <script>
            var config = {config_json};
            if (!config.chart) config.chart = {{}};
            config.chart.renderTo = '{container_id}';
            if (!config.credits) config.credits = {{ enabled: false }};
            Highcharts.chart(config);
        </script>
    </body>
    </html>
    """
    components.html(html, height=height, scrolling=False)


# ── Thin shims that adapt the typed ApiClient to the legacy dict-shaped
# call sites in the rest of this file. New code should call `api.X()`
# directly; these stay for compatibility while the rest of the file is
# being incrementally cleaned up.
#
# All errors get surfaced to the user via `_show_api_error(...)` so we
# never silently swallow a failure.

def _show_api_error(action: str, e: ApiError) -> None:
    """One canonical error renderer — uses the structured field errors when
    the server returned FastAPI's validation array."""
    if e.field_errors:
        st.error(f"{action} failed ({e.status}) — server rejected the payload:")
        for loc, msg in e.field_errors[:6]:
            st.markdown(f"- **`{loc}`** — {msg}")
    else:
        st.error(f"{action} failed ({e.status}): {e.detail}")


def api_list_drafts(user_id: str) -> list[dict]:
    """Return every draft for the user with its slots inlined.

    The API's `GET /canvas/drafts` returns metadata only (cheap listing);
    we hydrate each draft via `GET /canvas/drafts/{id}` so the UI can show
    slot counts, render the active canvas, and dedupe on add. With <50
    drafts per user, the per-draft fetch is sub-ms thanks to the connection
    pool — well within Streamlit's render budget.
    """
    try:
        meta_drafts = api.list_drafts(user_id)
    except (ApiError, requests.RequestException):
        return []

    out: list[dict] = []
    for d in meta_drafts:
        try:
            full = api.get_draft(d.draft_id)
            out.append(full.model_dump())
        except (ApiError, requests.RequestException):
            # Fall back to metadata-only on any per-draft hiccup.
            out.append({**d.model_dump(), "slots": []})
    return out


def api_create_draft(user_id: str, name: str, project_type: str) -> dict | None:
    try:
        return api.create_draft(user_id=user_id, name=name, project_type=project_type).model_dump()
    except ApiError as e:
        _show_api_error("Create draft", e); return None


def api_patch_draft(draft_id: str, name=None, slots=None) -> dict | None:
    try:
        # _sanitize_for_json: keep NaN/Inf out of the JSON body
        clean_slots = _sanitize_for_json(slots) if slots is not None else None
        return api.patch_draft(draft_id, name=name, slots=clean_slots).model_dump()
    except ApiError as e:
        _show_api_error("Update draft", e); return None


def api_delete_draft(draft_id: str) -> bool:
    try:
        return api.delete_draft(draft_id)
    except ApiError:
        return False


def api_list_thread_messages(thread_id: str, user_id: str) -> list[dict]:
    """Server enforces ownership — pass the current user_id."""
    try:
        return api.list_thread_messages(thread_id, user_id=user_id)
    except (ApiError, requests.RequestException):
        return []


def _messages_to_query_records(messages: list[dict]) -> list[dict]:
    """Pair the flat user/ai message stream back into per-turn records the
    chat renderer expects. Same `query_id` pairs them; user.content → query
    text, ai.content → rationale, ai.charts → charts."""
    by_qid: dict[str, dict] = {}
    order: list[str] = []
    for m in messages:
        qid = m.get("query_id") or ""
        if qid not in by_qid:
            by_qid[qid] = {"query_id": qid, "query": "", "rationale": "", "charts": []}
            order.append(qid)
        if m.get("role") == "user":
            by_qid[qid]["query"] = m.get("content") or ""
        elif m.get("role") == "ai":
            by_qid[qid]["rationale"] = m.get("content") or ""
            by_qid[qid]["charts"] = m.get("charts") or []
    return [by_qid[q] for q in order]


# ── Page setup ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Reporting Agent", page_icon="📊", layout="wide")
st.title("Reporting Agent")
st.caption("Ask on the left. Drop the graphs you want onto the canvas on the right. Finalize to save a reusable report.")


# ── Session state ───────────────────────────────────────────────────────────

def _init_state():
    if "thread_id" not in st.session_state:
        st.session_state.thread_id = str(uuid.uuid4())
    # Chat conversation for the current thread (list of query records).
    # This is rehydrated from the server on every render if empty, so a
    # browser refresh doesn't wipe prior Q&A turns.
    if "queries" not in st.session_state:
        st.session_state.queries = []
    if "active_draft_id" not in st.session_state:
        st.session_state.active_draft_id = None
    if "template_view" not in st.session_state:
        st.session_state.template_view = None

_init_state()


# ── Sidebar ─────────────────────────────────────────────────────────────────

def _list_threads_for_user(uid: str) -> list[dict]:
    """Pull every thread this user has, newest first. Empty list on error."""
    try:
        return [t.model_dump() for t in api.list_threads(uid)]
    except (ApiError, requests.RequestException):
        return []


def _list_queries_in_thread(tid: str, uid: str) -> list[dict]:
    """Sidebar Q list for an inactive thread. Pulls the chat messages and
    keeps just the user-side entries (the questions). Ownership-checked."""
    try:
        msgs = api.list_thread_messages(tid, user_id=uid)
    except (ApiError, requests.RequestException):
        return []
    # Sidebar only needs each user question — flatten ai messages out.
    return [
        {"query_id": m.get("query_id"),
         "original_query": m.get("content") or "",
         "rationale": ""}
        for m in msgs if m.get("role") == "user"
    ]


with st.sidebar:
    st.header("Settings")
    user_id = st.text_input("User ID", value="demo_user")
    project_type = st.selectbox("Project Type", options=PROJECT_TYPES, index=0)
    max_charts = st.slider("Max charts per query", 1, 5, 3)

    st.divider()
    if st.button("➕ New thread", use_container_width=True, type="primary"):
        st.session_state.thread_id = str(uuid.uuid4())
        st.session_state.queries = []
        st.session_state.active_draft_id = None
        st.session_state.template_view = None
        st.rerun()

    # ── Past threads — switch in/out, see the Q history ──────────────────
    st.subheader("💬 Past chats")
    threads = _list_threads_for_user(user_id)
    if not threads:
        st.caption("_No past chats yet — ask something to start one._")
    else:
        st.caption(f"{len(threads)} thread(s) for `{user_id}`")
        for t in threads:
            tid = t.get("thread_id")
            is_active = (tid == st.session_state.thread_id)
            title = (t.get("title") or "").strip() or "(untitled)"
            updated = str(t.get("updated_at") or "")[:16]   # YYYY-MM-DD HH:MM

            # Cache per-thread Q lists for inactive threads (they don't change),
            # but always refetch the active thread so a brand-new question
            # shows up in the sidebar without a manual refresh.
            cache_key = f"_thread_qs_{tid}"
            if is_active or cache_key not in st.session_state:
                st.session_state[cache_key] = _list_queries_in_thread(tid, user_id)
            qs = st.session_state[cache_key]

            label = f"{'🟢' if is_active else '💬'}  {title[:48]}"
            with st.expander(label, expanded=is_active):
                st.caption(f"`{tid[:8]}…` · last activity: `{updated}` · {len(qs)} Q")
                # Show the Q (and a short A preview) for each turn
                for i, q in enumerate(qs, 1):
                    qtxt = (q.get("original_query") or "").strip() or "(empty)"
                    rat  = (q.get("rationale")      or "").strip()
                    st.markdown(f"**Q{i}.** {qtxt}")
                    if rat:
                        st.caption(f"_{rat[:140]}{'…' if len(rat) > 140 else ''}_")
                # Switch action
                if not is_active:
                    if st.button("Open this thread",
                                 key=f"open_thr_{tid}",
                                 use_container_width=True):
                        st.session_state.thread_id = tid
                        st.session_state.queries = []           # forces rehydrate
                        st.session_state.active_draft_id = None
                        st.session_state.template_view = None
                        st.rerun()
                else:
                    st.caption("✅ Currently open in chat.")

    st.divider()
    try:
        _n_templates = len(api.list_templates(user_id))
    except (ApiError, requests.RequestException):
        _n_templates = 0
    try:
        _n_drafts = len(api.list_drafts(user_id))
    except (ApiError, requests.RequestException):
        _n_drafts = 0
    st.caption(f"📚 **{_n_drafts}** canvas draft(s) · **{_n_templates}** template(s)")
    st.caption("_Manage them in the **Canvas** and **Templates** tabs →_")


# ── Rehydrate chat from the DB ──────────────────────────────────────────────

if not st.session_state.queries:
    # Pull the thread as a chat-style message list (user/ai pairs).
    # Re-pair the messages back into per-turn records the renderer expects:
    # one record per query_id with {query, rationale, charts}.
    messages = api_list_thread_messages(st.session_state.thread_id, user_id)
    paired = _messages_to_query_records(messages)
    rebuilt = []
    for row in paired:
        qid = row.get("query_id") or str(uuid.uuid4())
        rebuilt.append({
            "query_id":           qid,
            "query":              row.get("query") or "",
            "project_type":       "",
            "charts":             row.get("charts") or [],
            "rationale":          row.get("rationale") or "",
            "traversal_steps":    0,
            "traversal_findings": "",
            "retrieval_nodes":    [],
            "retrieval_paths":    [],
        })
    if rebuilt:
        st.session_state.queries = rebuilt


# ── Load drafts for this user (always fresh, never cached in memory) ──────
# Drafts are user-scoped: every canvas the user has built across any thread
# shows up here so they can drop new charts into an old report, not just the
# one tied to the current chat.

drafts = api_list_drafts(user_id)

# If the stored active_draft_id no longer exists, clear it.
valid_draft_ids = {d["draft_id"] for d in drafts}
if st.session_state.active_draft_id and st.session_state.active_draft_id not in valid_draft_ids:
    st.session_state.active_draft_id = None
if not st.session_state.active_draft_id and drafts:
    # Canvas drafts are user-scoped (no thread_id) — pick the most recently
    # updated draft as the default active one. The list is already sorted
    # newest-first by the backend.
    st.session_state.active_draft_id = drafts[0]["draft_id"]


# ── Helpers used by chart cards ─────────────────────────────────────────────

def _slot_chart_id(s: dict) -> str:
    """Return the slot's stable chart_id (lives inside `chart.chart_id`)."""
    ch = s.get("chart") if isinstance(s, dict) else None
    return (ch.get("chart_id") if isinstance(ch, dict) else None) or ""


def _chart_in_draft(draft: dict, chart_id: str) -> bool:
    return any(_slot_chart_id(s) == chart_id for s in (draft.get("slots") or []))


def _add_chart_to_draft(draft: dict, q_rec: dict, chart_idx: int):
    slots = list(draft.get("slots") or [])
    chart = q_rec["charts"][chart_idx]
    # Defensive unwrap: if this came from /charts?query_id= as a row wrapper
    # (top-level query_id/user_id/title_text), the actual chart object is at
    # row["chart"]. Strict per-type schema rejects the row shape.
    if isinstance(chart, dict) and "chart" in chart and isinstance(chart["chart"], dict) and chart["chart"].get("type") in (
        "column", "bar", "line", "area", "spline", "areaspline", "scatter", "pie", "donut"
    ) and chart.get("title_text") is not None:
        chart = chart["chart"]
    cid = chart.get("chart_id") or ""

    if not cid:
        return False, "Chart has no chart_id (regenerate the report)."
    if any(_slot_chart_id(s) == cid for s in slots):
        return False, "Already in this draft."
    if len(slots) >= MAX_REPORT_CHARTS:
        return False, f"Draft is full ({MAX_REPORT_CHARTS} max)."

    # CanvasSlot is just: chart + position. Provenance (query_id,
    # original_query) lives on the chart row server-side — no need to
    # repeat it here.
    slots.append({
        "position": len(slots),
        "chart":    chart,
    })
    resp = api_patch_draft(draft["draft_id"], slots=slots)
    return resp is not None, "Added."


def _remove_slot(draft_id: str, slots: list, position: int):
    new_slots = [s for s in slots if s.get("position") != position]
    for i, s in enumerate(new_slots):
        s["position"] = i
    api_patch_draft(draft_id, slots=new_slots)


def _sync_chart_everywhere_in_session(chart_id: str, patched_chart: dict):
    """Mirror an edited chart into the in-session caches.

    The DB now stores each canvas slot's chart under its OWN chart_id
    (clone-on-add), so /charts/edit only mutates one row server-side.
    This function just refreshes the Streamlit-side caches so the user
    sees the new colors/labels without a hard reload — it doesn't leak
    across canvases or back into the chat.
    """
    for q in st.session_state.queries:
        for i, c in enumerate(q.get("charts", []) or []):
            if isinstance(c, dict) and c.get("chart_id") == chart_id:
                q["charts"][i] = patched_chart
    for d in drafts:
        slots = d.get("slots") or []
        if isinstance(slots, str):
            try:
                slots = json.loads(slots)
            except Exception:
                continue
        for s in slots:
            ch = s.get("chart") if isinstance(s, dict) else None
            if isinstance(ch, dict) and ch.get("chart_id") == chart_id:
                s["chart"] = patched_chart


@st.dialog("Edit chart")
def _edit_chart_dialog(chart_id: str, chart_title: str):
    st.caption(f"Editing: **{chart_title}**")
    st.write(
        "Describe the change in plain English. Examples:\n"
        "- _change the bars to red_\n"
        "- _rename x-axis to Market and y-axis to Pending Sites_\n"
        "- _sort descending and hide the legend_\n"
    )
    instruction = st.text_area(
        "Your edit",
        placeholder="e.g., change the color of the bars to red",
        key=f"edit_input_{chart_id}",
        height=100,
    )
    ca, cb = st.columns([1, 1])
    with ca:
        apply = st.button("Apply edit", type="primary", use_container_width=True)
    with cb:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    if apply:
        if not instruction.strip():
            st.warning("Please describe the change first.")
            st.stop()
        try:
            with st.spinner("Patching the chart config..."):
                edit = api.edit_chart(chart_id, instruction.strip())
            patched = edit.chart  # plain dict on read
            _sync_chart_everywhere_in_session(chart_id, patched)
            st.success("Edit applied and saved.")
            st.rerun()
        except ApiError as e:
            _show_api_error("Chart edit", e); st.stop()
        except requests.RequestException as e:
            st.error(f"Edit failed: {e}")


# ── Run a new query ─────────────────────────────────────────────────────────

def _run_query(query: str):
    params = urllib.parse.urlencode({
        "query": query.strip(),
        "project_type": project_type,
        "user_id": user_id,
        "max_charts": max_charts,
        "thread_id": st.session_state.thread_id,
    })
    sse_url = f"{API_BASE}/report/stream?{params}"

    progress = st.progress(0.0, text="Starting...")
    status = st.empty()
    result_payload = None
    query_id = None

    try:
        # The ONE place we bypass the typed `api` client: SSE is a long-lived
        # streaming response, not a JSON request/response. Progress events are
        # surfaced to the user via the streamlit progress bar below, so this
        # call doesn't need to appear in the API-trace panel.
        resp = requests.get(sse_url, stream=True, timeout=600)
        if resp.status_code != 200:
            progress.empty()
            st.error(f"API error ({resp.status_code}): {resp.text[:200]}")
            return
        current_event = None
        for line in resp.iter_lines(decode_unicode=True):
            if not line or line.startswith(":"):
                continue
            if line.startswith("event: "):
                current_event = line[7:]
                continue
            if line.startswith("data: "):
                data = json.loads(line[6:])
                if current_event == "stream_started":
                    query_id = data.get("query_id")
                    status.info(f"Query started (ID: {query_id[:8] if query_id else '?'}…)")
                elif current_event == "step":
                    step = data.get("step", 0)
                    total = data.get("total", 3)
                    progress.progress(min(step / (total + 1), 0.99), text=data.get("label", "..."))
                elif current_event == "retrieval_done":
                    status.info(f"Retrieval: {data.get('nodes', 0)} nodes · {data.get('paths', 0)} paths "
                                f"({data.get('elapsed_ms', 0):.0f}ms)")
                elif current_event == "traversal_done":
                    status.success(f"Traversal: {data.get('steps', 0)} tool call(s) in "
                                   f"{data.get('elapsed_ms', 0)/1000:.1f}s")
                elif current_event == "complete":
                    result_payload = data
                elif current_event == "error":
                    progress.empty()
                    status.error(f"Error: {data.get('message', 'Unknown error')}")
                    return

        if not result_payload:
            progress.empty()
            status.warning("Stream ended without a result.")
            return

        progress.progress(1.0, text="Complete")
        status.empty()
        # SSE complete payload is slim now: just charts + rationale + errors.
        # Each chart already carries chart_id + script + insight + colors etc.
        st.session_state.queries.append({
            "query_id":     query_id or str(uuid.uuid4()),
            "query":        query.strip(),
            "project_type": project_type,
            "charts":       result_payload.get("charts", []),
            "rationale":    result_payload.get("rationale", ""),
        })

    except requests.exceptions.ConnectionError:
        progress.empty()
        st.error("Could not connect to the API. Start the server with `uvicorn app.main:app --port 8002`.")
    except Exception as e:
        progress.empty()
        st.error(f"Error: {e}")


# ── Chart card (rendered in the chat column) ────────────────────────────────

def _add_chart_to_draft_by_id(draft_id: str, q_rec: dict, chart_idx: int):
    """Fetch the latest draft state, append a slot, PATCH back."""
    target = next((d for d in drafts if d["draft_id"] == draft_id), None)
    if target is None:
        return False, "Draft not found."
    return _add_chart_to_draft(target, q_rec, chart_idx)


def _render_chart_card(q_rec: dict, chart: dict, idx: int, active_draft: dict | None):
    title = "Chart"
    if isinstance(chart.get("title"), dict):
        title = chart["title"].get("text", f"Chart {idx+1}")
    elif isinstance(chart.get("title"), str):
        title = chart["title"]

    st.markdown(f"**{idx+1}. {title}**")
    description = chart.get("description", "")
    if description:
        st.caption(description)

    render_highchart(chart, container_key=f"{q_rec['query_id']}-{idx}")

    render_insight(chart.get("insight"))

    # Script is now a top-level field on the chart object itself.
    code = chart.get("script") or "(no script captured)"
    with st.expander("🐍 Python + SQL script"):
        st.code(code, language="python")
    # Legacy evidence rows blob — only show if it happens to be present
    # (not in the slim SSE response, but old session caches may carry it).
    ev = chart.get("evidence") or {}
    with st.expander("🗂 Evidence — data used"):
        result = ev.get("result")
        if isinstance(result, (dict, list)):
            st.json(result)
        elif result:
            st.code(str(result))
        else:
            st.caption("No data payload captured.")

    source_key = f"{q_rec['query_id']}:{idx}"
    in_drafts = [d for d in drafts if _chart_in_draft(d, source_key)]
    popover_label = (
        f"✓ In {len(in_drafts)} draft{'s' if len(in_drafts) != 1 else ''}"
        if in_drafts else "➕ Add to canvas"
    )

    # Chat-side action: only "Add to canvas". Editing a chart lives in the
    # Report canvas tab — pick the chart there, click ✏️ Edit. This keeps
    # the chat read-only and stops two parallel edit surfaces from competing.
    with st.popover(popover_label, use_container_width=True):
        st.markdown("**Add this chart to a draft report**")

        if drafts:
            st.caption("Pick an existing draft:")
            for d in drafts:
                did = d["draft_id"]
                n_slots = len(d.get("slots") or [])
                already = _chart_in_draft(d, source_key)
                full = n_slots >= MAX_REPORT_CHARTS and not already
                if already:
                    btn = f"✓ 📄 {d.get('name') or 'Untitled'} ({n_slots}/{MAX_REPORT_CHARTS})"
                elif full:
                    btn = f"⛔ 📄 {d.get('name') or 'Untitled'} (full)"
                else:
                    btn = f"📄 {d.get('name') or 'Untitled'} ({n_slots}/{MAX_REPORT_CHARTS})"
                if st.button(
                    btn,
                    key=f"addto_{q_rec['query_id']}_{idx}_{did}",
                    disabled=already or full,
                    use_container_width=True,
                ):
                    ok, msg = _add_chart_to_draft_by_id(did, q_rec, idx)
                    st.toast(msg, icon="✅" if ok else "⚠️")
                    if ok:
                        st.session_state.active_draft_id = did
                        st.rerun()
            st.divider()

        st.caption("…or start a new draft report:")
        new_name = st.text_input(
            "New draft name",
            placeholder="e.g., Q2 Power Rollout Status",
            key=f"newdraft_{q_rec['query_id']}_{idx}",
            label_visibility="collapsed",
        )
        if st.button(
            "➕ Create new draft & add",
            key=f"newbtn_{q_rec['query_id']}_{idx}",
            type="primary",
            use_container_width=True,
        ):
            if not new_name.strip():
                st.warning("Give the new draft a name first.")
            else:
                row = api_create_draft(user_id, new_name.strip(), project_type)
                if row:
                    ok, msg = _add_chart_to_draft(row, q_rec, idx)
                    st.session_state.active_draft_id = row["draft_id"]
                    st.toast(
                        f"Created “{new_name.strip()}” and added the chart." if ok else msg,
                        icon="✅" if ok else "⚠️",
                    )
                    st.rerun()


# ── LEFT (chat) · RIGHT (canvas) layout ────────────────────────────────────

active_draft = next((d for d in drafts if d["draft_id"] == st.session_state.active_draft_id), None)

col_chat, col_canvas = st.columns([3, 2], gap="large")


# ── LEFT: Chat column ───────────────────────────────────────────────────────

with col_chat:
    st.subheader("💬 Chat")

    if not st.session_state.queries:
        st.info("Ask a question below to get started.")

    for q_rec in st.session_state.queries:
        with st.container(border=True):
            st.markdown(f"**You:** {q_rec['query']}")
            if q_rec.get("rationale"):
                st.info(f"**AI:** {q_rec['rationale']}")
            charts = q_rec.get("charts") or []
            if not charts:
                st.warning("No charts were produced for this query.")
            for i, chart in enumerate(charts):
                with st.container(border=True):
                    _render_chart_card(q_rec, chart, i, active_draft)

    st.divider()
    with st.form("chat_form", clear_on_submit=True):
        new_q = st.text_area(
            "Ask another question",
            placeholder="e.g., What is the site completion rate by market for NTM projects?",
            height=80,
        )
        submitted = st.form_submit_button("Generate", type="primary")
    if submitted and new_q.strip():
        _run_query(new_q)
        st.rerun()
    elif submitted:
        st.warning("Please enter a question.")


# ── RIGHT: Canvas + Templates tabs ──────────────────────────────────────────

with col_canvas:
    # The list endpoint returns just {template_id, title} per row.
    # Each template card we want to display is hydrated via
    # GET /templates/{id}?user_id=... (ownership-checked, full state).
    try:
        _meta_tpls = api.list_templates(user_id)   # list[dict] — id+title
        templates_for_right = []
        for t in _meta_tpls:
            tid = t["template_id"]
            try:
                templates_for_right.append(api.get_template(tid, user_id=user_id).model_dump())
            except (ApiError, requests.RequestException):
                # Fall back to id+title only so the rest of the list still renders.
                templates_for_right.append({**t, "selections": []})
    except (ApiError, requests.RequestException):
        templates_for_right = []

    tab_canvas_pane, tab_templates_pane = st.tabs([
        f"🎨 Canvas ({len(drafts)})",
        f"📄 Templates ({len(templates_for_right)})",
    ])


# ============================================================================
# CANVAS TAB
# ============================================================================
with tab_canvas_pane:
    st.subheader("🎨 Report canvas")

    # ── Card-style list of all drafts at the top ──────────────────────────
    list_col, new_col = st.columns([4, 1])
    with new_col:
        with st.popover("➕ New draft", use_container_width=True):
            new_name = st.text_input("Draft name",
                                     placeholder="e.g., Q2 Power Rollout Status",
                                     key="_new_draft_name")
            if st.button("Create", type="primary", use_container_width=True):
                if not new_name.strip():
                    st.warning("Give the draft a name.")
                else:
                    row = api_create_draft(user_id, new_name.strip(), project_type)
                    if row:
                        st.session_state.active_draft_id = row["draft_id"]
                        st.toast(f"Created “{new_name.strip()}”", icon="✅")
                        st.rerun()
    with list_col:
        st.caption(f"📚 **My canvases** · {len(drafts)} total"
                   "  ·  📄 = this thread  ·  🧵 = from another thread")

    if drafts:
        # Render each draft as a clickable card row.
        for d in drafts:
            did = d["draft_id"]
            is_active = (did == st.session_state.active_draft_id)
            n_slots = len(d.get("slots") or [])
            updated = str(d.get("updated_at") or "")[:19]

            border_color = "#2563eb" if is_active else "#e5e7eb"
            bg = "rgba(37, 99, 235, 0.06)" if is_active else "transparent"
            with st.container(border=False):
                st.markdown(
                    f"<div style='padding:10px 12px;border:1px solid {border_color};"
                    f"border-radius:8px;background:{bg};margin-bottom:6px;"
                    f"display:flex;align-items:center;justify-content:space-between;'>"
                    f"<div><b>📄 {(d.get('name') or 'Untitled')}</b>"
                    f"  <span style='color:#6b7280;font-size:0.85em;'>"
                    f"· {n_slots}/{MAX_REPORT_CHARTS} charts · updated {updated}</span></div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                col_open, col_del = st.columns([5, 1])
                with col_open:
                    if not is_active:
                        if st.button(f"Open “{(d.get('name') or 'Untitled')[:30]}”",
                                     key=f"open_draft_{did}", use_container_width=True):
                            st.session_state.active_draft_id = did
                            st.rerun()
                    else:
                        st.caption("✅ Currently editing this canvas below.")
                with col_del:
                    pk = f"_draft_card_del_{did}"
                    if st.session_state.get(pk):
                        if st.button("✓", key=f"draft_card_del_ok_{did}",
                                     use_container_width=True, help="Confirm delete"):
                            api_delete_draft(did)
                            st.session_state.pop(pk, None)
                            if st.session_state.active_draft_id == did:
                                st.session_state.active_draft_id = None
                            st.toast("Draft deleted.", icon="🗑️")
                            st.rerun()
                    else:
                        if st.button("🗑", key=f"draft_card_del_{did}",
                                     use_container_width=True,
                                     help="Delete this draft (click again to confirm)"):
                            st.session_state[pk] = True
                            st.rerun()
    else:
        st.info("No canvas drafts yet. Use **➕ New draft** above, or click "
                "**Add to canvas** under any chart in the chat.")

    st.divider()

    # Refetch the active draft from the just-fetched drafts list
    active_draft = next((d for d in drafts if d["draft_id"] == st.session_state.active_draft_id), None)

    if not active_draft:
        with st.container(border=True):
            st.caption("Pick a draft above, or click **➕ New draft** to start one.")
    else:
        # Rename + delete controls
        rname_col, rdel_col = st.columns([4, 1])
        with rname_col:
            renamed = st.text_input("Draft name", value=active_draft.get("name") or "",
                                    key=f"rename_{active_draft['draft_id']}",
                                    label_visibility="collapsed")
            if renamed.strip() and renamed.strip() != active_draft.get("name"):
                api_patch_draft(active_draft["draft_id"], name=renamed.strip())
                st.rerun()
        with rdel_col:
            del_key = f"_draft_del_pending_{active_draft['draft_id']}"
            if st.session_state.get(del_key):
                if st.button("✓ Del", key=f"draft_del_ok_{active_draft['draft_id']}",
                             use_container_width=True):
                    api_delete_draft(active_draft["draft_id"])
                    st.session_state.pop(del_key, None)
                    st.session_state.active_draft_id = None
                    st.toast("Draft deleted.", icon="🗑️")
                    st.rerun()
            else:
                if st.button("🗑", key=f"draft_del_{active_draft['draft_id']}",
                             use_container_width=True, help="Delete this draft"):
                    st.session_state[del_key] = True
                    st.rerun()

        slots = active_draft.get("slots") or []
        if isinstance(slots, str):
            try:
                slots = json.loads(slots)
            except Exception:
                slots = []
        slots = sorted(slots, key=lambda s: int(s.get("position", 0)))
        for i, s in enumerate(slots):
            s["position"] = i

        st.caption(f"**{len(slots)} / {MAX_REPORT_CHARTS}** charts in **“{active_draft.get('name')}”**"
                   "  ·  _drag tiles with the mouse to reposition, drag a tile edge to resize. "
                   "Every move is saved instantly._")

        # 2D canvas — each chart is a draggable, resizable tile.
        if slots:
            render_gridstack_canvas(
                slots=slots,
                draft_id=active_draft["draft_id"],
                api_base=API_BASE,
                height=max(420, 80 * max([int(s.get("y", 0)) + int(s.get("h", 4))
                                          for s in slots], default=6) + 80),
            )
            if st.button("🔄 Pull latest layout from DB",
                         key=f"sync_layout_{active_draft['draft_id']}",
                         use_container_width=True,
                         help="After dragging tiles around, click this to refresh the rest of the page."):
                st.rerun()

        if not slots:
            st.info("Canvas is empty. Use **➕ Add to canvas** under any chart in the chat — "
                    "each new chart lands in the first free grid spot. After that, drag and "
                    "resize tiles here to arrange the page.")
        else:
            # ── Quick edit — select a canvas tile and patch it in-place ────
            # Edits flow through POST /charts/edit which updates the chart in
            # reporting_agent_queries.charts[idx] and propagates back into
            # every draft + linked template that holds this chart.
            sorted_slots = sorted(slots, key=lambda x: (int(x.get("y", 0)), int(x.get("x", 0))))
            # chart_id is the only handle the chart-edit API takes now.
            slot_titles: dict[str, str] = {}
            for s in sorted_slots:
                cid = _slot_chart_id(s)
                if not cid:
                    continue
                ch = s.get("chart") or {}
                title_obj = ch.get("title")
                t = (title_obj.get("text") if isinstance(title_obj, dict) else title_obj) or "Chart"
                slot_titles[cid] = t

            with st.container(border=True):
                st.markdown("##### ✏️ Edit a chart on the canvas")
                ed_sel, ed_btn = st.columns([4, 1])
                with ed_sel:
                    cid_options = list(slot_titles.keys())
                    picked_cid = st.selectbox(
                        "Which chart?",
                        options=cid_options,
                        format_func=lambda cid: f"{slot_titles[cid]}",
                        key=f"edit_slot_pick_{active_draft['draft_id']}",
                        label_visibility="collapsed",
                    )
                with ed_btn:
                    if st.button("✏️ Edit chart",
                                 key=f"edit_slot_btn_{active_draft['draft_id']}",
                                 use_container_width=True,
                                 help="Open the natural-language edit dialog for the picked chart"):
                        if picked_cid:
                            _edit_chart_dialog(picked_cid, slot_titles[picked_cid])

                st.caption("_Changes also update the same chart in the chat and in any finalized "
                           "template linked to this canvas._")

            # ── Per-tile details panel with inline edit buttons ───────────
            with st.expander("📋 Slot details (insight · script · edit)"):
                for s in sorted_slots:
                    cid = _slot_chart_id(s)
                    title = slot_titles.get(cid, "Chart")
                    cols_hdr, cols_edit = st.columns([5, 1])
                    with cols_hdr:
                        st.markdown(
                            f"**[{s.get('x', 0)},{s.get('y', 0)}] "
                            f"{s.get('w', '?')}×{s.get('h', '?')}** · {title}"
                        )
                    with cols_edit:
                        if cid and st.button(
                            "✏️ Edit",
                            key=f"edit_slot_inline_{active_draft['draft_id']}_{cid}",
                            use_container_width=True,
                        ):
                            _edit_chart_dialog(cid, title)
                    slot_insight = (s.get("chart") or {}).get("insight")
                    if slot_insight:
                        render_insight(slot_insight, header="💡 Insight")
                    # Script is now inside chart, not a separate `evidence` field.
                    script = (s.get("chart") or {}).get("script") or "(no script)"
                    st.code(script, language="python")
                    # Show edit history (if any) so the user sees what's been
                    # applied to this chart over time.
                    history = (s.get("chart") or {}).get("_edit_history") or []
                    if history:
                        st.caption(
                            f"Edits applied: {', '.join((h.get('instruction') or '')[:50] for h in history[-3:])}"
                            + (" …" if len(history) > 3 else "")
                        )
                    st.divider()

        st.divider()
        # Finalize — upserts by source_draft_id. If a template was already
        # finalized from this canvas, the existing template is updated in
        # place (title + selections) so we don't accumulate duplicates
        # every time the user clicks Finalize.
        existing_tpl_for_draft = next(
            (t for t in templates_for_right
             if t.get("source_draft_id") == active_draft["draft_id"]),
            None,
        )
        finalize_label = (
            f"💾 Update existing report: “{(existing_tpl_for_draft.get('title') or 'Untitled')[:30]}”"
            if existing_tpl_for_draft else "🆕 Finalize as new template"
        )
        default_title = (
            existing_tpl_for_draft.get("title") if existing_tpl_for_draft
            else (active_draft.get("name") or "")
        )
        with st.form(f"finalize_{active_draft['draft_id']}"):
            ftitle = st.text_input("Report title", value=default_title)
            finalize = st.form_submit_button(
                finalize_label,
                type="primary", use_container_width=True,
                disabled=not slots,
            )
        if finalize:
            # New API: just send {user_id, draft_id, title, project_type}.
            # The server pulls slots from the draft and copies them as the
            # template's selections by chart_id reference — no chart payload
            # duplication.
            try:
                action_resp = api.upsert_template(
                    user_id=user_id,
                    draft_id=active_draft["draft_id"],
                    title=ftitle,
                    project_type=project_type,
                )
                st.success(
                    f"Template **{action_resp.action}**. Open the **📄 Templates** tab to view it."
                )
                st.session_state.template_view = action_resp.template_id
                st.rerun()
            except ApiError as e:
                _show_api_error("Save template", e)
            except requests.RequestException as e:
                st.error(f"Save failed: {e}")


# ============================================================================
# TEMPLATES TAB
# ============================================================================
with tab_templates_pane:
    st.subheader("📄 Saved templates")

    tpls = templates_for_right  # already fetched above for the tab badge

    if not tpls:
        st.info("You haven't saved any templates yet. Build a canvas, click **Finalize**, and it lands here.")
    else:
        # Resolve which template is currently focused.
        tid = st.session_state.template_view
        valid_tids = {t["template_id"] for t in tpls}
        if tid and tid not in valid_tids:
            tid = None
            st.session_state.template_view = None
        if not tid:
            tid = tpls[0]["template_id"]
            st.session_state.template_view = tid

        st.caption(f"📚 **My templates** · {len(tpls)} total")

        # ── Card list of templates ──────────────────────────────────────
        for t in tpls:
            t_tid = t["template_id"]
            is_open = (t_tid == tid)
            sels = t.get("selections")
            if isinstance(sels, str):
                try:
                    sels = json.loads(sels)
                except Exception:
                    sels = []
            n_charts = len(sels or [])
            t_title = t.get("title") or "Untitled"
            t_proj = t.get("project_type") or "-"
            t_created = str(t.get("created_at") or "")[:19]
            t_last_run = str(t.get("last_run_at") or "")[:19] or "—"

            border_color = "#16a34a" if is_open else "#e5e7eb"
            bg = "rgba(22, 163, 74, 0.06)" if is_open else "transparent"
            st.markdown(
                f"<div style='padding:10px 12px;border:1px solid {border_color};"
                f"border-radius:8px;background:{bg};margin-bottom:6px;'>"
                f"<div style='font-weight:600;'>📄 {t_title}</div>"
                f"<div style='color:#6b7280;font-size:0.85em;margin-top:2px;'>"
                f"{n_charts} chart{'s' if n_charts != 1 else ''} · "
                f"project: <code>{t_proj}</code> · "
                f"created: <code>{t_created}</code> · "
                f"last run: <code>{t_last_run}</code>"
                f"</div></div>",
                unsafe_allow_html=True,
            )
            col_open, col_run, col_del = st.columns([3, 2, 1])
            with col_open:
                if not is_open:
                    if st.button(f"Open “{t_title[:30]}”",
                                 key=f"tpl_card_open_{t_tid}", use_container_width=True):
                        st.session_state.template_view = t_tid
                        st.rerun()
                else:
                    st.caption("✅ Currently viewing this template below.")
            with col_run:
                if st.button("▶ Re-run", key=f"tpl_card_run_{t_tid}",
                             type="primary", use_container_width=True):
                    with st.spinner("Re-running scripts…"):
                        try:
                            run = api.run_template(t_tid)
                            st.session_state[f"_rendered_{t_tid}"] = run.model_dump()
                            st.session_state.template_view = t_tid
                            st.toast("Refreshed.", icon="🔄")
                            st.rerun()
                        except ApiError as e:
                            _show_api_error("Re-run", e)
                        except requests.RequestException as e:
                            st.error(f"Re-run failed: {e}")
            with col_del:
                pk = f"_tpl_card_del_{t_tid}"
                if st.session_state.get(pk):
                    if st.button("✓", key=f"tpl_card_del_ok_{t_tid}",
                                 use_container_width=True, help="Confirm delete"):
                        try:
                            api.delete_template(t_tid)
                            st.session_state.pop(pk, None)
                            st.session_state.pop(f"_rendered_{t_tid}", None)
                            if st.session_state.template_view == t_tid:
                                st.session_state.template_view = None
                            st.toast("Template deleted.", icon="🗑️")
                            st.rerun()
                        except ApiError as e:
                            st.session_state.pop(pk, None)
                            _show_api_error("Delete template", e)
                        except requests.RequestException as e:
                            st.session_state.pop(pk, None)
                            st.error(f"Delete failed: {e}")
                else:
                    if st.button("🗑", key=f"tpl_card_del_{t_tid}",
                                 use_container_width=True,
                                 help="Delete (click again to confirm)"):
                        st.session_state[pk] = True
                        st.rerun()

        st.divider()

        # ── Render the currently-open template below the list ───────────
        # `templates_for_right` already includes selections (we hydrated
        # the list above), so no extra fetch needed for the open template.
        meta: dict = next(
            (t for t in templates_for_right if t.get("template_id") == tid),
            {},
        )

        if meta:
            st.markdown(f"### 📄 {meta.get('title', '')}")
            st.caption(
                f"project_type: `{meta.get('project_type') or '-'}` · "
                f"created: `{meta.get('created_at', '')}` · "
                f"last_run: `{meta.get('last_run_at', '')}`"
            )

            selections = sorted(
                meta.get("selections") or [],
                key=lambda s: int(s.get("position", 0)
                                  if s.get("position") is not None
                                  else 0),
            )

            # `last_rendered` cache was removed — show the saved canvas
            # snapshot from `selections` by default. If the user just clicked
            # "Re-run", `_rendered_{tid}` carries the freshly rebuilt
            # SELECTIONS (each with its rebuilt chart) for this session.
            rendered = st.session_state.get(f"_rendered_{tid}") or {}
            rendered_selections = rendered.get("selections") or []
            if rendered_selections:
                # Sort the rerun result the same way we sort saved selections.
                rendered_selections = sorted(
                    rendered_selections,
                    key=lambda s: int(s.get("position") if s.get("position") is not None else 0),
                )
                # The rerun's selections take precedence over the saved snapshot
                # — same layout, fresh chart contents.
                selections = rendered_selections
            charts_to_show = [s.get("chart") for s in selections if s.get("chart")]

            reports = (rendered or {}).get("script_reports") or []
            if reports:
                oks = sum(1 for r in reports if r.get("status") == "success")
                errs = len(reports) - oks
                st.caption(f"Scripts: {oks} succeeded, {errs} failed")
                with st.expander("Script run report"):
                    for r in reports:
                        st.write(r)

            if not charts_to_show:
                st.warning("This template has no charts to render.")
            else:
                # Render at saved x/y/w/h so the page reproduces the canvas layout.
                layout_slots = []
                for i, c in enumerate(charts_to_show):
                    sel = selections[i] if i < len(selections) else {}
                    x = int(sel.get("x", (i % 2) * 6) or 0)
                    y = int(sel.get("y", (i // 2) * 4) or 0)
                    w = int(sel.get("w", 6) or 6)
                    h = int(sel.get("h", 4) or 4)
                    layout_slots.append({"x": x, "y": y, "w": w, "h": h, "chart": c})
                grid_height = max(420,
                                  80 * max([s["y"] + s["h"] for s in layout_slots], default=6) + 80)
                render_readonly_grid(
                    slots=layout_slots,
                    container_key=f"tpl-{tid}",
                    height=grid_height,
                )

                with st.expander("📋 Chart details (description · insight · script)"):
                    for i, chart in enumerate(charts_to_show):
                        title = "Chart"
                        if isinstance(chart.get("title"), dict):
                            title = chart["title"].get("text", title)
                        sel = selections[i] if i < len(selections) else {}
                        st.markdown(
                            f"**[{sel.get('x', 0)},{sel.get('y', 0)}] "
                            f"{sel.get('w', '?')}×{sel.get('h', '?')}** · {title}"
                        )
                        if chart.get("description"):
                            st.caption(chart["description"])
                        ins = chart.get("insight") or (sel or {}).get("chart", {}).get("insight")
                        if ins:
                            header = ("💡 Insight (refreshed against today's data)"
                                      if (chart.get("_insight_history") or [])
                                      else "💡 Insight")
                            render_insight(ins, header=header)
                        # Script lives inside chart now (chart.script);
                        # legacy evidence shape kept as a fallback.
                        sel_chart = (sel or {}).get("chart") or {}
                        script = sel_chart.get("script") or ((sel or {}).get("evidence") or {}).get("code") or "(no script)"
                        st.markdown("**🐍 Python + SQL**")
                        st.code(script, language="python")
                        st.divider()
