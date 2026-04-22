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

try:
    from streamlit_sortables import sort_items  # type: ignore[import-unresolved]
    _HAVE_SORTABLE = True
except Exception:
    _HAVE_SORTABLE = False


# ── Configuration ───────────────────────────────────────────────────────────

API_BASE = "http://localhost:8002/api/v1"
PROJECT_TYPES = ["NTM", "AHLOB Modernization", "Both"]
MAX_REPORT_CHARTS = 6


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
        title = ""
        if isinstance((s.get("chart") or {}).get("title"), dict):
            title = (s["chart"]["title"].get("text") or "")
        title_safe = (title or "Chart")[:80].replace('"', '&quot;')
        tiles.append(f"""
          <div class="grid-stack-item" gs-x="{x}" gs-y="{y}" gs-w="{w}" gs-h="{h}" gs-no-resize gs-no-move>
            <div class="grid-stack-item-content tile">
              <div class="tile-head"><span class="tile-title" title="{title_safe}">{title_safe}</span></div>
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
          overflow: hidden; display: flex; flex-direction: column;
        }}
        .tile-head {{
          padding: 6px 10px; border-bottom: 1px solid #f3f4f6;
          font-weight: 600; font-size: 13px; background: #fafafa;
          white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }}
        .chart-host {{ flex: 1; padding: 4px; min-height: 0; }}
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
    # Build one gridstack-item per slot. The chart config is embedded as a
    # JSON string and rendered with the same Highcharts script we use
    # elsewhere — so tiles look identical to the chat chart cards.
    items_html = []
    for s in slots:
        x = int(s.get("x", 0) or 0)
        y = int(s.get("y", 0) or 0)
        w = int(s.get("w", 6) or 6)
        h = int(s.get("h", 4) or 4)
        source = s.get("source") or f"{s.get('query_id', '')}:{s.get('chart_index', '')}"
        chart_json = json.dumps(s.get("chart") or {}, default=str)
        title = ""
        if isinstance((s.get("chart") or {}).get("title"), dict):
            title = (s["chart"]["title"].get("text") or "")
        title_safe = (title or "Chart")[:80].replace('"', '&quot;')
        slot_json = json.dumps(s, default=str)
        items_html.append(f"""
          <div class="grid-stack-item"
               gs-x="{x}" gs-y="{y}" gs-w="{w}" gs-h="{h}"
               gs-id="{source}"
               data-slot='{slot_json}'>
            <div class="grid-stack-item-content tile">
              <div class="tile-head">
                <span class="tile-title" title="{title_safe}">{title_safe}</span>
                <button class="tile-rm" data-source="{source}" title="Remove from canvas">✕</button>
              </div>
              <div class="chart-host" id="chart-{source}"></div>
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
          display: flex; flex-direction: column; cursor: move;
        }}
        .tile-head {{
          padding: 6px 10px; border-bottom: 1px solid #f3f4f6;
          display: flex; justify-content: space-between; align-items: center;
          font-weight: 600; font-size: 13px; background: #fafafa;
        }}
        .tile-title {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .tile-rm {{
          border: 0; background: transparent; cursor: pointer; color: #991b1b;
          font-size: 14px; padding: 0 4px;
        }}
        .tile-rm:hover {{ color: #dc2626; }}
        .chart-host {{ flex: 1; padding: 4px; min-height: 0; }}
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
          const src = s.source || (s.query_id + ':' + s.chart_index);
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
            const src = s.source || (s.query_id + ':' + s.chart_index);
            const pos = byId[src];
            if (!pos) return s;  // removed
            return Object.assign({{}}, s, {{
              x: pos.x, y: pos.y, w: pos.w, h: pos.h,
            }});
          }}).filter(s => {{
            const src = s.source || (s.query_id + ':' + s.chart_index);
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


# ── Canvas API helpers ──────────────────────────────────────────────────────

def api_list_drafts(user_id: str, thread_id: str | None = None):
    """Return all drafts for a user. If thread_id is provided, only drafts in
    that thread. If None, every draft the user has across all threads."""
    try:
        params = {"user_id": user_id}
        if thread_id:
            params["thread_id"] = thread_id
        r = requests.get(f"{API_BASE}/canvas/drafts", params=params, timeout=10)
        return r.json().get("drafts", []) if r.status_code == 200 else []
    except Exception:
        return []


def api_create_draft(user_id: str, username: str, thread_id: str, name: str, project_type: str):
    r = requests.post(
        f"{API_BASE}/canvas/drafts",
        json={
            "user_id":   user_id,
            "username":  username,
            "thread_id": thread_id,
            "name":      name,
            "project_type": project_type,
        },
        timeout=10,
    )
    if r.status_code != 200:
        st.error(f"Create draft failed ({r.status_code}): {r.text[:200]}")
        return None
    return r.json()


def api_patch_draft(draft_id: str, name=None, slots=None):
    body = {}
    if name is not None:
        body["name"] = name
    if slots is not None:
        body["slots"] = _sanitize_for_json(slots)
    r = requests.patch(f"{API_BASE}/canvas/drafts/{draft_id}", json=body, timeout=15)
    if r.status_code != 200:
        st.error(f"Update draft failed ({r.status_code}): {r.text[:200]}")
        return None
    return r.json()


def api_delete_draft(draft_id: str):
    r = requests.delete(f"{API_BASE}/canvas/drafts/{draft_id}", timeout=10)
    return r.status_code == 200


def api_list_thread_queries(thread_id: str):
    try:
        r = requests.get(f"{API_BASE}/threads/{thread_id}/queries", timeout=10)
        return r.json().get("queries", []) if r.status_code == 200 else []
    except Exception:
        return []


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

with st.sidebar:
    st.header("Settings")
    user_id = st.text_input("User ID", value="demo_user")
    username = st.text_input("Username", value="Demo User")
    project_type = st.selectbox("Project Type", options=PROJECT_TYPES, index=0)
    max_charts = st.slider("Max charts per query", 1, 5, 3)

    st.divider()
    st.subheader("Thread")
    st.caption(f"`{st.session_state.thread_id[:8]}…`")
    if st.button("New thread", use_container_width=True):
        st.session_state.thread_id = str(uuid.uuid4())
        st.session_state.queries = []
        st.session_state.active_draft_id = None
        st.session_state.template_view = None
        st.rerun()

    st.divider()
    st.subheader("Saved templates")
    try:
        t_resp = requests.get(f"{API_BASE}/templates", params={"user_id": user_id}, timeout=10)
        templates = t_resp.json().get("templates", []) if t_resp.status_code == 200 else []
    except Exception:
        templates = []

    if not templates:
        st.caption("No saved templates yet.")
    for t in templates:
        tid = t["template_id"]
        label = (t.get("title") or "Untitled")[:40]
        col_open, col_del = st.columns([5, 1])
        with col_open:
            if st.button(f"📄 {label}", key=f"tpl_open_{tid}", use_container_width=True):
                st.session_state.template_view = tid
                st.rerun()
        with col_del:
            pending_key = f"_tpl_delete_pending_{tid}"
            if st.session_state.get(pending_key):
                if st.button("✓", key=f"tpl_del_confirm_{tid}", use_container_width=True):
                    try:
                        r = requests.delete(f"{API_BASE}/templates/{tid}", timeout=15)
                        if r.status_code == 200:
                            st.session_state.pop(pending_key, None)
                            if st.session_state.get("template_view") == tid:
                                st.session_state.template_view = None
                            st.toast("Template deleted.", icon="🗑️")
                            st.rerun()
                        else:
                            st.session_state.pop(pending_key, None)
                            st.error(f"Delete failed ({r.status_code}): {r.text[:200]}")
                    except Exception as e:
                        st.session_state.pop(pending_key, None)
                        st.error(f"Delete failed: {e}")
            else:
                if st.button("🗑", key=f"tpl_del_{tid}", use_container_width=True):
                    st.session_state[pending_key] = True
                    st.rerun()


# ── Rehydrate chat from the DB ──────────────────────────────────────────────

if not st.session_state.queries:
    server_rows = api_list_thread_queries(st.session_state.thread_id)
    rebuilt = []
    for row in server_rows:
        charts = row.get("charts") or []
        if isinstance(charts, str):
            try:
                charts = json.loads(charts)
            except Exception:
                charts = []
        evidence = row.get("evidence") or []
        if isinstance(evidence, str):
            try:
                evidence = json.loads(evidence)
            except Exception:
                evidence = []
        rebuilt.append({
            "query_id":           row.get("query_id") or str(uuid.uuid4()),
            "query":              row.get("original_query") or "",
            "project_type":       row.get("project_type") or "",
            "charts":             charts,
            "rationale":          row.get("rationale") or "",
            "traversal_steps":    row.get("traversal_steps") or 0,
            "traversal_findings": row.get("traversal_findings") or "",
            "evidence":           evidence,
            "retrieval_nodes":    [],
            "retrieval_paths":    [],
        })
    if rebuilt:
        st.session_state.queries = rebuilt


# ── Load drafts for this user (always fresh, never cached in memory) ──────
# Drafts are user-scoped: every canvas the user has built across any thread
# shows up here so they can drop new charts into an old report, not just the
# one tied to the current chat.

drafts = api_list_drafts(user_id, thread_id=None)
current_thread = st.session_state.thread_id

# If the stored active_draft_id no longer exists, clear it.
valid_draft_ids = {d["draft_id"] for d in drafts}
if st.session_state.active_draft_id and st.session_state.active_draft_id not in valid_draft_ids:
    st.session_state.active_draft_id = None
if not st.session_state.active_draft_id and drafts:
    # Prefer a draft in the current thread, else most-recently-updated overall.
    in_thread = [d for d in drafts if d.get("thread_id") == current_thread]
    st.session_state.active_draft_id = (in_thread or drafts)[0]["draft_id"]


# ── Helpers used by chart cards ─────────────────────────────────────────────

def _slot_source(s: dict) -> str:
    """Return the stable dedupe key for a slot.

    Older slots (and slots written directly via the API without the UI) may
    not carry a `source` field — `CanvasSlot` doesn't declare one. The key is
    always recoverable from query_id + chart_index, so compute it if absent.
    """
    sk = s.get("source")
    if sk:
        return sk
    return f"{s.get('query_id', '')}:{s.get('chart_index', '')}"


def _chart_in_draft(draft: dict, source_key: str) -> bool:
    return any(_slot_source(s) == source_key for s in (draft.get("slots") or []))


def _add_chart_to_draft(draft: dict, q_rec: dict, chart_idx: int):
    slots = list(draft.get("slots") or [])
    chart = q_rec["charts"][chart_idx]
    source_key = f"{q_rec['query_id']}:{chart_idx}"

    if any(_slot_source(s) == source_key for s in slots):
        return False, "Already in this draft."
    if len(slots) >= MAX_REPORT_CHARTS:
        return False, f"Draft is full ({MAX_REPORT_CHARTS} max)."

    slots.append({
        "position":       len(slots),
        "source":         source_key,
        "query_id":       q_rec["query_id"],
        "chart_index":    chart_idx,
        "original_query": q_rec.get("query", ""),
        "chart":          chart,
        "evidence":       chart.get("evidence") or {},
    })
    resp = api_patch_draft(draft["draft_id"], slots=slots)
    return resp is not None, "Added."


def _remove_slot(draft_id: str, slots: list, position: int):
    new_slots = [s for s in slots if s.get("position") != position]
    for i, s in enumerate(new_slots):
        s["position"] = i
    api_patch_draft(draft_id, slots=new_slots)


def _sync_chart_everywhere_in_db(query_id: str, chart_index: int, patched_chart: dict):
    """Chart edit → propagate into every draft that contains this chart."""
    # Update the in-session query record
    for q in st.session_state.queries:
        if q.get("query_id") == query_id:
            if 0 <= chart_index < len(q.get("charts", [])):
                q["charts"][chart_index] = patched_chart
            break
    source_key = f"{query_id}:{chart_index}"
    # Propagate to every draft in the thread
    for d in drafts:
        slots = d.get("slots") or []
        if isinstance(slots, str):
            try:
                slots = json.loads(slots)
            except Exception:
                slots = []
        touched = False
        for s in slots:
            if _slot_source(s) == source_key:
                s["chart"] = patched_chart
                touched = True
        if touched:
            api_patch_draft(d["draft_id"], slots=slots)


@st.dialog("Edit chart")
def _edit_chart_dialog(query_id: str, chart_index: int, chart_title: str):
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
        key=f"edit_input_{query_id}_{chart_index}",
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
                resp = requests.post(
                    f"{API_BASE}/charts/edit",
                    json={"query_id": query_id, "chart_index": chart_index, "instruction": instruction.strip()},
                    timeout=90,
                )
            if resp.status_code != 200:
                st.error(f"Edit failed ({resp.status_code}): {resp.text[:300]}")
                st.stop()
            patched = (resp.json() or {}).get("chart") or {}
            _sync_chart_everywhere_in_db(query_id, chart_index, patched)
            st.success("Edit applied and saved.")
            st.rerun()
        except Exception as e:
            st.error(f"Edit failed: {e}")


# ── Run a new query ─────────────────────────────────────────────────────────

def _run_query(query: str):
    params = urllib.parse.urlencode({
        "query": query.strip(),
        "project_type": project_type,
        "user_id": user_id,
        "username": username,
        "max_charts": max_charts,
        "thread_id": st.session_state.thread_id,
    })
    sse_url = f"{API_BASE}/report/stream?{params}"

    progress = st.progress(0.0, text="Starting...")
    status = st.empty()
    result_payload = None
    query_id = None

    try:
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
                    total = data.get("total", 4)
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
        st.session_state.queries.append({
            "query_id": query_id or str(uuid.uuid4()),
            "query": query.strip(),
            "project_type": project_type,
            "charts": result_payload.get("charts", []),
            "rationale": result_payload.get("rationale", ""),
            "traversal_steps": result_payload.get("traversal_steps", 0),
            "traversal_findings": result_payload.get("traversal_findings", ""),
            "evidence": result_payload.get("evidence", []),
            "retrieval_nodes": result_payload.get("retrieval_nodes", []),
            "retrieval_paths": result_payload.get("retrieval_paths", []),
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

    ev = chart.get("evidence") or {}
    code = ev.get("code") or "(no script captured)"
    with st.expander("🐍 Python + SQL script"):
        st.code(code, language="python")
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

    col_a, col_b = st.columns([1, 1])
    with col_a:
        with st.popover(popover_label, use_container_width=True):
            st.markdown("**Add this chart to a draft report**")

            if drafts:
                st.caption("Pick an existing draft:")
                for d in drafts:
                    did = d["draft_id"]
                    n_slots = len(d.get("slots") or [])
                    from_other_thread = d.get("thread_id") != current_thread
                    already = _chart_in_draft(d, source_key)
                    full = n_slots >= MAX_REPORT_CHARTS and not already
                    dot = "🧵" if from_other_thread else "📄"
                    if already:
                        btn = f"✓ {dot} {d.get('name') or 'Untitled'} ({n_slots}/{MAX_REPORT_CHARTS})"
                    elif full:
                        btn = f"⛔ {dot} {d.get('name') or 'Untitled'} (full)"
                    else:
                        btn = f"{dot} {d.get('name') or 'Untitled'} ({n_slots}/{MAX_REPORT_CHARTS})"
                    if st.button(
                        btn,
                        key=f"addto_{q_rec['query_id']}_{idx}_{did}",
                        disabled=already or full,
                        use_container_width=True,
                        help=("From another thread" if from_other_thread else None),
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
                    row = api_create_draft(
                        user_id, username, st.session_state.thread_id,
                        new_name.strip(), project_type,
                    )
                    if row:
                        ok, msg = _add_chart_to_draft(row, q_rec, idx)
                        st.session_state.active_draft_id = row["draft_id"]
                        st.toast(
                            f"Created “{new_name.strip()}” and added the chart." if ok else msg,
                            icon="✅" if ok else "⚠️",
                        )
                        st.rerun()

    with col_b:
        if st.button("✏️ Edit chart", key=f"edit_btn_{q_rec['query_id']}_{idx}",
                     use_container_width=True):
            _edit_chart_dialog(q_rec["query_id"], idx, title)


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
                st.info(f"**Insight:** {q_rec['rationale']}")
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


# ── RIGHT: Canvas column ────────────────────────────────────────────────────

with col_canvas:
    st.subheader("🎨 Report canvas")

    # Draft selector + "new draft" popover
    sel_col, new_col = st.columns([3, 2])
    with sel_col:
        if drafts:
            draft_ids = [d["draft_id"] for d in drafts]
            labels = {}
            for d in drafts:
                dot = "📄" if d.get("thread_id") == current_thread else "🧵"
                labels[d["draft_id"]] = (
                    f"{dot} {(d.get('name') or 'Untitled')[:28]} "
                    f"({len(d.get('slots') or [])}/{MAX_REPORT_CHARTS})"
                )
            idx = draft_ids.index(st.session_state.active_draft_id) if st.session_state.active_draft_id in draft_ids else 0
            picked = st.selectbox(
                f"Active draft ({len(drafts)} total)",
                options=draft_ids,
                index=idx,
                format_func=lambda x: labels[x],
                label_visibility="collapsed",
                help="📄 = this thread · 🧵 = from another thread",
            )
            if picked != st.session_state.active_draft_id:
                st.session_state.active_draft_id = picked
                st.rerun()
        else:
            st.caption("No drafts yet — click **➕ New draft** or use **Add to canvas** from any chart.")
    with new_col:
        with st.popover("➕ New draft", use_container_width=True):
            new_name = st.text_input("Draft name", placeholder="e.g., Q2 Power Rollout Status",
                                     key="_new_draft_name")
            if st.button("Create", type="primary", use_container_width=True):
                if not new_name.strip():
                    st.warning("Give the draft a name.")
                else:
                    row = api_create_draft(user_id, username, st.session_state.thread_id,
                                           new_name.strip(), project_type)
                    if row:
                        st.session_state.active_draft_id = row["draft_id"]
                        st.toast(f"Created “{new_name.strip()}”", icon="✅")
                        st.rerun()

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
            # Compact slot list below the canvas so the user can see insight /
            # script details without re-opening each tile. No drag here — the
            # 2D canvas above is the single source of truth for positioning.
            with st.expander("📋 Slot details (insight + script per tile)"):
                for s in sorted(slots, key=lambda x: (int(x.get("y", 0)), int(x.get("x", 0)))):
                    st.markdown(
                        f"**[{s.get('x', 0)},{s.get('y', 0)}] "
                        f"{s.get('w', '?')}×{s.get('h', '?')}** · "
                        f"{((s.get('chart') or {}).get('title') or {}).get('text', 'chart')}"
                    )
                    slot_insight = (s.get("chart") or {}).get("insight")
                    if slot_insight:
                        render_insight(slot_insight, header="💡 Insight")
                    ev = s.get("evidence") or {}
                    st.code(ev.get("code") or "(no script)", language="python")
                    st.divider()

        st.divider()
        # Finalize
        with st.form(f"finalize_{active_draft['draft_id']}"):
            ftitle = st.text_input("Report title", value=active_draft.get("name") or "")
            finalize = st.form_submit_button("Finalize report (save as template)",
                                             type="primary", use_container_width=True,
                                             disabled=not slots)
        if finalize:
            # `slots` is already sorted by .position from the drag-drop handler.
            # `source_draft_id` links the finalized template back to this
            # canvas so later layout changes propagate automatically.
            payload = {
                "user_id": user_id,
                "username": username,
                "thread_id": st.session_state.thread_id,
                "title": ftitle,
                "project_type": project_type,
                "source_draft_id": active_draft["draft_id"],
                "selections": [
                    {
                        "position": s.get("position", i),
                        "query_id": s["query_id"],
                        "chart_index": s["chart_index"],
                        "chart": s["chart"],
                        "evidence": s.get("evidence") or {},
                        "original_query": s.get("original_query", ""),
                    } for i, s in enumerate(slots)
                ],
            }
            try:
                resp = requests.post(f"{API_BASE}/templates",
                                     json=_sanitize_for_json(payload), timeout=30)
                if resp.status_code == 200:
                    tid = resp.json().get("template_id")
                    st.success("Template saved. Scroll down to the Saved template view.")
                    st.session_state.template_view = tid
                else:
                    st.error(f"Save failed ({resp.status_code}): {resp.text[:200]}")
            except Exception as e:
                st.error(f"Save failed: {e}")


# ── Bottom: Saved template view ─────────────────────────────────────────────

st.divider()
with st.expander("📄 Saved template view", expanded=bool(st.session_state.template_view)):
    try:
        r = requests.get(f"{API_BASE}/templates", params={"user_id": user_id}, timeout=10)
        tpls = r.json().get("templates", []) if r.status_code == 200 else []
    except Exception:
        tpls = []

    if not tpls:
        st.info("You haven't saved any templates yet.")
    else:
        ids = [t["template_id"] for t in tpls]
        titles = {t["template_id"]: (t.get("title") or "Untitled") for t in tpls}
        tid = st.session_state.template_view
        if tid and tid not in ids:
            tid = None
        if not tid:
            tid = ids[0]
            st.session_state.template_view = tid

        picked = st.selectbox(
            "Template",
            options=ids,
            index=ids.index(tid),
            format_func=lambda x: f"📄 {titles[x]}",
        )
        if picked != tid:
            st.session_state.template_view = picked
            tid = picked
            st.rerun()

        head, btn, btn_del = st.columns([3, 1, 1])
        with head:
            st.markdown(f"### {titles[tid]}")
        with btn:
            re_run = st.button("▶ Re-run with today's data", type="primary",
                               use_container_width=True)
        with btn_del:
            del_key = f"_tpl_view_del_{tid}"
            if st.session_state.get(del_key):
                if st.button("✓ Confirm", key=f"tv_ok_{tid}", use_container_width=True):
                    try:
                        r = requests.delete(f"{API_BASE}/templates/{tid}", timeout=15)
                        if r.status_code == 200:
                            st.session_state.pop(del_key, None)
                            st.session_state.pop(f"_rendered_{tid}", None)
                            st.session_state.template_view = None
                            st.toast("Template deleted.", icon="🗑️")
                            st.rerun()
                        else:
                            st.session_state.pop(del_key, None)
                            st.error(f"Delete failed ({r.status_code}): {r.text[:200]}")
                    except Exception as e:
                        st.session_state.pop(del_key, None)
                        st.error(f"Delete failed: {e}")
            else:
                if st.button("🗑 Delete", key=f"tv_del_{tid}", use_container_width=True):
                    st.session_state[del_key] = True
                    st.rerun()

        # Fetch metadata
        meta = {}
        try:
            mr = requests.get(f"{API_BASE}/templates/{tid}", timeout=15)
            if mr.status_code == 200:
                meta = mr.json()
        except Exception as e:
            st.error(f"Could not load template: {e}")

        if meta:
            st.caption(
                f"project_type: `{meta.get('project_type') or '-'}` · "
                f"created: `{meta.get('created_at', '')}` · "
                f"last_run: `{meta.get('last_run_at', '')}`"
            )
            selections = meta.get("selections") or []
            if isinstance(selections, str):
                try:
                    selections = json.loads(selections)
                except Exception:
                    selections = []
            # Respect saved drag-drop order. Fallback to current index for
            # templates saved before positions were persisted.
            selections = sorted(
                selections,
                key=lambda s: int(s.get("position", 0)
                                  if s.get("position") is not None
                                  else selections.index(s)),
            )

            if re_run:
                with st.spinner("Re-running all scripts..."):
                    try:
                        resp = requests.post(f"{API_BASE}/templates/{tid}/run", timeout=600)
                        if resp.status_code == 200:
                            st.session_state[f"_rendered_{tid}"] = resp.json()
                            st.success("Refreshed.")
                        else:
                            st.error(f"Re-run failed ({resp.status_code}): {resp.text[:200]}")
                    except Exception as e:
                        st.error(f"Re-run failed: {e}")

            rendered = st.session_state.get(f"_rendered_{tid}")
            if rendered is None:
                last = meta.get("last_rendered")
                if isinstance(last, str):
                    try:
                        last = json.loads(last)
                    except Exception:
                        last = None
                if isinstance(last, dict):
                    rendered = last

            charts_to_show = (rendered or {}).get("charts") or []
            if not charts_to_show and selections:
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
                # Render charts at their saved x/y/w/h so the re-run page
                # reproduces the exact layout the user arranged on the canvas.
                layout_slots = []
                for i, c in enumerate(charts_to_show):
                    sel = selections[i] if i < len(selections) else {}
                    # Prefer layout from the selection (drag-drop at finalize
                    # time); fall back to a sensible default if missing.
                    x = int(sel.get("x", (i % 2) * 6) or 0)
                    y = int(sel.get("y", (i // 2) * 4) or 0)
                    w = int(sel.get("w", 6) or 6)
                    h = int(sel.get("h", 4) or 4)
                    layout_slots.append({
                        "x": x, "y": y, "w": w, "h": h,
                        "chart": c,
                    })
                grid_height = max(
                    420,
                    80 * max([s["y"] + s["h"] for s in layout_slots], default=6) + 80,
                )
                render_readonly_grid(
                    slots=layout_slots,
                    container_key=f"tpl-{tid}",
                    height=grid_height,
                )

                # Detail panel underneath: description / insight / script per chart.
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
                        ev = (sel or {}).get("evidence") or {}
                        st.markdown("**🐍 Python + SQL**")
                        st.code(ev.get("code") or "(no script)", language="python")
                        if ev.get("result") is not None:
                            with st.expander("🗂 Evidence rows (saved snapshot)"):
                                st.json(ev.get("result"))
                        st.divider()
