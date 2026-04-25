"""
Streamlit UI for the Reporting Agent — single-file, self-contained.

Talks to every API endpoint the FastAPI backend exposes. Use it to:
  • ask questions and watch the SSE stream
  • browse threads + their chat-style message history
  • build canvas drafts (free-form drag/drop)
  • save canvases as templates, re-run them, download as HTML/PDF
  • inspect every API response in a "Last response" expander per call

There is NO separate `ui/` package — everything (HTTP helpers, view-model
shaping, rendering) lives in this file so the app is easy to read end-to-end.

Run: streamlit run streamlit_app.py
"""
from __future__ import annotations

import json
import math
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

import requests
import streamlit as st  # type: ignore
import streamlit.components.v1 as components  # type: ignore

# ── Configuration ───────────────────────────────────────────────────────────

API_BASE = "http://localhost:8002/api/v1"
PROJECT_TYPES = ["NTM", "AHLOB Modernization", "Both"]
MAX_REPORT_CHARTS = 6


# ── HTTP helpers (no ApiClient class — just functions) ─────────────────────

# A persistent Session keeps TCP connections warm across reruns.
@st.cache_resource
def _session() -> requests.Session:
    return requests.Session()


def _request(method: str, path: str, *, params=None, json_body=None, timeout=15.0) -> tuple[int, Any, str | None]:
    """Single API call. Returns (status, body_or_dict, error_msg).

    On success: returns (200..299, parsed JSON dict/list, None).
    On API error: returns (status, parsed_or_text, short_error_msg).
    On network error: returns (0, None, error_msg).
    """
    url = f"{API_BASE}{path}"
    try:
        r = _session().request(method, url, params=params, json=json_body, timeout=timeout)
    except requests.RequestException as e:
        _record_call(method, path, params, json_body, status="ERR", elapsed_ms=0, body=None, error=str(e))
        return 0, None, f"network: {e}"

    elapsed = r.elapsed.total_seconds() * 1000
    body: Any = None
    try:
        body = r.json() if r.content else None
    except Exception:
        body = r.text

    _record_call(method, path, params, json_body, status=r.status_code, elapsed_ms=elapsed, body=body, error=None)

    if 200 <= r.status_code < 300:
        return r.status_code, body, None
    err = f"HTTP {r.status_code}"
    if isinstance(body, dict):
        d = body.get("detail")
        if isinstance(d, str):
            err += f" — {d}"
        elif isinstance(d, list) and d and isinstance(d[0], dict):
            loc = ".".join(str(x) for x in (d[0].get("loc") or []))
            err += f" — {loc}: {d[0].get('msg')}"
    return r.status_code, body, err


def _record_call(method, path, params, json_body, *, status, elapsed_ms, body, error):
    """Append the call to the in-memory trace ring shown in the debug pane."""
    if "_api_trace" not in st.session_state:
        st.session_state._api_trace = []
    st.session_state._api_trace.insert(0, {
        "ts":          time.strftime("%H:%M:%S"),
        "method":      method.upper(),
        "path":        path,
        "params":      params,
        "request":     json_body,
        "status":      status,
        "elapsed_ms":  round(elapsed_ms, 1),
        "response":    body,
        "error":       error,
    })
    del st.session_state._api_trace[200:]


# Typed wrappers (one per endpoint) — every response from the server passes
# through one of these so there's a single audit point.

def api_health() -> tuple[int, Any, str | None]:
    return _request("GET", "/health/")

# Threads
def api_list_threads(user_id: str, limit: int = 50):
    return _request("GET", "/threads", params={"user_id": user_id, "limit": limit})

def api_thread_messages(thread_id: str, user_id: str, limit: int = 100):
    return _request("GET", f"/threads/{thread_id}/messages",
                    params={"user_id": user_id, "limit": limit})

# Charts
def api_chart(chart_id: str):
    return _request("GET", f"/charts/{chart_id}")

def api_charts_by_query(query_id: str):
    return _request("GET", "/charts", params={"query_id": query_id})

def api_charts_by_user(user_id: str, limit: int = 100):
    return _request("GET", "/charts", params={"user_id": user_id, "limit": limit})

def api_charts_by_thread(thread_id: str, limit: int = 100):
    return _request("GET", "/charts", params={"thread_id": thread_id, "limit": limit})

def api_chart_edit(chart_id: str, instruction: str):
    return _request("POST", "/charts/edit",
                    json_body={"chart_id": chart_id, "instruction": instruction},
                    timeout=120)

def api_chart_edit_history(chart_id: str, limit: int = 20):
    return _request("GET", "/charts/edits", params={"chart_id": chart_id, "limit": limit})

# Canvas
def api_list_drafts(user_id: str, limit: int = 50):
    return _request("GET", "/canvas/drafts", params={"user_id": user_id, "limit": limit})

def api_get_draft(draft_id: str):
    return _request("GET", f"/canvas/drafts/{draft_id}")

def api_create_draft(user_id: str, name: str, project_type: str = ""):
    return _request("POST", "/canvas/drafts",
                    json_body={"user_id": user_id, "name": name, "project_type": project_type})

def api_patch_draft(draft_id: str, *, name: str | None = None, slots: list | None = None):
    body: dict[str, Any] = {}
    if name is not None:  body["name"] = name
    if slots is not None: body["slots"] = slots
    return _request("PATCH", f"/canvas/drafts/{draft_id}", json_body=body, timeout=30)

def api_delete_draft(draft_id: str):
    return _request("DELETE", f"/canvas/drafts/{draft_id}")

# Templates
def api_list_templates(user_id: str, limit: int = 50):
    return _request("GET", "/templates", params={"user_id": user_id, "limit": limit})

def api_get_template(template_id: str, user_id: str):
    return _request("GET", f"/templates/{template_id}", params={"user_id": user_id})

def api_create_template(user_id: str, draft_id: str,
                        title: str | None = None, project_type: str | None = None):
    return _request("POST", "/templates",
                    json_body={"user_id": user_id, "draft_id": draft_id,
                               "title": title, "project_type": project_type},
                    timeout=30)

def api_run_template(template_id: str):
    return _request("POST", f"/templates/{template_id}/run", json_body={}, timeout=600)

def api_delete_template(template_id: str):
    return _request("DELETE", f"/templates/{template_id}")


# ── Highcharts rendering (load JS once) ─────────────────────────────────────

_HIGHCHARTS_JS_PATH = Path(__file__).resolve().parent / "static" / "highcharts.js"
_HIGHCHARTS_JS = _HIGHCHARTS_JS_PATH.read_text(encoding="utf-8") if _HIGHCHARTS_JS_PATH.exists() else ""


def _sanitize(obj):
    """Replace NaN / Inf with None so the chart payload is valid JSON."""
    if obj is None: return None
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj): return None
        return obj
    if isinstance(obj, dict):  return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [_sanitize(v) for v in obj]
    return obj


def render_chart(chart: dict, container_key: str, height: int = 380):
    """Drop one Highcharts chart into the page."""
    config = json.dumps(_sanitize(chart), default=str)
    cid = f"chart-{container_key}"
    html = f"""
    <!DOCTYPE html><html><head>
      <style>body{{margin:0;padding:0;background:transparent;font-family:-apple-system,BlinkMacSystemFont,sans-serif;}}#{cid}{{width:100%;height:{height-20}px;}}</style>
      <script>{_HIGHCHARTS_JS}</script>
    </head><body>
      <div id="{cid}"></div>
      <script>
        var cfg={config};
        if(!cfg.chart) cfg.chart={{}}; cfg.chart.renderTo='{cid}';
        if(!cfg.credits) cfg.credits={{enabled:false}};
        Highcharts.chart(cfg);
      </script>
    </body></html>"""
    components.html(html, height=height, scrolling=False)


def render_freeform_canvas(slots: list[dict], draft_id: str, height: int = 720):
    """Render a free-form canvas with drag-to-move and resize-edge support.

    Each tile is absolutely-positioned via CSS using its (x, y, w, h)
    fractions of the canvas. A small homemade drag handler (no GridStack
    dependency — that combo of column=100 + cellHeight=8 didn't render
    reliably) PATCHes the server on drop with the new fractional coords.
    """
    canvas_h = height - 60   # leaves room for the status bar above

    items: list[str] = []
    for s in slots:
        x = float(s.get("x", 0) or 0)
        y = float(s.get("y", 0) or 0)
        w = float(s.get("w", 0.5) or 0.5)
        h = float(s.get("h", 0.4) or 0.4)
        ch = s.get("chart") or {}
        cid = ch.get("chart_id") or s.get("chart_id") or ""
        items.append(f"""
          <div class="tile" data-cid="{cid}"
               style="left:{x*100:.4f}%;top:{y*100:.4f}%;width:{w*100:.4f}%;height:{h*100:.4f}%;">
            <button class="rm" data-cid="{cid}" title="Remove">✕</button>
            <div class="re" title="Drag to resize"></div>
            <div class="host" id="host-{cid}"></div>
            <script type="application/json" class="cfg">{json.dumps(_sanitize(ch), default=str)}</script>
          </div>
        """)

    slots_payload = json.dumps(_sanitize(slots), default=str)
    html = f"""
    <!DOCTYPE html><html><head>
      <script>{_HIGHCHARTS_JS}</script>
      <style>
        body{{margin:0;padding:0;background:transparent;font-family:-apple-system,BlinkMacSystemFont,sans-serif;}}
        .status{{font-size:12px;color:#6b7280;padding:4px 2px;}}
        .status.saving{{color:#2563eb;}}.status.saved{{color:#16a34a;}}.status.error{{color:#dc2626;}}
        .canvas{{position:relative;width:100%;height:{canvas_h}px;background:rgba(0,0,0,.015);border-radius:6px;overflow:hidden;}}
        .tile{{position:absolute;background:#fff;border:1px solid #e5e7eb;border-radius:8px;
               box-shadow:0 1px 3px rgba(0,0,0,.04);overflow:hidden;box-sizing:border-box;cursor:move;}}
        .tile.dragging{{z-index:1000;opacity:.92;box-shadow:0 6px 18px rgba(0,0,0,.18);}}
        .host{{position:absolute;inset:4px;}}
        .rm{{position:absolute;top:4px;right:6px;border:0;background:rgba(255,255,255,.85);cursor:pointer;
             color:#991b1b;font-size:13px;padding:2px 6px;border-radius:4px;
             opacity:0;transition:opacity .15s;z-index:5;line-height:1;}}
        .tile:hover .rm{{opacity:1;}}
        .rm:hover{{color:#dc2626;background:#fff;}}
        .re{{position:absolute;right:0;bottom:0;width:14px;height:14px;cursor:nwse-resize;z-index:6;
             background:linear-gradient(135deg,transparent 50%,#94a3b8 50%);border-bottom-right-radius:8px;opacity:.6;}}
        .re:hover{{opacity:1;}}
      </style>
    </head><body>
      <div id="status" class="status">Ready · drag tiles to move, drag the corner to resize · auto-saves</div>
      <div class="canvas" id="canvas">{''.join(items)}</div>

      <script>
        const DRAFT_ID = {json.dumps(draft_id)};
        const API_BASE = {json.dumps(API_BASE)};
        const STATUS   = document.getElementById('status');
        const CANVAS   = document.getElementById('canvas');

        // Keep slot data + chart instance keyed by chart_id so we can
        // PATCH the latest layout and call chart.reflow() after resize.
        const SLOTS = {{}};
        const CHARTS = {{}};
        ({slots_payload}).forEach(s => {{
          const cid = (s.chart && s.chart.chart_id) || s.chart_id || '';
          if (cid) SLOTS[cid] = s;
        }});

        // Render charts AFTER the DOM is laid out (their hosts already
        // have real pixel sizes from the absolute-positioned .tile).
        function renderCharts() {{
          document.querySelectorAll('.tile').forEach(tile => {{
            const cid = tile.dataset.cid;
            const cfg_el = tile.querySelector('.cfg');
            const host = tile.querySelector('.host');
            if (!cfg_el || !host || CHARTS[cid]) return;
            try {{
              const cfg = JSON.parse(cfg_el.textContent);
              if (!cfg.chart) cfg.chart = {{}};
              cfg.chart.renderTo = host.id;
              if (!cfg.credits) cfg.credits = {{ enabled: false }};
              cfg.title = cfg.title || {{}};
              if (typeof cfg.title === 'string') cfg.title = {{ text: cfg.title }};
              cfg.title.style = Object.assign({{fontSize:'13px'}}, cfg.title.style || {{}});
              CHARTS[cid] = Highcharts.chart(cfg);
            }} catch (e) {{
              host.innerText = '⚠ render failed: ' + (e && e.message || e);
              console.error('chart render failed for', cid, e);
            }}
          }});
        }}
        // Wait for two frames to be sure layout is final, then render.
        requestAnimationFrame(() => requestAnimationFrame(renderCharts));

        // ─── drag-to-move + corner-to-resize ─────────────────────────────
        let saveTimer = null;
        function scheduleSave() {{
          clearTimeout(saveTimer);
          saveTimer = setTimeout(persistLayout, 250);
        }}

        function tileFractions(tile) {{
          const cw = CANVAS.getBoundingClientRect().width;
          const ch = CANVAS.getBoundingClientRect().height;
          const r  = tile.getBoundingClientRect();
          const cr = CANVAS.getBoundingClientRect();
          return {{
            x: (r.left - cr.left) / cw,
            y: (r.top  - cr.top)  / ch,
            w: r.width  / cw,
            h: r.height / ch,
          }};
        }}

        function attachDrag(tile) {{
          tile.addEventListener('mousedown', (e) => {{
            // Skip if the click hit the resize handle or the remove button
            if (e.target.classList.contains('re') || e.target.classList.contains('rm')) return;
            e.preventDefault();
            tile.classList.add('dragging');
            const startX = e.clientX, startY = e.clientY;
            const startLeft = tile.offsetLeft, startTop = tile.offsetTop;
            function move(ev) {{
              const dx = ev.clientX - startX, dy = ev.clientY - startY;
              const cw = CANVAS.clientWidth, chx = CANVAS.clientHeight;
              const nx = Math.max(0, Math.min(cw - tile.offsetWidth,  startLeft + dx));
              const ny = Math.max(0, Math.min(chx - tile.offsetHeight, startTop  + dy));
              tile.style.left = (nx / cw  * 100) + '%';
              tile.style.top  = (ny / chx * 100) + '%';
            }}
            function up() {{
              tile.classList.remove('dragging');
              document.removeEventListener('mousemove', move);
              document.removeEventListener('mouseup', up);
              const f = tileFractions(tile);
              const cid = tile.dataset.cid;
              if (SLOTS[cid]) {{
                Object.assign(SLOTS[cid], f);
                scheduleSave();
              }}
            }}
            document.addEventListener('mousemove', move);
            document.addEventListener('mouseup', up);
          }});
        }}

        function attachResize(tile) {{
          const handle = tile.querySelector('.re');
          if (!handle) return;
          handle.addEventListener('mousedown', (e) => {{
            e.preventDefault(); e.stopPropagation();
            tile.classList.add('dragging');
            const startX = e.clientX, startY = e.clientY;
            const startW = tile.offsetWidth, startH = tile.offsetHeight;
            function move(ev) {{
              const dw = ev.clientX - startX, dh = ev.clientY - startY;
              const cw = CANVAS.clientWidth, chx = CANVAS.clientHeight;
              const nw = Math.max(80, Math.min(cw - tile.offsetLeft, startW + dw));
              const nh = Math.max(80, Math.min(chx - tile.offsetTop, startH + dh));
              tile.style.width  = (nw / cw  * 100) + '%';
              tile.style.height = (nh / chx * 100) + '%';
              const c = CHARTS[tile.dataset.cid];
              if (c && c.reflow) c.reflow();
            }}
            function up() {{
              tile.classList.remove('dragging');
              document.removeEventListener('mousemove', move);
              document.removeEventListener('mouseup', up);
              const f = tileFractions(tile);
              const cid = tile.dataset.cid;
              if (SLOTS[cid]) {{
                Object.assign(SLOTS[cid], f);
                scheduleSave();
              }}
              const c = CHARTS[cid];
              if (c && c.reflow) c.reflow();
            }}
            document.addEventListener('mousemove', move);
            document.addEventListener('mouseup', up);
          }});
        }}

        document.querySelectorAll('.tile').forEach(tile => {{
          attachDrag(tile);
          attachResize(tile);
        }});

        // Remove button
        document.querySelectorAll('.rm').forEach(btn => {{
          btn.addEventListener('click', (e) => {{
            e.stopPropagation();
            const cid = btn.dataset.cid;
            delete SLOTS[cid];
            const c = CHARTS[cid];
            if (c) {{ try {{ c.destroy(); }} catch (e) {{}}; delete CHARTS[cid]; }}
            const tile = document.querySelector(`.tile[data-cid="${{cid}}"]`);
            if (tile) tile.remove();
            scheduleSave();
          }});
        }});

        async function persistLayout() {{
          STATUS.textContent='Saving layout…'; STATUS.className='status saving';
          try {{
            const r = await fetch(`${{API_BASE}}/canvas/drafts/${{DRAFT_ID}}`, {{
              method: 'PATCH',
              headers: {{'Content-Type': 'application/json'}},
              body: JSON.stringify({{ slots: Object.values(SLOTS) }}),
            }});
            if (!r.ok) throw new Error('HTTP '+r.status);
            STATUS.textContent='Saved · ' + new Date().toLocaleTimeString();
            STATUS.className='status saved';
          }} catch (e) {{
            STATUS.textContent='Save failed: ' + e.message;
            STATUS.className='status error';
          }}
        }}
      </script>
    </body></html>
    """
    components.html(html, height=height, scrolling=False)


def render_readonly_grid(slots: list[dict], container_key: str, height: int = 720):
    """Render slots at their saved positions, no drag — used in template view.
    Same CSS-absolute-positioning approach as the editable canvas, just
    without drag/resize handlers."""
    canvas_h = height - 20
    items: list[str] = []
    for s in slots:
        x = float(s.get("x", 0) or 0)
        y = float(s.get("y", 0) or 0)
        w = float(s.get("w", 0.5) or 0.5)
        h = float(s.get("h", 0.4) or 0.4)
        ch = s.get("chart") or {}
        cid = ch.get("chart_id") or ""
        host_id = f"ro-host-{container_key}-{cid}"
        items.append(f"""
          <div class="tile" style="left:{x*100:.4f}%;top:{y*100:.4f}%;width:{w*100:.4f}%;height:{h*100:.4f}%;">
            <div class="host" id="{host_id}"></div>
            <script type="application/json" class="cfg" data-host="{host_id}">{json.dumps(_sanitize(ch), default=str)}</script>
          </div>
        """)
    html = f"""
    <!DOCTYPE html><html><head>
      <script>{_HIGHCHARTS_JS}</script>
      <style>
        body{{margin:0;padding:0;background:transparent;font-family:-apple-system,BlinkMacSystemFont,sans-serif;}}
        .canvas{{position:relative;width:100%;height:{canvas_h}px;background:rgba(0,0,0,.015);border-radius:6px;overflow:hidden;}}
        .tile{{position:absolute;background:#fff;border:1px solid #e5e7eb;border-radius:8px;
               box-shadow:0 1px 3px rgba(0,0,0,.04);overflow:hidden;box-sizing:border-box;}}
        .host{{position:absolute;inset:4px;}}
      </style>
    </head><body>
      <div class="canvas">{''.join(items)}</div>
      <script>
        const RO_CHARTS = [];
        function renderRO() {{
          document.querySelectorAll('.cfg').forEach(s => {{
            try {{
              const cfg = JSON.parse(s.textContent);
              if (!cfg.chart) cfg.chart = {{}};
              cfg.chart.renderTo = s.dataset.host;
              if (!cfg.credits) cfg.credits = {{enabled:false}};
              RO_CHARTS.push(Highcharts.chart(cfg));
            }} catch (e) {{
              const h = document.getElementById(s.dataset.host);
              if (h) h.innerText = '⚠ render failed: ' + (e && e.message || e);
              console.error('RO chart render failed', e);
            }}
          }});
        }}
        // Two RAF beats so layout has stabilized before Highcharts measures.
        requestAnimationFrame(() => requestAnimationFrame(renderRO));
      </script>
    </body></html>"""
    components.html(html, height=height, scrolling=True)


def show_api_status(label: str, status: int, err: str | None) -> bool:
    """Render success/error banner under an action button. Returns True on OK."""
    if err:
        st.error(f"{label} failed — {err}")
        return False
    if status >= 200 and status < 300:
        return True
    st.error(f"{label} unexpected status {status}")
    return False


# ── Page setup ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Reporting Agent", page_icon="📊", layout="wide")
st.title("📊 Reporting Agent")
st.caption("Chat → canvas → template → re-run/download. Every API endpoint is exercised here.")


# ── Session state ───────────────────────────────────────────────────────────

def _init_state():
    if "thread_id"        not in st.session_state: st.session_state.thread_id = str(uuid.uuid4())
    if "queries"          not in st.session_state: st.session_state.queries = []
    if "active_draft_id"  not in st.session_state: st.session_state.active_draft_id = None
    if "template_view"    not in st.session_state: st.session_state.template_view = None
_init_state()


# ── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")
    user_id      = st.text_input("User ID", value="demo_user")
    project_type = st.selectbox("Project type", PROJECT_TYPES, index=0)
    max_charts   = st.slider("Max charts per query", 1, 5, 3)

    st.divider()
    if st.button("➕ New thread", use_container_width=True, type="primary"):
        st.session_state.thread_id = str(uuid.uuid4())
        st.session_state.queries = []
        st.rerun()

    st.subheader("💬 Past threads")
    if st.button("🩺 Health check", use_container_width=True):
        st_h, body, err = api_health()
        if not err:
            st.success("Server OK")
        else:
            st.error(err)

    s, body, err = api_list_threads(user_id)
    threads = (body or {}).get("threads") or []
    if err:
        st.caption(f"_thread fetch error: {err}_")
    elif not threads:
        st.caption("_No past threads — ask something to start one._")
    else:
        st.caption(f"{len(threads)} thread(s) for `{user_id}`")
        for t in threads[:30]:
            tid    = t.get("thread_id")
            title  = (t.get("title") or "(untitled)").strip()[:60]
            active = (tid == st.session_state.thread_id)
            label  = f"{'🟢' if active else '💬'}  {title}"
            with st.expander(label, expanded=active):
                st.caption(f"`{tid[:8]}…` · updated `{str(t.get('updated_at') or '')[:19]}`")
                if not active:
                    if st.button("Open this thread", key=f"open_{tid}", use_container_width=True):
                        st.session_state.thread_id = tid
                        st.session_state.queries = []
                        st.rerun()
                else:
                    st.caption("✅ Currently open")


# ── Hydrate the open chat thread from /messages ─────────────────────────────

if not st.session_state.queries:
    s, body, err = api_thread_messages(st.session_state.thread_id, user_id)
    if not err and isinstance(body, dict):
        # Pair user/ai messages back into per-turn records (chat renderer expects {query, rationale, charts}).
        by_qid: dict[str, dict] = {}
        order: list[str] = []
        for m in body.get("messages") or []:
            qid = m.get("query_id") or ""
            if qid not in by_qid:
                by_qid[qid] = {"query_id": qid, "query": "", "rationale": "", "charts": []}
                order.append(qid)
            if m.get("role") == "user":
                by_qid[qid]["query"] = m.get("content") or ""
            elif m.get("role") == "ai":
                by_qid[qid]["rationale"] = m.get("content") or ""
                by_qid[qid]["charts"]    = m.get("charts") or []
        st.session_state.queries = [by_qid[q] for q in order]


# ── Drafts list (always fresh — slots inlined by the API) ──────────────────

s, body, err = api_list_drafts(user_id)
drafts: list[dict] = (body or {}).get("drafts") or []

valid_did = {d["draft_id"] for d in drafts}
if st.session_state.active_draft_id and st.session_state.active_draft_id not in valid_did:
    st.session_state.active_draft_id = None
if not st.session_state.active_draft_id and drafts:
    st.session_state.active_draft_id = drafts[0]["draft_id"]


# ── Helpers used by the chat / canvas tabs ─────────────────────────────────

def _slot_chart_id(s: dict) -> str:
    ch = s.get("chart") if isinstance(s, dict) else None
    return (ch.get("chart_id") if isinstance(ch, dict) else None) or s.get("chart_id") or ""

def _chart_in_draft(d: dict, cid: str) -> bool:
    return any(_slot_chart_id(s) == cid for s in (d.get("slots") or []))

def _add_chart_to_draft(draft: dict, chart: dict) -> tuple[bool, str]:
    """Append a chart to a draft as a new slot. Server clones chart_id on add."""
    slots = list(draft.get("slots") or [])
    cid = chart.get("chart_id") or ""
    if not cid:
        return False, "chart has no chart_id"
    if any(_slot_chart_id(s) == cid for s in slots):
        return False, "already in this draft"
    if len(slots) >= MAX_REPORT_CHARTS:
        return False, f"draft is full ({MAX_REPORT_CHARTS} max)"
    # Auto-place: stack new tile below everything else, half width / 0.4 height.
    max_y = max((float(s.get("y", 0) or 0) + float(s.get("h", 0.4) or 0.4) for s in slots), default=0.0)
    new_slot = {"chart": chart, "x": 0.0, "y": max_y, "w": 0.5, "h": 0.4}
    slots.append(new_slot)
    s_, body, err = api_patch_draft(draft["draft_id"], slots=slots)
    if err:
        return False, err
    return True, "Added"

@st.dialog("Edit chart (NL)")
def _edit_chart_dialog(chart_id: str, chart_title: str):
    st.caption(f"Editing **{chart_title}**")
    st.write("Examples: _change bars to red_, _hide the legend_, _rename y-axis to Sites/week_")
    instr = st.text_area("Your edit", placeholder="e.g., make the bars red", height=100, key=f"edit_in_{chart_id}")
    c1, c2 = st.columns(2)
    with c1:
        apply = st.button("Apply edit", type="primary", use_container_width=True)
    with c2:
        if st.button("Cancel", use_container_width=True): st.rerun()
    if apply:
        if not instr.strip():
            st.warning("Please describe the change first.")
            return
        with st.spinner("Patching the chart…"):
            s_, body, err = api_chart_edit(chart_id, instr.strip())
        if err:
            st.error(f"Edit failed — {err}")
            return
        st.success("Edit applied + persisted")
        st.rerun()


# ── Page tabs ───────────────────────────────────────────────────────────────

tab_chat, tab_canvas, tab_templates, tab_explore, tab_trace = st.tabs([
    f"💬 Chat ({len(st.session_state.queries)})",
    f"🎨 Canvas ({len(drafts)})",
    "📄 Templates",
    "🔍 Explore (charts / edit history)",
    "🛠 API trace",
])


# ============================================================================
# CHAT TAB — SSE stream + history rendering
# ============================================================================
with tab_chat:
    st.subheader("Conversation")

    if not st.session_state.queries:
        st.info("Ask a question below to start a new thread.")

    for q_rec in st.session_state.queries:
        with st.container(border=True):
            st.markdown(f"**👤 You:** {q_rec.get('query') or ''}")
            if q_rec.get("rationale"):
                st.info(f"**🤖 AI:** {q_rec['rationale']}")
            charts = q_rec.get("charts") or []
            if not charts:
                st.warning("No charts produced for this question.")
            for i, chart in enumerate(charts):
                with st.container(border=True):
                    title = (chart.get("title") or {}).get("text") if isinstance(chart.get("title"), dict) else "Chart"
                    st.markdown(f"**{i+1}. {title}**")
                    if chart.get("description"):
                        st.caption(chart["description"])
                    render_chart(chart, container_key=f"{q_rec['query_id']}-{i}")
                    if chart.get("insight"):
                        st.success(f"💡 **Insight:** {chart['insight']}")
                    with st.expander("🐍 Python + SQL script", expanded=False):
                        st.code(chart.get("script") or "(no script)", language="python")

                    # "Add to canvas" popover
                    cid = chart.get("chart_id") or ""
                    in_drafts = [d for d in drafts if _chart_in_draft(d, cid)]
                    label = f"✓ In {len(in_drafts)} draft(s)" if in_drafts else "➕ Add to canvas"
                    with st.popover(label, use_container_width=True):
                        if drafts:
                            for d in drafts:
                                already = _chart_in_draft(d, cid)
                                full = (len(d.get("slots") or []) >= MAX_REPORT_CHARTS) and not already
                                btn = ("✓ " if already else ("⛔ " if full else "📄 ")) + (d.get("name") or "Untitled") + f" ({len(d.get('slots') or [])}/{MAX_REPORT_CHARTS})"
                                if st.button(btn, key=f"addto_{q_rec['query_id']}_{i}_{d['draft_id']}",
                                             disabled=already or full, use_container_width=True):
                                    ok, msg = _add_chart_to_draft(d, chart)
                                    st.toast(msg, icon="✅" if ok else "⚠️")
                                    if ok:
                                        st.session_state.active_draft_id = d["draft_id"]
                                        st.rerun()
                            st.divider()
                        new_name = st.text_input("New draft name", key=f"newdraft_{q_rec['query_id']}_{i}",
                                                  placeholder="e.g. Q2 Run-Rate Report",
                                                  label_visibility="collapsed")
                        if st.button("➕ Create draft & add", key=f"newbtn_{q_rec['query_id']}_{i}",
                                     type="primary", use_container_width=True):
                            if not new_name.strip():
                                st.warning("Give the draft a name.")
                            else:
                                s_, body, err = api_create_draft(user_id, new_name.strip(), project_type)
                                if err:
                                    st.error(err)
                                else:
                                    ok, msg = _add_chart_to_draft(body, chart)
                                    st.session_state.active_draft_id = body["draft_id"]
                                    st.toast(f"Created “{new_name.strip()}” + added" if ok else msg,
                                             icon="✅" if ok else "⚠️")
                                    st.rerun()

    st.divider()
    with st.form("chat_form", clear_on_submit=True):
        new_q = st.text_area("Ask a question",
                              placeholder="e.g. Give the GC run rate region wise",
                              height=80)
        submitted = st.form_submit_button("Generate", type="primary")

    if submitted:
        if not new_q.strip():
            st.warning("Please enter a question.")
        else:
            params = urllib.parse.urlencode({
                "query": new_q.strip(), "project_type": project_type,
                "user_id": user_id, "max_charts": max_charts,
                "thread_id": st.session_state.thread_id,
            })
            sse_url = f"{API_BASE}/report/stream?{params}"
            progress = st.progress(0.0, text="Starting…")
            status   = st.empty()
            result   = None
            qid      = None
            try:
                resp = requests.get(sse_url, stream=True, timeout=600)
                if resp.status_code != 200:
                    st.error(f"SSE error {resp.status_code}: {resp.text[:200]}")
                else:
                    cur = None
                    for line in resp.iter_lines(decode_unicode=True):
                        if not line or line.startswith(":"): continue
                        if line.startswith("event: "):
                            cur = line[7:]
                        elif line.startswith("data: "):
                            data = json.loads(line[6:])
                            if cur == "stream_started":
                                qid = data.get("query_id")
                                status.info(f"query_id = {qid[:8]}…")
                            elif cur == "step":
                                progress.progress(min(data.get("step",0)/(data.get("total",3)+1), 0.99),
                                                  text=data.get("label","…"))
                            elif cur == "retrieval_done":
                                status.info(f"Retrieval: {data.get('nodes',0)} nodes / {data.get('paths',0)} paths in {data.get('elapsed_ms',0):.0f}ms")
                            elif cur == "traversal_done":
                                status.info(f"Traversal: {data.get('steps',0)} step(s) in {data.get('elapsed_ms',0)/1000:.1f}s")
                            elif cur == "complete":
                                result = data
                            elif cur == "error":
                                progress.empty()
                                st.error(f"Error: {data.get('message')}")
                                break
                    if result:
                        progress.progress(1.0, text="Complete")
                        status.empty()
                        st.session_state.queries.append({
                            "query_id":  qid or str(uuid.uuid4()),
                            "query":     new_q.strip(),
                            "rationale": result.get("rationale",""),
                            "charts":    result.get("charts", []),
                        })
                        st.rerun()
            except requests.RequestException as e:
                st.error(f"Network error: {e}")


# ============================================================================
# CANVAS TAB — drafts list, create, free-form drag/drop, edit, save, download
# ============================================================================
with tab_canvas:
    top, top_btn = st.columns([4, 1])
    with top_btn:
        with st.popover("➕ New draft", use_container_width=True):
            nd_name = st.text_input("Draft name", key="_new_draft_name",
                                     placeholder="e.g. Q2 NTM Run-Rate Report")
            if st.button("Create", type="primary", use_container_width=True):
                if not nd_name.strip():
                    st.warning("Give the draft a name.")
                else:
                    s_, body, err = api_create_draft(user_id, nd_name.strip(), project_type)
                    if err: st.error(err)
                    else:
                        st.session_state.active_draft_id = body["draft_id"]
                        st.rerun()
    with top:
        st.subheader(f"📚 My canvases · {len(drafts)}")

    if drafts:
        for d in drafts:
            did = d["draft_id"]
            is_active = (did == st.session_state.active_draft_id)
            n_slots = len(d.get("slots") or [])
            updated = str(d.get("updated_at") or "")[:19]
            color = "#2563eb" if is_active else "#e5e7eb"
            bg = "rgba(37,99,235,.06)" if is_active else "transparent"
            st.markdown(
                f"<div style='padding:10px 12px;border:1px solid {color};border-radius:8px;background:{bg};margin-bottom:6px;'>"
                f"<b>📄 {(d.get('name') or 'Untitled')}</b>"
                f"&nbsp;&nbsp;<span style='color:#6b7280;font-size:.85em;'>· {n_slots}/{MAX_REPORT_CHARTS} charts · updated {updated}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
            ca, cb = st.columns([5, 1])
            with ca:
                if not is_active:
                    if st.button(f"Open “{(d.get('name') or 'Untitled')[:30]}”",
                                 key=f"open_d_{did}", use_container_width=True):
                        st.session_state.active_draft_id = did
                        st.rerun()
                else:
                    st.caption("✅ Currently editing this canvas below")
            with cb:
                pk = f"_del_pending_{did}"
                if st.session_state.get(pk):
                    if st.button("✓ confirm", key=f"del_ok_{did}", use_container_width=True):
                        api_delete_draft(did)
                        st.session_state.pop(pk, None)
                        if st.session_state.active_draft_id == did:
                            st.session_state.active_draft_id = None
                        st.rerun()
                else:
                    if st.button("🗑", key=f"del_{did}", use_container_width=True, help="Delete (click again to confirm)"):
                        st.session_state[pk] = True
                        st.rerun()
    else:
        st.info("No canvas drafts yet. Use **➕ New draft** above, or **Add to canvas** under any chart in the chat.")

    st.divider()

    # Active draft editor
    active = next((d for d in drafts if d["draft_id"] == st.session_state.active_draft_id), None)
    if active:
        c1, c2 = st.columns([4, 1])
        with c1:
            renamed = st.text_input("Draft name", value=active.get("name") or "",
                                     key=f"rename_{active['draft_id']}", label_visibility="collapsed")
            if renamed.strip() and renamed.strip() != active.get("name"):
                api_patch_draft(active["draft_id"], name=renamed.strip())
                st.rerun()
        with c2:
            if st.button("🗑 Delete this draft", key=f"del_act_{active['draft_id']}", use_container_width=True):
                api_delete_draft(active["draft_id"])
                st.session_state.active_draft_id = None
                st.rerun()

        slots = sorted(
            active.get("slots") or [],
            key=lambda s: (float(s.get("y", 0) or 0), float(s.get("x", 0) or 0)),
        )
        st.caption(f"**{len(slots)}/{MAX_REPORT_CHARTS}** chart(s) in **“{active.get('name')}”** · "
                   "drag tiles to move, drag corners to resize · positions auto-save")

        if slots:
            render_freeform_canvas(slots, active["draft_id"])
            if st.button("🔄 Refresh layout from server",
                         key=f"sync_{active['draft_id']}", use_container_width=True):
                st.rerun()
        else:
            st.info("Canvas is empty. Use **➕ Add to canvas** under any chart in the chat.")

        # Edit a slot's chart via NL
        if slots:
            with st.container(border=True):
                st.markdown("##### ✏️ Edit a slot's chart (NL)")
                opts = [(_slot_chart_id(s),
                         (s.get("chart") or {}).get("title", {}).get("text", "(untitled)")) for s in slots]
                e1, e2 = st.columns([4, 1])
                with e1:
                    pick = st.selectbox("Which chart?", options=[o[0] for o in opts],
                                        format_func=lambda c: dict(opts).get(c, c),
                                        key=f"edit_pick_{active['draft_id']}",
                                        label_visibility="collapsed")
                with e2:
                    if st.button("✏️ Edit", key=f"edit_btn_{active['draft_id']}", use_container_width=True):
                        if pick: _edit_chart_dialog(pick, dict(opts).get(pick, "Chart"))

        # Downloads + finalize
        st.divider()
        cols = st.columns([2, 2, 2, 3])
        with cols[0]:
            st.markdown("**Export**")
        with cols[1]:
            url_html = f"{API_BASE}/canvas/drafts/{active['draft_id']}/download?user_id={urllib.parse.quote(user_id)}"
            st.markdown(f"[⬇ HTML]({url_html})", help="Self-contained Highcharts page")
        with cols[2]:
            url_pdf = f"{API_BASE}/canvas/drafts/{active['draft_id']}/download.pdf?user_id={urllib.parse.quote(user_id)}"
            st.markdown(f"[⬇ PDF]({url_pdf})", help="Server-rendered PDF (Markdown intermediate)")
        with cols[3]:
            existing_tpl = next(
                (None for _ in [None]),  # placeholder; we look it up via templates list below
                None,
            )

        # Finalize as template (auto-upserts by source_draft_id server-side)
        with st.form(f"finalize_{active['draft_id']}"):
            ftitle = st.text_input("Template title", value=active.get("name") or "")
            do_finalize = st.form_submit_button("💾 Save as template (upsert)",
                                                 type="primary", use_container_width=True,
                                                 disabled=not slots)
        if do_finalize:
            s_, body, err = api_create_template(
                user_id=user_id, draft_id=active["draft_id"],
                title=ftitle.strip() or None, project_type=project_type,
            )
            if err:
                st.error(err)
            else:
                st.success(f"Template **{body.get('action')}** · `{body.get('template_id')[:8]}…`")
                st.session_state.template_view = body.get("template_id")
                st.rerun()
    else:
        st.caption("Pick a draft above, or click **➕ New draft** to start one.")


# ============================================================================
# TEMPLATES TAB — list, open, re-run, download, delete
# ============================================================================
with tab_templates:
    s_, body, err = api_list_templates(user_id)
    tpls = (body or {}).get("templates") or []

    st.subheader(f"📄 Saved templates · {len(tpls)}")

    if not tpls:
        st.info("No saved templates yet. Build a canvas → click **Save as template** to get one here.")
    else:
        # List of slim rows
        for t in tpls:
            tid    = t["template_id"]
            ttitle = t.get("title") or "Untitled"
            is_open = (tid == st.session_state.template_view)
            border = "#16a34a" if is_open else "#e5e7eb"
            bg     = "rgba(22,163,74,.06)" if is_open else "transparent"
            st.markdown(
                f"<div style='padding:10px 12px;border:1px solid {border};border-radius:8px;background:{bg};margin-bottom:6px;'>"
                f"<b>📄 {ttitle}</b>&nbsp;&nbsp;<span style='color:#6b7280;font-size:.85em;'>· `{tid[:8]}…`</span></div>",
                unsafe_allow_html=True,
            )
            ca, cb, cc, cd = st.columns([3, 2, 2, 1])
            with ca:
                if not is_open:
                    if st.button(f"Open “{ttitle[:28]}”", key=f"tpl_open_{tid}", use_container_width=True):
                        st.session_state.template_view = tid
                        st.rerun()
                else:
                    st.caption("✅ Open below")
            with cb:
                if st.button("▶ Re-run", key=f"tpl_run_{tid}", type="primary", use_container_width=True):
                    with st.spinner("Re-running scripts…"):
                        s_, body, err = api_run_template(tid)
                    if err: st.error(err)
                    else:
                        st.session_state[f"_rerun_{tid}"] = body
                        st.session_state.template_view = tid
                        st.toast("Refreshed", icon="🔄")
                        st.rerun()
            with cc:
                url_pdf = f"{API_BASE}/templates/{tid}/download.pdf?user_id={urllib.parse.quote(user_id)}"
                st.markdown(f"[⬇ PDF]({url_pdf})", help="Re-runs server-side, returns fresh PDF")
            with cd:
                pk = f"_tpl_del_p_{tid}"
                if st.session_state.get(pk):
                    if st.button("✓", key=f"tpl_del_ok_{tid}", use_container_width=True):
                        api_delete_template(tid)
                        st.session_state.pop(pk, None)
                        if st.session_state.template_view == tid:
                            st.session_state.template_view = None
                        st.rerun()
                else:
                    if st.button("🗑", key=f"tpl_del_{tid}", use_container_width=True, help="Delete"):
                        st.session_state[pk] = True
                        st.rerun()

        st.divider()

        # Open one
        view_tid = st.session_state.template_view or tpls[0]["template_id"]
        st.session_state.template_view = view_tid

        s_, body, err = api_get_template(view_tid, user_id)
        if err:
            st.error(err)
        else:
            full = body or {}
            st.markdown(f"### 📄 {full.get('title') or '(untitled)'}")
            st.caption(
                f"created `{str(full.get('created_at') or '')[:19]}` · "
                f"last_run `{str(full.get('last_run_at') or '')[:19]}` · "
                f"source_draft `{(full.get('source_draft_id') or '')[:8] if full.get('source_draft_id') else '(none)'}`"
            )

            # Re-run output (if any in session) overrides the saved snapshot.
            rerun = st.session_state.get(f"_rerun_{view_tid}") or {}
            rendered_sels = rerun.get("selections") or []
            if rendered_sels:
                st.caption("🔄 Showing freshly re-run data")
                slots_to_show = sorted(
                    rendered_sels,
                    key=lambda s: (float(s.get("y", 0) or 0), float(s.get("x", 0) or 0)),
                )
            else:
                slots_to_show = sorted(
                    full.get("selections") or [],
                    key=lambda s: (float(s.get("y", 0) or 0), float(s.get("x", 0) or 0)),
                )

            if rerun.get("script_reports"):
                with st.expander("📋 Script reports"):
                    for r in rerun["script_reports"]:
                        st.write(r)

            if not slots_to_show:
                st.warning("This template has no selections to render.")
            else:
                render_readonly_grid(slots_to_show, container_key=f"tpl-{view_tid}")
                with st.expander("📋 Per-chart details (insight + script)"):
                    for sel in slots_to_show:
                        ch = sel.get("chart") or {}
                        title = (ch.get("title") or {}).get("text") if isinstance(ch.get("title"), dict) else "?"
                        st.markdown(f"**[{sel.get('x'):.2f}, {sel.get('y'):.2f}] {sel.get('w'):.2f}×{sel.get('h'):.2f}** · {title}")
                        if ch.get("insight"): st.success(ch["insight"])
                        st.code(ch.get("script") or "(no script)", language="python")
                        st.divider()


# ============================================================================
# EXPLORE TAB — charts library + edit history
# ============================================================================
with tab_explore:
    st.subheader("All charts for this user")
    s_, body, err = api_charts_by_user(user_id)
    rows = (body or {}).get("charts") or []
    if err:
        st.error(err)
    elif not rows:
        st.info("No charts saved yet for this user.")
    else:
        st.caption(f"{len(rows)} chart(s) — newest first")
        for row in rows[:30]:
            cid = row["chart_id"]
            with st.expander(f"📊  {row.get('title_text') or '(untitled)'}  ·  type={row.get('chart_type')}  ·  `{cid[:8]}…`"):
                meta_cols = st.columns(2)
                with meta_cols[0]:
                    st.caption(f"thread_id: `{(row.get('thread_id') or '')[:8]}…`")
                    st.caption(f"query_id:  `{(row.get('query_id') or '')[:8]}…`")
                with meta_cols[1]:
                    st.caption(f"created: `{str(row.get('created_at') or '')[:19]}`")
                    st.caption(f"updated: `{str(row.get('updated_at') or '')[:19]}`")
                if row.get("chart"):
                    render_chart(row["chart"], container_key=f"explore-{cid}", height=320)
                if row.get("insight"):
                    st.success(row["insight"])
                # Edit history
                with st.expander("📜 Edit history"):
                    s_, hbody, herr = api_chart_edit_history(cid)
                    edits = (hbody or {}).get("edits") or []
                    if not edits:
                        st.caption("_no edits yet_")
                    else:
                        for e in edits:
                            st.write(f"`{str(e.get('created_at'))[:19]}` — _{e.get('instruction')}_")


# ============================================================================
# API TRACE TAB — every call made this session
# ============================================================================
with tab_trace:
    trace = st.session_state.get("_api_trace") or []
    cols = st.columns([4, 1])
    with cols[0]:
        st.caption(f"{len(trace)} call(s) recorded this session (newest first)")
    with cols[1]:
        if st.button("Clear", use_container_width=True):
            st.session_state._api_trace = []
            st.rerun()

    for i, call in enumerate(trace[:80]):
        bg = "#fef2f2" if (isinstance(call["status"], int) and call["status"] >= 400) else \
             "#f0fdf4" if (isinstance(call["status"], int) and call["status"] < 300) else "#fffbeb"
        head = f"{call['ts']}  ·  {call['method']:6}  {call['path']}  →  {call['status']}  ({call['elapsed_ms']}ms)"
        with st.expander(head):
            if call.get("params"):
                st.markdown("**params:**"); st.json(call["params"])
            if call.get("request"):
                st.markdown("**request body:**"); st.json(call["request"])
            st.markdown("**response:**")
            if call.get("error"):
                st.error(call["error"])
            if isinstance(call["response"], (dict, list)):
                st.json(call["response"])
            elif call["response"]:
                st.code(str(call["response"])[:2000])
