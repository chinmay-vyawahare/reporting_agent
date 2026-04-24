"""
Typed HTTP client for the Reporting Agent API.

Owns:
  * URL building, JSON serialisation, timeouts.
  * Response parsing into Pydantic view-models (ui.models).
  * Structured error type (`ApiError`) — caller never has to inspect
    `response.text` or `response.status_code` directly.
  * Per-call trace ring (last N=100 calls) for the debug panel.
  * Schema validation on the way out — the chart payload sent into
    `patch_draft` / `create_template` is validated against the same
    strict per-type Chart union the server uses.

This replaces:
  - `requests.*` calls scattered across `streamlit_app.py`
  - the per-helper `try/except + st.error(...)` boilerplate
  - the monkey-patch on `requests.api.request` for tracing
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

import requests
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

from models.chart_types import parse_chart
from ui.models import (
    CanvasDraft,
    ChartEditResponse,
    ChartRow,
    Query,
    Template,
    TemplateActionResponse,
    TemplateRunResult,
    Thread,
)


# ── Error type ──────────────────────────────────────────────────────────────

class ApiError(Exception):
    """A non-2xx response from the server.

    Attributes:
        status:        HTTP status code (e.g. 422)
        method:        e.g. "PATCH"
        path:          e.g. "/api/v1/canvas/drafts/abc"
        detail:        the parsed `detail` field from FastAPI's error body
        field_errors:  list[(loc, msg)] when `detail` is FastAPI's field-error array
        raw:           the raw response body string (for the debug panel)
    """
    def __init__(self, *, status: int, method: str, path: str, detail: Any, raw: str):
        self.status = status
        self.method = method
        self.path = path
        self.detail = detail
        self.raw = raw
        self.field_errors: list[tuple[str, str]] = []
        if isinstance(detail, list):
            for d in detail:
                if isinstance(d, dict):
                    loc = ".".join(str(x) for x in (d.get("loc") or []))
                    self.field_errors.append((loc, str(d.get("msg") or "")))
        super().__init__(f"{method} {path} → {status}: {detail}")

    def short_message(self) -> str:
        if self.field_errors:
            tail = "; ".join(f"{loc}: {msg}" for loc, msg in self.field_errors[:3])
            return f"{self.status} validation: {tail}"
        if isinstance(self.detail, str):
            return f"{self.status}: {self.detail}"
        return f"{self.status}: {self.detail}"


# ── Trace ring ──────────────────────────────────────────────────────────────

@dataclass
class ApiCall:
    ts:           str             # "HH:MM:SS"
    method:       str
    path:         str             # path-relative, e.g. "/canvas/drafts"
    status:       int | str       # int on response, "ERR" on network error
    elapsed_ms:   float
    params:       dict[str, Any] | None = None
    request_body: Any | None = None
    response:     Any | None = None
    error:        str | None = None


class _Trace:
    """Thread-safe ring buffer of recent ApiCall entries (newest first)."""
    def __init__(self, cap: int = 100):
        self._cap = cap
        self._items: list[ApiCall] = []
        self._lock = threading.Lock()

    def add(self, item: ApiCall) -> None:
        with self._lock:
            self._items.insert(0, item)
            del self._items[self._cap:]

    def snapshot(self) -> list[ApiCall]:
        with self._lock:
            return list(self._items)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()


# Module-level singletons so the trace + HTTP session survive across
# Streamlit reruns WITHOUT needing @st.cache_resource on the client itself.
# Why: caching the ApiClient instance pinned the methods table from the
# moment the cache was warmed — adding a new client method (e.g.
# `get_template`) needed a process restart to become visible. Holding state
# at module level instead means we can re-instantiate ApiClient on every
# rerun (cheap), pick up new methods immediately, and still benefit from
# connection reuse + a persistent trace.
_TRACE_SINGLETON: _Trace = _Trace()
_SESSION_SINGLETON: requests.Session | None = None


def _shared_session() -> requests.Session:
    global _SESSION_SINGLETON
    if _SESSION_SINGLETON is None:
        _SESSION_SINGLETON = requests.Session()
    return _SESSION_SINGLETON


# ── Client ──────────────────────────────────────────────────────────────────

class ApiClient:
    """Single source of truth for talking to the backend."""

    def __init__(self, base_url: str, default_timeout: float = 15.0):
        self._base = base_url.rstrip("/")
        self._timeout = default_timeout
        # Trace + HTTP session live at module level so they survive across
        # Streamlit reruns even if the client is re-instantiated each time.
        self._trace = _TRACE_SINGLETON
        self._session = _shared_session()

    # ── Trace --------------------------------------------------------------
    @property
    def trace(self) -> list[ApiCall]:
        return self._trace.snapshot()

    def clear_trace(self) -> None:
        self._trace.clear()

    # ── Low-level request ------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Send a request, log it to trace, return parsed JSON.

        Raises `ApiError` on non-2xx. Network failures bubble up as `requests`
        exceptions (rare in practice; the trace still records them).
        """
        url = f"{self._base}{path}"
        t0 = time.perf_counter()
        resp = None
        err: Exception | None = None
        try:
            resp = self._session.request(
                method, url,
                params=params,
                json=json_body,
                timeout=timeout or self._timeout,
            )
            return self._handle(method, path, resp)
        except requests.RequestException as e:
            err = e
            raise
        finally:
            elapsed = (time.perf_counter() - t0) * 1000
            entry = ApiCall(
                ts=time.strftime("%H:%M:%S"),
                method=method.upper(),
                path=path,
                status=(resp.status_code if resp is not None else "ERR"),
                elapsed_ms=round(elapsed, 1),
                params=params,
                request_body=json_body,
                response=self._safe_body(resp),
                error=str(err) if err else None,
            )
            self._trace.add(entry)

    def _handle(self, method: str, path: str, resp: requests.Response) -> Any:
        """Return parsed JSON on 2xx, raise ApiError otherwise."""
        if 200 <= resp.status_code < 300:
            if not resp.content:
                return None
            return resp.json()
        # Error path — try to surface FastAPI's structured detail field.
        detail: Any
        try:
            payload = resp.json()
            detail = payload.get("detail", payload) if isinstance(payload, dict) else payload
        except Exception:
            detail = resp.text[:600]
        raw_text = ""
        try: raw_text = resp.text
        except Exception: pass
        raise ApiError(
            status=resp.status_code, method=method.upper(), path=path,
            detail=detail, raw=raw_text,
        )

    @staticmethod
    def _safe_body(resp: requests.Response | None) -> Any | None:
        if resp is None:
            return None
        try:
            return resp.json()
        except Exception:
            try:
                return resp.text[:1200]
            except Exception:
                return None

    # ── Helpers for parsing into Pydantic models -------------------------
    @staticmethod
    def _parse(model: type[BaseModel], data: Any) -> Any:
        """Validate one item with the given model. Re-raises ApiError-flavored
        errors if the body shape doesn't match, so callers always get a clean
        message."""
        try:
            return model.model_validate(data)
        except ValidationError as e:
            raise ApiError(
                status=200, method="GET", path="<parse>",
                detail=f"Response shape did not match {model.__name__}: {e.errors()[:3]}",
                raw=str(data)[:600],
            )

    @staticmethod
    def _parse_list(model: type[BaseModel], items: Iterable[Any]) -> list[Any]:
        """Parse a list of items into the given model.

        Per-item tolerant: if one item fails validation, it is logged and
        skipped instead of aborting the whole list. Reason: in the UI, a
        single malformed row (e.g., a chart saved under an older shape)
        should not blank out the user's entire thread / canvas / template
        list. The bad row gets dropped, the rest of the list renders.
        """
        out: list[Any] = []
        for i, item in enumerate(items or []):
            try:
                out.append(model.model_validate(item))
            except ValidationError as e:
                logger.warning(
                    "skipped %s[%d] — did not match schema: %s",
                    model.__name__, i, e.errors()[:2],
                )
        return out

    # ────────────────────────────────────────────────────────────────────
    # Threads
    # ────────────────────────────────────────────────────────────────────
    def list_threads(self, user_id: str, limit: int = 50) -> list[Thread]:
        body = self._request("GET", "/threads", params={"user_id": user_id, "limit": limit})
        return self._parse_list(Thread, body.get("threads", []))

    def list_thread_messages(self, thread_id: str, user_id: str, limit: int = 100) -> list[dict[str, Any]]:
        """Chat-style messages for a thread — alternating user/ai entries.

        Ownership-checked: server returns 403 if the thread doesn't belong
        to user_id. Each message is `{role, query_id, content, ...}` — see
        the threads endpoint docstring for the per-role shape.
        """
        body = self._request(
            "GET", f"/threads/{thread_id}/messages",
            params={"user_id": user_id, "limit": limit},
        )
        return body.get("messages", []) or []

    # ────────────────────────────────────────────────────────────────────
    # Charts
    # ────────────────────────────────────────────────────────────────────
    def get_chart(self, chart_id: str) -> ChartRow:
        body = self._request("GET", f"/charts/{chart_id}")
        return self._parse(ChartRow, body)

    def list_charts_by_query(self, query_id: str) -> list[ChartRow]:
        body = self._request("GET", "/charts", params={"query_id": query_id})
        return self._parse_list(ChartRow, body.get("charts", []))

    def list_charts_by_user(self, user_id: str, limit: int = 100) -> list[ChartRow]:
        body = self._request("GET", "/charts", params={"user_id": user_id, "limit": limit})
        return self._parse_list(ChartRow, body.get("charts", []))

    def list_charts_by_thread(self, thread_id: str, limit: int = 100) -> list[ChartRow]:
        body = self._request("GET", "/charts", params={"thread_id": thread_id, "limit": limit})
        return self._parse_list(ChartRow, body.get("charts", []))

    def edit_chart(self, chart_id: str, instruction: str) -> ChartEditResponse:
        body = self._request(
            "POST", "/charts/edit",
            json_body={"chart_id": chart_id, "instruction": instruction},
            timeout=120,    # NL edit needs an LLM call
        )
        return self._parse(ChartEditResponse, body)

    def chart_edit_history(self, chart_id: str, limit: int = 20) -> list[dict[str, Any]]:
        body = self._request("GET", "/charts/edits", params={"chart_id": chart_id, "limit": limit})
        return body.get("edits", []) or []

    # ────────────────────────────────────────────────────────────────────
    # Canvas
    # ────────────────────────────────────────────────────────────────────
    def list_drafts(self, user_id: str, limit: int = 50) -> list[CanvasDraft]:
        body = self._request("GET", "/canvas/drafts", params={"user_id": user_id, "limit": limit})
        return self._parse_list(CanvasDraft, body.get("drafts", []))

    def get_draft(self, draft_id: str) -> CanvasDraft:
        body = self._request("GET", f"/canvas/drafts/{draft_id}")
        return self._parse(CanvasDraft, body)

    def create_draft(self, *, user_id: str, name: str, project_type: str = "") -> CanvasDraft:
        body = self._request(
            "POST", "/canvas/drafts",
            json_body={"user_id": user_id, "name": name, "project_type": project_type},
        )
        return self._parse(CanvasDraft, body)

    def patch_draft(
        self,
        draft_id: str,
        *,
        name: str | None = None,
        slots: list[dict[str, Any]] | None = None,
    ) -> CanvasDraft:
        """Send a draft patch. If `slots` is provided, each slot's `chart` is
        validated against the strict per-type Chart union BEFORE the request
        leaves the UI — so a malformed payload fails locally with a clear
        message instead of round-tripping through the server."""
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if slots is not None:
            for i, s in enumerate(slots):
                ch = s.get("chart")
                if ch is None:
                    raise ApiError(
                        status=400, method="PATCH", path=f"/canvas/drafts/{draft_id}",
                        detail=f"slots[{i}].chart is required (none given)", raw="",
                    )
                try:
                    parse_chart(ch)
                except (ValueError, ValidationError) as e:
                    raise ApiError(
                        status=422, method="PATCH", path=f"/canvas/drafts/{draft_id}",
                        detail=f"slots[{i}].chart failed strict schema: {e}", raw="",
                    )
            body["slots"] = slots
        resp = self._request("PATCH", f"/canvas/drafts/{draft_id}", json_body=body)
        return self._parse(CanvasDraft, resp)

    def delete_draft(self, draft_id: str) -> bool:
        body = self._request("DELETE", f"/canvas/drafts/{draft_id}")
        return bool((body or {}).get("deleted"))

    # ────────────────────────────────────────────────────────────────────
    # Templates
    # ────────────────────────────────────────────────────────────────────
    def list_templates(self, user_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Lightweight list — each row is just `{template_id, title}`.

        Open a single template via `get_template(template_id, user_id=...)`
        to fetch its full state (selections + reconstructed charts).
        """
        body = self._request("GET", "/templates", params={"user_id": user_id, "limit": limit})
        return body.get("templates", []) or []

    def get_template(self, template_id: str, user_id: str) -> Template:
        """Single template with selections + reconstructed chart per selection.

        `user_id` is required — server enforces ownership and returns 403
        if the template doesn't belong to that user.
        """
        body = self._request(
            "GET", f"/templates/{template_id}",
            params={"user_id": user_id},
        )
        return self._parse(Template, body)

    def upsert_template(
        self,
        *,
        user_id: str,
        draft_id: str,
        title: str | None = None,
        project_type: str | None = None,
    ) -> TemplateActionResponse:
        """Save a canvas as a template — by reference, no chart payload sent.

        Server pulls the slots from the draft, copies them as the template's
        selections (chart_id reference), and upserts by draft_id.
        """
        body = self._request(
            "POST", "/templates",
            json_body={
                "user_id":      user_id,
                "draft_id":     draft_id,
                "title":        title,
                "project_type": project_type,
            },
            timeout=30,
        )
        return self._parse(TemplateActionResponse, body)

    def run_template(self, template_id: str) -> TemplateRunResult:
        body = self._request("POST", f"/templates/{template_id}/run", json_body={}, timeout=600)
        return self._parse(TemplateRunResult, body)

    def delete_template(self, template_id: str) -> bool:
        body = self._request("DELETE", f"/templates/{template_id}")
        return bool((body or {}).get("deleted"))

    # ────────────────────────────────────────────────────────────────────
    # Health (simple status check — not strictly needed by views but
    # nice to have for the trace panel)
    # ────────────────────────────────────────────────────────────────────
    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health/")


__all__ = ["ApiClient", "ApiCall", "ApiError"]
