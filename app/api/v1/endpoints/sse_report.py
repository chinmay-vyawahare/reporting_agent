"""
SSE streaming endpoint for report generation.

GET /api/v1/report/stream?query=...&project_type=...&user_id=...&username=...

Returns a Server-Sent Events stream with real-time progress updates:
  event: step       → {step, total, label}
  event: traversal_done → {steps, elapsed_ms}
  event: complete    → {status, charts, rationale, ...}
  event: error       → {message}
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from services.reporting_service import stream_report
from services.sse_manager import sse_manager
from services import db_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Reporting"])

# ── ANSI colors ──────────────────────────────────────────────────────────────
_CYAN = "\033[96m"
_GREEN = "\033[92m"
_RED = "\033[91m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"

_DONE_SENTINEL = "__done__"


def _run_stream_thread(
    query: str,
    project_type: str,
    query_id: str,
    user_id: str,
    username: str,
    max_charts: int,
    thread_id: str | None = None,
) -> None:
    """Sync worker function — runs the pipeline and pushes SSE events."""
    start_time = time.perf_counter()

    print(f"\n{_BOLD}{'=' * 70}", flush=True)
    print(f"  REPORTING AGENT (SSE) — New Streaming Request", flush=True)
    print(f"{'=' * 70}{_RESET}", flush=True)
    print(f"  {_DIM}Query ID    : {query_id}{_RESET}", flush=True)
    print(f"  {_DIM}Thread ID   : {thread_id or '-'}{_RESET}", flush=True)
    print(f"  {_DIM}User        : {username} ({user_id}){_RESET}", flush=True)
    print(f"  {_DIM}Query       : {query}{_RESET}", flush=True)
    print(f"  {_DIM}Project type: {project_type}{_RESET}", flush=True)

    # Upsert chat thread row if thread_id provided
    if thread_id:
        db_service.ensure_thread(
            thread_id=thread_id,
            user_id=user_id,
            username=username,
            project_type=project_type,
            title=query[:120],
        )

    # Persist the incoming request
    db_service.create_query(
        query_id=query_id,
        user_id=user_id,
        username=username,
        original_query=query,
        project_type=project_type,
        max_charts=max_charts,
        thread_id=thread_id,
    )

    def emit(event: str, data: dict):
        sse_manager.put_sync(query_id, event, data)

    try:
        result = stream_report(
            query=query,
            project_type=project_type,
            query_id=query_id,
            emit=emit,
            max_charts=max_charts,
        )

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        if result["status"] == "success":
            db_service.update_query_complete(
                query_id=query_id,
                charts=result.get("charts", []),
                rationale=result.get("rationale", ""),
                traversal_findings=result.get("traversal_findings", ""),
                traversal_steps=result.get("traversal_steps", 0),
                duration_ms=elapsed_ms,
                errors=result.get("errors"),
                evidence=result.get("evidence", []),
            )
            print(f"\n{_BOLD}{'=' * 70}", flush=True)
            print(f"  {_GREEN}SUCCESS — {len(result.get('charts', []))} chart(s) in {elapsed_ms:.0f}ms{_RESET}", flush=True)
            print(f"{_BOLD}{'=' * 70}{_RESET}\n", flush=True)
        else:
            db_service.update_query_error(
                query_id=query_id,
                duration_ms=elapsed_ms,
                errors=result.get("errors"),
                traversal_findings=result.get("traversal_findings", ""),
                traversal_steps=result.get("traversal_steps", 0),
            )
            print(f"\n{_BOLD}{'=' * 70}", flush=True)
            print(f"  {_RED}FAILED after {elapsed_ms:.0f}ms{_RESET}", flush=True)
            print(f"{_BOLD}{'=' * 70}{_RESET}\n", flush=True)

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.error("SSE stream failed: %s", e)
        emit("error", {"message": str(e)})
        db_service.update_query_error(query_id=query_id, duration_ms=elapsed_ms, errors=[str(e)])
    finally:
        sse_manager.put_sync(query_id, _DONE_SENTINEL, {})


async def _event_generator(query_id: str, queue: asyncio.Queue):
    """Async generator that yields SSE-formatted events from the queue."""
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send heartbeat to keep connection alive
                yield ": heartbeat\n\n"
                continue

            event_name = item.get("event", "message")
            data = item.get("data", {})

            if event_name == _DONE_SENTINEL:
                break

            yield f"event: {event_name}\ndata: {json.dumps(data, default=str)}\n\n"

            if event_name in ("complete", "error"):
                break
    finally:
        sse_manager.cleanup(query_id)


@router.get("/report/stream", summary="Stream report generation via SSE")
async def stream_report_endpoint(
    query: str = Query(..., description="Natural language query"),
    project_type: str = Query(..., description="NTM, AHLOB Modernization, or Both"),
    user_id: str = Query(..., description="User identifier"),
    username: str = Query(..., description="Display name"),
    max_charts: int = Query(default=3, ge=1, le=5, description="Maximum charts"),
    thread_id: str | None = Query(default=None, description="Chat thread ID"),
):
    """
    Stream report generation with real-time progress events.

    Events:
    - `step` — pipeline step started (1/3, 2/3, 3/3)
    - `traversal_done` — traversal agent finished with tool call count
    - `complete` — full result with charts, rationale, etc.
    - `error` — something went wrong
    """
    query_id = str(uuid.uuid4())
    loop = asyncio.get_event_loop()
    queue = sse_manager.register(query_id, loop)

    # Spawn the sync worker in a thread
    loop.run_in_executor(
        None,
        _run_stream_thread,
        query, project_type, query_id, user_id, username, max_charts, thread_id,
    )

    # Send preamble + stream events
    async def generate():
        # Initial event with query_id
        yield f"event: stream_started\ndata: {json.dumps({'query_id': query_id, 'thread_id': thread_id})}\n\n"
        async for chunk in _event_generator(query_id, queue):
            yield chunk

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )
