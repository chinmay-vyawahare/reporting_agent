"""
Chat threads API.

A thread is a single conversation a user has with the agent. Each thread
contains 1..N queries (Q&A turns), and each query produces one or more
charts. The thread row is created on first query (via SSE) — there is no
explicit "create thread" call.

Endpoints:
  GET /api/v1/threads?user_id=                    list a user's threads
  GET /api/v1/threads/{tid}/messages              chat-style message list
                                                  (alternating user / ai),
                                                  ownership-checked via
                                                  the `user_id` query param
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from services import db_service

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Threads"])


def _assert_thread_owned(thread_id: str, user_id: str) -> None:
    """404 if the thread doesn't exist; 403 if it belongs to someone else."""
    row = db_service.get_thread(thread_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    if row.get("user_id") != user_id:
        raise HTTPException(
            status_code=403,
            detail=f"Thread {thread_id} does not belong to user {user_id}",
        )


@router.get("/threads", summary="List chat threads for a user")
def list_threads(user_id: str = Query(..., description="Owner of the threads"),
                 limit: int = Query(default=50, le=200)):
    """Newest first by `updated_at`. Each row carries `thread_id`, `user_id`,
    `title` (auto-set to the first question), `project_type`, timestamps."""
    return {"threads": db_service.get_threads_by_user(user_id, limit=limit)}


@router.get(
    "/threads/{thread_id}/messages",
    summary="Chat-style message list for a thread (ownership-checked, alternating user / ai messages)",
)
def list_thread_messages(
    thread_id: str,
    user_id: str = Query(..., description="Owner — server verifies the thread belongs to this user"),
    limit: int = Query(default=100, le=500),
):
    """Return the thread as a flat chat-style message list.

    Ownership is enforced — `user_id` must match `thread.user_id`,
    otherwise 403. 404 if the thread doesn't exist.

    Each Q&A turn becomes TWO messages — one `user`, one `ai` — sharing
    the same `query_id` so the UI can pair them. Order is chronological.

    Message shape:
      user:
        { role: "user", query_id, content, created_at }
      ai:
        { role: "ai",   query_id, content,        # = the rationale
                        charts: [...],            # full Highcharts configs
                        created_at, duration_ms,
                        status }                  # "complete" | "running" | "error"

    The UI just iterates and renders by `role` — no nesting, no per-query
    sub-fetches.
    """
    _assert_thread_owned(thread_id, user_id)
    queries = db_service.get_queries_for_thread(thread_id, limit=limit)
    messages: list[dict[str, Any]] = []
    for q in queries:
        qid = q["query_id"]
        # User message — what the user asked
        messages.append({
            "role":       "user",
            "query_id":   qid,
            "content":    q.get("original_query") or "",
            "created_at": q.get("started_at"),
        })
        # AI message — rationale + charts (only present once status moves
        # past 'running'; for an in-flight query the AI message is empty).
        rows = db_service.get_charts_for_query(qid)
        messages.append({
            "role":        "ai",
            "query_id":    qid,
            "content":     q.get("rationale") or "",
            "charts":      [r.get("chart") or {} for r in rows],
            "status":      q.get("status") or "complete",
            "created_at":  q.get("completed_at") or q.get("started_at"),
            "duration_ms": q.get("duration_ms"),
        })
    return {"thread_id": thread_id, "messages": messages}


# Legacy /queries endpoint removed — use /messages for the chat-style view.
