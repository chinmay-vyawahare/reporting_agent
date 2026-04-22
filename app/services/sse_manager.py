"""
SSE Manager — Bridge between sync worker threads and async event streams.

Each report query gets its own asyncio.Queue. The sync worker thread pushes
events via put_sync(), and the async SSE generator awaits them.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from utils.json_safe import sanitize_for_json

logger = logging.getLogger(__name__)


class SSEManager:
    """Manages per-query SSE event queues."""

    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}
        self._loops: dict[str, asyncio.AbstractEventLoop] = {}

    def register(self, query_id: str, loop: asyncio.AbstractEventLoop) -> asyncio.Queue:
        """Create a queue for a new query (called from async context)."""
        q: asyncio.Queue = asyncio.Queue()
        self._queues[query_id] = q
        self._loops[query_id] = loop
        return q

    def put_sync(self, query_id: str, event: str, data: dict[str, Any]) -> None:
        """Push an SSE event from a sync worker thread into the async queue.

        Sanitizes NaN/Inf floats here so they never reach the browser or end
        up in Streamlit session state — strict JSON (FastAPI, Pydantic,
        requests) refuses those values with "Out of range float values are
        not JSON compliant: nan".
        """
        q = self._queues.get(query_id)
        loop = self._loops.get(query_id)
        if q is None or loop is None:
            return
        payload = {"event": event, "data": sanitize_for_json(data)}
        asyncio.run_coroutine_threadsafe(q.put(payload), loop)

    def cleanup(self, query_id: str) -> None:
        """Release resources for a completed query."""
        self._queues.pop(query_id, None)
        self._loops.pop(query_id, None)


# Module-level singleton
sse_manager = SSEManager()
