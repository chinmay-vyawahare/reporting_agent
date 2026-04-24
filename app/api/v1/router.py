"""API v1 router — aggregates all endpoint routers."""
from fastapi import APIRouter

from api.v1.endpoints.health import router as health_router
from api.v1.endpoints.sse_report import router as sse_report_router
from api.v1.endpoints.threads import router as threads_router
from api.v1.endpoints.templates import router as templates_router
from api.v1.endpoints.chart_edits import router as chart_edits_router
from api.v1.endpoints.canvas import router as canvas_router

router = APIRouter()
router.include_router(health_router)
router.include_router(sse_report_router)
router.include_router(threads_router)
router.include_router(templates_router)
router.include_router(chart_edits_router)
router.include_router(canvas_router)
