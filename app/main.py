"""
Reporting Agent — FastAPI application entry point.

Generates Highcharts visualizations from natural language queries
by querying a Neo4j Knowledge Graph and PostgreSQL database.
"""
import os
import sys
from pathlib import Path

# Ensure the app/ directory is on sys.path for internal imports
_APP_DIR = str(Path(__file__).resolve().parent)
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

# Load .env before any config module is imported
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.v1.router import router as v1_router
from services.db_service import ensure_tables

# ── ANSI colors ──────────────────────────────────────────────────────────────
_GREEN = "\033[92m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"

app = FastAPI(
    title="Reporting Agent API",
    description="Generates Highcharts visualizations from natural language queries about telecom deployment data.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(v1_router, prefix="/api/v1")


@app.on_event("startup")
def startup():
    print(f"\n{_BOLD}{'=' * 70}", flush=True)
    print(f"  REPORTING AGENT v1.0.0", flush=True)
    print(f"{'=' * 70}{_RESET}\n", flush=True)
    print(f"  {_DIM}Ensuring DB tables...{_RESET}", flush=True)
    ensure_tables()
    print(f"  {_GREEN}DB tables ready{_RESET}", flush=True)
    print(f"  {_GREEN}Server is ready to accept requests{_RESET}", flush=True)
    print(f"\n{_BOLD}{'=' * 70}{_RESET}\n", flush=True)


@app.get("/")
def root():
    return {
        "service": "Reporting Agent",
        "version": "1.0.0",
        "docs": "/docs",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8001, reload=True)
