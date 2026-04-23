"""
Shared PostgreSQL connection pool.

Replaces per-call psycopg2.connect() (~50-200ms each) with a thread-safe
ThreadedConnectionPool sized for production load (min=2, max=30).

Usage:

    from services.db_pool import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            ...
        # commits on success, rolls back on exception, ALWAYS returns
        # the connection to the pool

Call `close_pool()` once at process shutdown (registered in app/main.py).
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

import config

logger = logging.getLogger(__name__)

# Pool sizing — production-friendly defaults.
_POOL_MIN = 2
_POOL_MAX = 30

_pool: ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> ThreadedConnectionPool:
    """Return the shared pool, creating it on first call (double-checked lock)."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(
                    minconn=_POOL_MIN,
                    maxconn=_POOL_MAX,
                    host=config.PG_HOST,
                    port=config.PG_PORT,
                    database=config.PG_DATABASE,
                    user=config.PG_USER,
                    password=config.PG_PASSWORD,
                    connect_timeout=5,
                )
                logger.info(
                    "PostgreSQL connection pool created (min=%d, max=%d, host=%s:%s, db=%s)",
                    _POOL_MIN, _POOL_MAX, config.PG_HOST, config.PG_PORT, config.PG_DATABASE,
                )
    return _pool


@contextmanager
def get_conn():
    """Check out a connection from the pool.

    Commits on clean exit, rolls back on exception, **always** returns the
    connection to the pool — even if the caller forgets to commit.

    Connection health: if the checked-out connection is dead (network blip,
    server restart) we discard it and let the pool make a fresh one.
    """
    pool = _get_pool()
    conn = pool.getconn()
    healthy = True
    try:
        yield conn
        conn.commit()
    except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
        # Dead connection — don't return it to the pool, force a new one next time.
        healthy = False
        logger.warning("Discarding bad connection: %s", e)
        try: conn.rollback()
        except Exception: pass
        raise
    except Exception:
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        # `close=True` removes the connection from the pool (used when it's bad).
        pool.putconn(conn, close=not healthy)


def close_pool() -> None:
    """Close all pooled connections — call once at process shutdown."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            try:
                _pool.closeall()
                logger.info("PostgreSQL connection pool closed.")
            except Exception as e:
                logger.warning("Error closing pool: %s", e)
            _pool = None
