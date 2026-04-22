"""Python Sandbox tool for executing computation code safely."""
from __future__ import annotations

import ast
import re
import time
import logging
import traceback
from typing import Any
from io import StringIO
import contextlib
import math
import json
import statistics

import psycopg2
import pandas as pd
import numpy as np
import concurrent.futures
import config

logger = logging.getLogger(__name__)

SAFE_MODULES = {"math": math, "json": json, "statistics": statistics, "numpy": np, "pandas": pd}
BLOCKED_BUILTINS = {"exec", "eval", "compile", "open", "breakpoint", "exit", "quit"}
ALLOWED_IMPORT_MODULES = {*SAFE_MODULES.keys(), "collections", "datetime", "itertools", "functools"}

# ── Schema safety net ───────────────────────────────────────────────────────
# All analytical tables live in `pwc_macro_staging_schema`. A single public
# table is allow-listed. If the LLM writes `public.<anything_else>` we rewrite
# it to `pwc_macro_staging_schema.<table>` before Postgres sees it.
STAGING_SCHEMA = "pwc_macro_staging_schema"
PUBLIC_ALLOWLIST: frozenset[str] = frozenset({"gc_capacity_market_trial"})

# Match `public.<table>` or `"public"."<table>"` (with or without quotes)
_PUBLIC_REF_RE = re.compile(
    r'(?P<schema>"?public"?)\.(?P<table>"?[a-zA-Z_][a-zA-Z0-9_]*"?)',
    re.IGNORECASE,
)


def _rewrite_public_schema(sql: str) -> tuple[str, list[str]]:
    """Rewrite non-allow-listed `public.<table>` references.

    Returns (rewritten_sql, rewrites) where `rewrites` is a list of strings
    like `"public.foo -> pwc_macro_staging_schema.foo"` for logging.
    """
    if not sql or "public" not in sql.lower():
        return sql, []

    rewrites: list[str] = []

    def _sub(m: re.Match) -> str:
        raw_table = m.group("table")
        bare_table = raw_table.strip('"')
        if bare_table in PUBLIC_ALLOWLIST:
            return m.group(0)
        original = m.group(0)
        new = f"{STAGING_SCHEMA}.{bare_table}"
        rewrites.append(f"{original} -> {new}")
        return new

    rewritten = _PUBLIC_REF_RE.sub(_sub, sql)
    return rewritten, rewrites


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    """Restricted __import__ that only allows whitelisted modules."""
    top_level = name.split(".")[0]
    if top_level not in ALLOWED_IMPORT_MODULES:
        raise ImportError(f"Import of '{name}' is not allowed in sandbox.")
    return __builtins__["__import__"](name, globals, locals, fromlist, level) \
        if isinstance(__builtins__, dict) \
        else __builtins__.__dict__["__import__"](name, globals, locals, fromlist, level)


def _validate_code(code: str) -> tuple[bool, str]:
    """Static analysis to reject dangerous code patterns."""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return False, f"Syntax error: {e}"

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = ""
            if isinstance(node, ast.ImportFrom) and node.module:
                module = node.module.split(".")[0]
            elif isinstance(node, ast.Import):
                module = node.names[0].name.split(".")[0]
            if module not in ALLOWED_IMPORT_MODULES:
                return False, f"Import of '{module}' is not allowed in sandbox."

        if isinstance(node, ast.Attribute):
            if node.attr.startswith("__") and node.attr not in ("__init__", "__str__", "__repr__", "__len__"):
                return False, f"Access to '{node.attr}' is not allowed."

    return True, "OK"


def execute_python(code: str, context: dict[str, Any] | None = None) -> dict:
    """Execute Python code in a restricted sandbox (no DB access)."""
    is_safe, reason = _validate_code(code)
    if not is_safe:
        return {"status": "error", "error": f"Code validation failed: {reason}", "output": "", "result": None}

    builtins_dict = __builtins__.__dict__ if hasattr(__builtins__, "__dict__") else __builtins__
    safe_builtins = {k: v for k, v in builtins_dict.items() if k not in BLOCKED_BUILTINS}
    safe_builtins["__import__"] = _safe_import

    namespace = {"__builtins__": safe_builtins, **SAFE_MODULES, "np": np, "pd": pd}
    if context:
        namespace.update(context)

    stdout_capture = StringIO()
    start = time.perf_counter()

    try:
        lines = code.strip().splitlines()
        last_line = lines[-1].strip() if lines else ""
        auto_capture = False
        if last_line and not any(last_line.startswith(k) for k in ("result", "#", "print", "import", "from", "if ", "for ", "while ", "def ", "class ", "return", "try", "except", "with ")):
            try:
                ast.parse(last_line, mode="eval")
                auto_capture = True
            except SyntaxError:
                pass

        with contextlib.redirect_stdout(stdout_capture):
            exec(code, namespace)

        elapsed_ms = (time.perf_counter() - start) * 1000
        result = namespace.get("result", None)

        if result is None and auto_capture:
            try:
                result = eval(last_line, namespace)
            except Exception:
                pass

        if result is None and stdout_capture.getvalue().strip():
            result = stdout_capture.getvalue().strip()

        return {"status": "success", "output": stdout_capture.getvalue(), "result": result, "elapsed_ms": round(elapsed_ms, 2)}
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "traceback": traceback.format_exc(), "output": stdout_capture.getvalue(), "result": None, "elapsed_ms": round(elapsed_ms, 2)}


class PythonSandbox:
    """PostgreSQL-backed execution sandbox."""

    def __init__(self):
        self.conn = None
        self.session_vars = {}
        self._connect()

    def _connect(self):
        if self.conn is not None:
            return
        try:
            self.conn = psycopg2.connect(
                host=config.PG_HOST,
                port=config.PG_PORT,
                database=config.PG_DATABASE,
                user=config.PG_USER,
                password=config.PG_PASSWORD,
                options="-c default_transaction_read_only=on",
            )
            self.conn.autocommit = True
        except Exception as e:
            print(f"Postgres not available: {e}")
            self.conn = None

    def _is_raw_sql(self, code: str) -> bool:
        first_line = code.strip().split("\n")[0].strip().rstrip(";").upper()
        sql_starts = ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "WITH ", "EXPLAIN ")
        return first_line.startswith(sql_starts)

    @staticmethod
    def _fix_sql_quoting(code: str) -> str:
        """Fix common SQL string quoting issues that cause syntax errors.

        The LLM often generates code like:
            base_sql = 'SELECT ... WHERE smp_name = 'NTM''
        which breaks because 'NTM' clashes with the outer single quotes.

        This pre-processor detects such patterns and rewrites them
        to use triple-quoted strings.
        """
        import re as _re
        lines = code.split("\n")
        fixed_lines = []
        for line in lines:
            # Detect: variable = 'SELECT ... (long SQL with inner single quotes)
            # Pattern: assignment of a single-quoted string containing SQL keywords
            stripped = line.lstrip()
            if (_re.match(r"^[\w.]+\s*=\s*'SELECT\s", stripped, _re.IGNORECASE)
                    or _re.match(r"^[\w.]+\s*=\s*'WITH\s", stripped, _re.IGNORECASE)):
                # Extract the variable name and indent
                indent = line[:len(line) - len(stripped)]
                eq_pos = stripped.index("=")
                var_name = stripped[:eq_pos].strip()
                sql_part = stripped[eq_pos + 1:].strip()
                # Remove the outer single quotes
                if sql_part.startswith("'") and sql_part.endswith("'"):
                    sql_part = sql_part[1:-1]
                elif sql_part.startswith("'"):
                    sql_part = sql_part[1:]
                    if sql_part.endswith("'"):
                        sql_part = sql_part[:-1]
                # Rebuild with triple quotes
                fixed_lines.append(f'{indent}{var_name} = """{sql_part}"""')
                continue
            fixed_lines.append(line)
        return "\n".join(fixed_lines)

    def execute(self, code: str, timeout_seconds: int = 30) -> dict:
        if self.conn is None:
            self._connect()

        if self._is_raw_sql(code):
            # Apply the schema-rewrite safety net before wrapping into a
            # pd.read_sql call — otherwise a bare `SELECT ... FROM public.stg_...`
            # goes directly to Postgres and 500s.
            code, rewrites = _rewrite_public_schema(code)
            if rewrites:
                logger.warning("Raw-SQL schema rewrite applied: %s", "; ".join(rewrites))
            escaped = code.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
            code = f'result = pd.read_sql("""{escaped}""", conn).to_dict(orient="records")'

        # Auto-fix SQL string quoting issues before execution
        code = self._fix_sql_quoting(code)

        def _execute_query(sql, params=None, db=None, max_rows=None):
            """Helper: run SQL and return list[dict].

            Before running, silently rewrites `public.<non-allowlisted-table>`
            references to `pwc_macro_staging_schema.<table>` so a sloppy prompt
            doesn't produce a relation-not-found error. The one exception is
            `public.gc_capacity_market_trial`, which stays untouched.
            """
            rewritten, rewrites = _rewrite_public_schema(sql)
            if rewrites:
                logger.warning("SQL schema rewrite applied: %s", "; ".join(rewrites))
                sql = rewritten
            df = pd.read_sql(sql, self.conn, params=params)
            if max_rows is not None:
                df = df.head(max_rows)
            return df.to_dict(orient="records")

        namespace = {
            "conn": self.conn, "pd": pd, "np": np,
            "json": json, "execute_query": _execute_query,
            "session": self.session_vars, "result": None,
        }

        lines = code.strip().splitlines()
        last_line = lines[-1].strip() if lines else ""
        auto_capture = False
        if last_line and not any(last_line.startswith(k) for k in ("result", "#", "print", "import", "from", "if ", "for ", "while ", "def ", "class ", "return", "try", "except", "with ")):
            try:
                ast.parse(last_line, mode="eval")
                auto_capture = True
            except SyntaxError:
                pass

        def _run():
            exec(code, namespace)
            return namespace

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_run)
                try:
                    result_ns = future.result(timeout=timeout_seconds)
                except concurrent.futures.TimeoutError:
                    raise TimeoutError(f"Execution timed out after {timeout_seconds}s")

            if "session" in result_ns:
                self.session_vars = result_ns["session"]

            result = result_ns.get("result", None)
            if result is None and auto_capture:
                try:
                    result = eval(last_line, result_ns)
                except Exception:
                    pass

            if isinstance(result, pd.DataFrame):
                result = result.to_dict(orient="records")
            elif isinstance(result, dict):
                for key, val in list(result.items()):
                    if isinstance(val, pd.DataFrame):
                        result[key] = val.to_dict(orient="records")
            elif result is None:
                result = {}
            return {"status": "success", "result": result}

        except Exception as e:
            return {"status": "error", "error": str(e)}

    def close(self):
        if self.conn:
            self.conn.close()
