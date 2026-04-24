"""
Embedding-based retrieval service.

Given a natural-language query, embed it, look up the most relevant
BKGNodes and paths from Postgres (populated by `app/neo4j_embeddings/`),
and hydrate the full node records live from Neo4j.

The retrieved context is attached to `ReportingState` so the traversal
agent can jump straight to SQL execution without calling
`find_relevant` / `get_node` / `get_kpi` first.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
from openai import OpenAI

import config
from tools.neo4j_tool import neo4j_tool

logger = logging.getLogger(__name__)

EMBED_MODEL = "text-embedding-3-small"
EMBED_SCHEMA = "pwc_agent_utility_schema"

# The only Postgres tables the traversal agent should query live in
# `pwc_macro_staging_schema`, with a single exception below. If Neo4j's
# `map_database_name` says "public" for everything else, we rewrite it here
# so the agent never sees `public.<table>` in the prompt and never copies it.
STAGING_SCHEMA = "pwc_macro_staging_schema"
PUBLIC_ALLOWLIST: frozenset[str] = frozenset({
    "gc_capacity_market_trial",
})


def _canonical_source(db: str | None, table: str) -> str:
    """Return the `<schema>.<table>` an agent should actually use.

    - Tables in PUBLIC_ALLOWLIST keep `public.<table>`.
    - Everything else is routed to `pwc_macro_staging_schema.<table>`,
      regardless of whatever `db` came back from Neo4j.
    """
    if not table:
        return ""
    tbl = table.strip().strip('"')
    if tbl in PUBLIC_ALLOWLIST:
        return f"public.{tbl}"
    return f"{STAGING_SCHEMA}.{tbl}"

_CYAN = "\033[96m"
_GREEN = "\033[92m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


# Pool-backed connection (replaces per-call psycopg2.connect — saves the
# 50-200ms TCP+auth handshake on every retrieval call). Same context manager
# semantics: commit on clean exit, rollback on exception, always returned.
from services.db_pool import get_conn as _pg_conn  # noqa: F401


def _openai() -> OpenAI:
    return OpenAI(api_key=config.OPENAI_API_KEY or os.getenv("OPENAI_API_KEY"))


_NODE_CACHE: dict[str, Any] = {"rows": None, "mat": None, "ts": 0.0}
_PATH_CACHE: dict[str, Any] = {"rows": None, "mat": None, "ts": 0.0}
_CACHE_TTL = 300.0


def _load_nodes(force: bool = False) -> tuple[list[dict[str, Any]], np.ndarray]:
    now = time.time()
    if not force and _NODE_CACHE["rows"] is not None and now - _NODE_CACHE["ts"] < _CACHE_TTL:
        return _NODE_CACHE["rows"], _NODE_CACHE["mat"]

    with _pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT element_id, node_id, label, entity_type, embedding "
                f"FROM {EMBED_SCHEMA}.nodes ORDER BY label"
            )
            rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        _NODE_CACHE.update(rows=[], mat=np.zeros((0, 0), dtype=np.float32), ts=now)
        return [], np.zeros((0, 0), dtype=np.float32)

    mat = np.asarray([r["embedding"] for r in rows], dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat /= norms
    for r in rows:
        del r["embedding"]

    _NODE_CACHE.update(rows=rows, mat=mat, ts=now)
    return rows, mat


def _load_paths(force: bool = False) -> tuple[list[dict[str, Any]], np.ndarray]:
    now = time.time()
    if not force and _PATH_CACHE["rows"] is not None and now - _PATH_CACHE["ts"] < _CACHE_TTL:
        return _PATH_CACHE["rows"], _PATH_CACHE["mat"]

    with _pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT path_id, hops, node_element_ids, node_labels, "
                f"relationship_types, composed_text, embedding FROM {EMBED_SCHEMA}.paths"
            )
            rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        _PATH_CACHE.update(rows=[], mat=np.zeros((0, 0), dtype=np.float32), ts=now)
        return [], np.zeros((0, 0), dtype=np.float32)

    mat = np.asarray([r["embedding"] for r in rows], dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat /= norms
    for r in rows:
        del r["embedding"]

    _PATH_CACHE.update(rows=rows, mat=mat, ts=now)
    return rows, mat


def _embed_question(q: str) -> np.ndarray:
    client = _openai()
    resp = client.embeddings.create(model=EMBED_MODEL, input=[q])
    v = np.asarray(resp.data[0].embedding, dtype=np.float32)
    n = np.linalg.norm(v) or 1.0
    return v / n


def _hydrate_from_neo4j(element_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch full node records live from Neo4j, including incoming/outgoing edges."""
    if not element_ids:
        return {}
    cypher = """
    MATCH (n:BKGNode) WHERE elementId(n) IN $ids
    OPTIONAL MATCH (n)-[r_out:RELATES_TO]->(m_out:BKGNode)
    WITH n,
         collect(DISTINCT CASE WHEN m_out IS NULL THEN NULL ELSE
            {label: m_out.label, node_id: m_out.node_id, rel: r_out.relationship_type}
         END) AS out_edges
    OPTIONAL MATCH (n)<-[r_in:RELATES_TO]-(m_in:BKGNode)
    WITH n, out_edges,
         collect(DISTINCT CASE WHEN m_in IS NULL THEN NULL ELSE
            {label: m_in.label, node_id: m_in.node_id, rel: r_in.relationship_type}
         END) AS in_edges
    RETURN elementId(n) AS element_id,
           n.node_id    AS node_id,
           properties(n) AS props,
           [e IN out_edges WHERE e IS NOT NULL] AS out_edges,
           [e IN in_edges  WHERE e IS NOT NULL] AS in_edges
    """
    with neo4j_tool.driver.session(database=config.NEO4J_DATABASE) as s:
        return {r["element_id"]: dict(r) for r in s.run(cypher, ids=element_ids)}


def _summarize_hydrated(hydrated: dict[str, dict[str, Any]], paths: list[dict[str, Any]]) -> str:
    """Compose a human-readable Markdown context block for the traversal agent prompt."""
    lines: list[str] = []
    lines.append("=== PRE-RETRIEVED KG CONTEXT ===\n")
    lines.append("The following nodes and paths were selected via embedding-based retrieval.")
    lines.append("Use this data directly — you do NOT need to call `get_node`, `get_kpi`, "
                 "`find_relevant`, or `traverse_graph`. Jump straight to `run_sql_python`.\n")

    if hydrated:
        lines.append("── Nodes (hydrated from Neo4j) ──")
        for eid, rec in hydrated.items():
            p = rec.get("props", {}) or {}
            label = p.get("label") or "(unknown)"
            etype = p.get("entity_type") or "-"
            nid = p.get("node_id") or "-"
            lines.append(f"\n• {label}  [entity_type={etype}]  (node_id={nid})")
            defn = (p.get("definition") or p.get("nl_description") or "").strip()
            if defn:
                lines.append(f"  definition: {defn}")
            rule = (p.get("nl_business_rule") or "").strip()
            if rule:
                lines.append(f"  business_rule: {rule}")
            tbl = p.get("map_table_name")
            if tbl:
                src = _canonical_source(p.get("map_database_name"), tbl)
                lines.append(f"  source: {src}")
            if p.get("map_key_column"):
                lines.append(f"  key_column: {p['map_key_column']}")
            if p.get("map_label_column"):
                lines.append(f"  label_column: {p['map_label_column']}")
            if p.get("map_python_function"):
                # Never truncate: a half Python function in the prompt is broken
                # syntax and the traversal agent will copy it and crash.
                fn = str(p["map_python_function"]).strip()
                lines.append(f"  map_python_function:\n    {fn}")
            if p.get("kpi_python_function"):
                fn = str(p["kpi_python_function"]).strip()
                lines.append(f"  kpi_python_function:\n    {fn}")
            if p.get("kpi_dimensions"):
                lines.append(f"  kpi_dimensions: {p['kpi_dimensions']}")
            if p.get("kpi_filters"):
                lines.append(f"  kpi_filters: {p['kpi_filters']}")
            out_edges = rec.get("out_edges") or []
            in_edges = rec.get("in_edges") or []
            if out_edges:
                outs = sorted({f"{e['label']} ({e['rel']})" for e in out_edges if e.get("label")})
                lines.append(f"  connects_to: {', '.join(outs)}")
            if in_edges:
                ins = sorted({f"{e['label']} ({e['rel']})" for e in in_edges if e.get("label")})
                lines.append(f"  referenced_by: {', '.join(ins)}")

    if paths:
        lines.append("\n── Paths (ranked by embedding similarity) ──")
        for p in paths:
            lines.append(f"  ({p['hops']}h) {p['composed_text'].replace('PATH: ', '')}")

    return "\n".join(lines)


def retrieve_context(
    query: str,
    top_k_nodes: int = 4,
    top_k_paths: int = 4,
    min_score: float = 0.40,
) -> dict[str, Any]:
    """
    Run embedding-based retrieval for a query.

    Returns:
        {
          "retrieval_used": True,
          "retrieval_summary": str,                     # Markdown block for the LLM prompt
          "retrieval_nodes": [ { element_id, node_id, label, entity_type, score, props } ],
          "retrieval_paths": [ { hops, node_labels, relationship_types, composed_text, score } ],
          "retrieval_hydrated": { element_id: { props, out_edges, in_edges } },
          "retrieval_elapsed_ms": float,
        }

    On any failure, returns { "retrieval_used": False, "retrieval_error": ... }
    so the pipeline can still proceed via the slow path.
    """
    t0 = time.perf_counter()
    print(f"\n  {_BOLD}{_CYAN}Embedding Retrieval:{_RESET} scoring nodes + paths against query...", flush=True)
    try:
        nodes, nmat = _load_nodes()
        paths, pmat = _load_paths()
        if not nodes or not paths:
            msg = "Embedding indexes empty — run app/neo4j_embeddings/*.py first"
            logger.warning(msg)
            return {"retrieval_used": False, "retrieval_error": msg}

        qv = _embed_question(query)
        n_scores = nmat @ qv
        n_idx = np.argsort(-n_scores)[: top_k_nodes]
        kept_nodes = [
            (int(i), float(n_scores[i])) for i in n_idx if float(n_scores[i]) >= min_score
        ]

        p_scores = pmat @ qv
        p_idx = np.argsort(-p_scores)[: top_k_paths]
        kept_paths = [
            (int(i), float(p_scores[i])) for i in p_idx if float(p_scores[i]) >= min_score
        ]

        needed_eids: set[str] = set()
        for i, _ in kept_nodes:
            needed_eids.add(nodes[i]["element_id"])
        for i, _ in kept_paths:
            needed_eids.update(paths[i]["node_element_ids"])

        hydrated = _hydrate_from_neo4j(list(needed_eids))

        node_out: list[dict[str, Any]] = []
        for i, s in kept_nodes:
            n = nodes[i]
            rec = hydrated.get(n["element_id"]) or {}
            node_out.append({
                "element_id": n["element_id"],
                "node_id": n["node_id"],
                "label": n["label"],
                "entity_type": n.get("entity_type"),
                "score": s,
                "props": rec.get("props", {}),
                "out_edges": rec.get("out_edges", []),
                "in_edges": rec.get("in_edges", []),
            })

        path_out: list[dict[str, Any]] = []
        for i, s in kept_paths:
            pr = paths[i]
            path_out.append({
                "path_id": pr.get("path_id"),
                "hops": pr["hops"],
                "node_labels": pr["node_labels"],
                "relationship_types": pr["relationship_types"],
                "node_element_ids": pr["node_element_ids"],
                "composed_text": pr["composed_text"],
                "score": s,
            })

        summary = _summarize_hydrated(hydrated, path_out)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        print(f"  {_GREEN}OK Retrieval:{_RESET} "
              f"{len(node_out)} node(s), {len(path_out)} path(s), "
              f"{len(hydrated)} hydrated from Neo4j in {elapsed_ms:.0f}ms", flush=True)
        for n in node_out[:5]:
            print(f"     {_DIM}• {n['score']:.3f}  {n['label']}  [{n.get('entity_type') or '-'}]{_RESET}", flush=True)

        return {
            "retrieval_used": True,
            "retrieval_summary": summary,
            "retrieval_nodes": node_out,
            "retrieval_paths": path_out,
            "retrieval_hydrated": hydrated,
            "retrieval_elapsed_ms": round(elapsed_ms, 2),
        }

    except Exception as e:
        logger.exception("Embedding retrieval failed: %s", e)
        return {"retrieval_used": False, "retrieval_error": str(e)}


def build_context_block_for_prompt(retrieval: dict[str, Any]) -> str:
    """Return the Markdown retrieval summary (or empty string if retrieval failed)."""
    if not retrieval or not retrieval.get("retrieval_used"):
        return ""
    return retrieval.get("retrieval_summary", "")
