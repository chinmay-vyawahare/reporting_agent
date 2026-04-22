"""
Fetch BKGNodes + edges from Neo4j, compose a relevance-optimized text per node,
embed with OpenAI, and persist everything to Postgres (nokia_embeddings DB).

Tables created:
    nodes(node_id PK, label, entity_type, node_type, composed_text, embedding float8[], props jsonb)
    edges(edge_id PK, source_id FK, target_id FK, relationship_type)

Run:
    python compose_and_embed.py --dry-run          # print composed text, no writes
    python compose_and_embed.py                    # full load
    python compose_and_embed.py --limit 3          # test with first 3 nodes
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from neo4j import GraphDatabase
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.environ["NEO4J_DATABASE"]

PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]
PG_DATABASE = os.environ["PG_DATABASE"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

EMBED_MODEL = "text-embedding-3-small"
EMBED_DIM = 1536
BATCH_SIZE = 64

DDL = """
CREATE TABLE IF NOT EXISTS nodes (
    element_id    TEXT PRIMARY KEY,
    node_id       TEXT NOT NULL,
    label         TEXT NOT NULL,
    entity_type   TEXT,
    node_type     TEXT,
    composed_text TEXT NOT NULL,
    embedding     FLOAT8[] NOT NULL,
    props         JSONB NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_nodes_node_id     ON nodes(node_id);
CREATE INDEX IF NOT EXISTS idx_nodes_label       ON nodes(label);
CREATE INDEX IF NOT EXISTS idx_nodes_entity_type ON nodes(entity_type);

CREATE TABLE IF NOT EXISTS edges (
    edge_id           TEXT PRIMARY KEY,
    source_element_id TEXT NOT NULL REFERENCES nodes(element_id) ON DELETE CASCADE,
    target_element_id TEXT NOT NULL REFERENCES nodes(element_id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_element_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_element_id);
CREATE INDEX IF NOT EXISTS idx_edges_reltype ON edges(relationship_type);
"""


def fetch_nodes_with_neighbors(session) -> list[dict[str, Any]]:
    cypher = """
    MATCH (n:BKGNode)
    OPTIONAL MATCH (n)-[r_out:RELATES_TO]->(m_out:BKGNode)
    WITH n,
         collect(DISTINCT CASE WHEN m_out IS NULL THEN NULL ELSE
            {label: m_out.label, rel: r_out.relationship_type}
         END) AS out_edges
    OPTIONAL MATCH (n)<-[r_in:RELATES_TO]-(m_in:BKGNode)
    WITH n, out_edges,
         collect(DISTINCT CASE WHEN m_in IS NULL THEN NULL ELSE
            {label: m_in.label, rel: r_in.relationship_type}
         END) AS in_edges
    RETURN elementId(n) AS element_id,
           n.node_id AS node_id,
           properties(n) AS props,
           [e IN out_edges WHERE e IS NOT NULL] AS out_edges,
           [e IN in_edges  WHERE e IS NOT NULL] AS in_edges
    ORDER BY n.label
    """
    return [dict(r) for r in session.run(cypher)]


def fetch_edges(session) -> list[dict[str, Any]]:
    cypher = """
    MATCH (a:BKGNode)-[r:RELATES_TO]->(b:BKGNode)
    RETURN toString(elementId(r)) AS edge_id,
           elementId(a) AS source_element_id,
           elementId(b) AS target_element_id,
           r.relationship_type AS relationship_type
    """
    return [dict(r) for r in session.run(cypher)]


def _contract_params(raw: Any) -> list[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return []
    params = data.get("parameters") or []
    return [p.get("name") for p in params if isinstance(p, dict) and p.get("name")]


def compose_text(row: dict[str, Any]) -> str:
    p = row["props"]
    label = p.get("label") or ""
    entity_type = p.get("entity_type") or ""
    node_type = p.get("node_type") or ""
    definition = (p.get("definition") or p.get("nl_description") or "").strip()
    business_rule = (p.get("nl_business_rule") or "").strip()
    table = p.get("map_table_name") or ""
    key_col = p.get("map_key_column") or ""
    label_col = p.get("map_label_column") or ""
    db = p.get("map_database_name") or ""
    params = _contract_params(p.get("map_contract"))

    out_edges = row.get("out_edges") or []
    in_edges = row.get("in_edges") or []

    lines: list[str] = []
    lines.append(f"ENTITY: {label}")
    if entity_type or node_type:
        lines.append(f"TYPE: {entity_type} / {node_type}".strip(" /"))
    if definition:
        lines.append(f"DEFINITION: {definition}")
    if business_rule:
        lines.append(f"BUSINESS_RULE: {business_rule}")
    if table:
        src = f"{db}.{table}" if db else table
        key_suffix = f" (key={key_col}" + (f", label={label_col})" if label_col else ")") if key_col else ""
        lines.append(f"SOURCE: {src}{key_suffix}")
    if params:
        lines.append(f"FILTERABLE_BY: {', '.join(params)}")
    if out_edges:
        outs = sorted({f"{e['label']} ({e['rel']})" for e in out_edges if e.get("label") and e.get("rel")})
        if outs:
            lines.append(f"CONNECTS_TO: {', '.join(outs)}")
    if in_edges:
        ins = sorted({f"{e['label']} ({e['rel']})" for e in in_edges if e.get("label") and e.get("rel")})
        if ins:
            lines.append(f"REFERENCED_BY: {', '.join(ins)}")

    return "\n".join(lines)


def embed_batch(client: OpenAI, texts: list[str]) -> list[list[float]]:
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD,
    )


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def write_nodes(conn, rows: list[dict[str, Any]]) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE nodes CASCADE")
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO nodes (element_id, node_id, label, entity_type, node_type, composed_text, embedding, props)
            VALUES %s
            """,
            [
                (
                    r["element_id"],
                    r["node_id"],
                    r["label"],
                    r["entity_type"],
                    r["node_type"],
                    r["composed_text"],
                    r["embedding"],
                    psycopg2.extras.Json(r["props"]),
                )
                for r in rows
            ],
        )
    conn.commit()


def write_edges(conn, edges: list[dict[str, Any]]) -> None:
    # Dedupe by edge_id to avoid CardinalityViolation if any duplicates slipped in.
    seen: dict[str, dict[str, Any]] = {}
    for e in edges:
        seen.setdefault(e["edge_id"], e)
    deduped = list(seen.values())
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO edges (edge_id, source_element_id, target_element_id, relationship_type)
            VALUES %s
            """,
            [
                (e["edge_id"], e["source_element_id"], e["target_element_id"], e["relationship_type"])
                for e in deduped
            ],
        )
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print composed text, skip writes")
    ap.add_argument("--limit", type=int, default=0, help="only process first N nodes (0 = all)")
    ap.add_argument("--show", type=int, default=2, help="print this many composed examples for sanity")
    args = ap.parse_args()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session(database=NEO4J_DATABASE) as s:
        node_rows = fetch_nodes_with_neighbors(s)
        edges = fetch_edges(s)
    driver.close()

    if args.limit:
        node_rows = node_rows[: args.limit]
        allowed = {r["element_id"] for r in node_rows}
        edges = [
            e for e in edges
            if e["source_element_id"] in allowed and e["target_element_id"] in allowed
        ]

    print(f"Fetched {len(node_rows)} nodes, {len(edges)} edges from Neo4j.")

    composed = [(r, compose_text(r)) for r in node_rows]
    for r, text in composed[: args.show]:
        print(f"\n===== {r['props'].get('label')} (node_id={r['node_id']}) =====\n{text}")

    if args.dry_run:
        print(f"\n[dry-run] Skipped embedding/write for {len(composed)} nodes.")
        return 0

    client = OpenAI(api_key=OPENAI_API_KEY)
    payloads: list[dict[str, Any]] = []
    for i in range(0, len(composed), BATCH_SIZE):
        batch = composed[i : i + BATCH_SIZE]
        vectors = embed_batch(client, [t for _, t in batch])
        for (r, text), vec in zip(batch, vectors):
            p = r["props"]
            payloads.append(
                {
                    "element_id": r["element_id"],
                    "node_id": r["node_id"],
                    "label": p.get("label") or "",
                    "entity_type": p.get("entity_type"),
                    "node_type": p.get("node_type"),
                    "composed_text": text,
                    "embedding": vec,
                    "props": p,
                }
            )
        print(f"  embedded {i + len(batch)}/{len(composed)}")

    conn = pg_connect()
    try:
        ensure_schema(conn)
        write_nodes(conn, payloads)
        write_edges(conn, edges)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM nodes")
            n_nodes = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM edges")
            n_edges = cur.fetchone()[0]
        print(f"Postgres now has {n_nodes} nodes and {n_edges} edges in `{PG_DATABASE}`.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
