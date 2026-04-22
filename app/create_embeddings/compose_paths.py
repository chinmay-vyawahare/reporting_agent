"""
Enumerate 1..N-hop simple directed paths in the BKG, linearize each as text,
embed with OpenAI, and persist to Postgres `paths` table.

Run:
    python compose_paths.py                     # default max_hops=3
    python compose_paths.py --max-hops 2
    python compose_paths.py --dry-run --show 5  # preview path texts
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]
PG_DATABASE = os.environ["PG_DATABASE"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

EMBED_MODEL = "text-embedding-3-small"
BATCH_SIZE = 128

DDL = """
DROP TABLE IF EXISTS paths;
CREATE TABLE paths (
    path_id             SERIAL PRIMARY KEY,
    hops                INT NOT NULL,
    node_element_ids    TEXT[] NOT NULL,
    node_labels         TEXT[] NOT NULL,
    relationship_types  TEXT[] NOT NULL,
    composed_text       TEXT NOT NULL,
    embedding           FLOAT8[] NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_paths_hops ON paths(hops);
"""


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD,
    )


def load_graph(conn) -> tuple[dict[str, dict[str, Any]], list[tuple[str, str, str]]]:
    """Return (node_by_eid, unique_edges as (src, rel, tgt) tuples)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT element_id, label, entity_type FROM nodes")
        nodes = {r["element_id"]: dict(r) for r in cur.fetchall()}
        cur.execute(
            "SELECT source_element_id, target_element_id, relationship_type FROM edges"
        )
        raw = cur.fetchall()
    uniq: set[tuple[str, str, str]] = set()
    for e in raw:
        uniq.add((e["source_element_id"], e["relationship_type"], e["target_element_id"]))
    return nodes, sorted(uniq)


def enumerate_paths(
    nodes: dict[str, dict[str, Any]],
    edges: list[tuple[str, str, str]],
    max_hops: int,
) -> list[dict[str, Any]]:
    """DFS-enumerate simple directed paths of length 1..max_hops (no node repeats)."""
    adj: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for src, rel, tgt in edges:
        adj[src].append((rel, tgt))

    paths: list[dict[str, Any]] = []

    def dfs(current_nodes: list[str], current_rels: list[str]):
        if 1 <= len(current_rels) <= max_hops:
            paths.append(
                {
                    "hops": len(current_rels),
                    "node_eids": list(current_nodes),
                    "rel_types": list(current_rels),
                }
            )
        if len(current_rels) >= max_hops:
            return
        last = current_nodes[-1]
        for rel, tgt in adj.get(last, []):
            if tgt in current_nodes:  # simple path: no revisits
                continue
            current_nodes.append(tgt)
            current_rels.append(rel)
            dfs(current_nodes, current_rels)
            current_nodes.pop()
            current_rels.pop()

    for start in nodes:
        dfs([start], [])
    return paths


def linearize(path: dict[str, Any], nodes: dict[str, dict[str, Any]]) -> tuple[str, list[str]]:
    """Return (composed_text, node_labels) for a path."""
    labels = [nodes[eid]["label"] for eid in path["node_eids"]]
    parts = [labels[0]]
    for rel, lbl in zip(path["rel_types"], labels[1:]):
        parts.append(f"--[{rel}]-->")
        parts.append(lbl)
    text = "PATH: " + " ".join(parts)
    return text, labels


def embed_batch(client: OpenAI, texts: list[str]) -> list[list[float]]:
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]


def write_paths(conn, rows: list[dict[str, Any]]) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO paths (hops, node_element_ids, node_labels, relationship_types, composed_text, embedding)
            VALUES %s
            """,
            [
                (
                    r["hops"],
                    r["node_eids"],
                    r["node_labels"],
                    r["rel_types"],
                    r["composed_text"],
                    r["embedding"],
                )
                for r in rows
            ],
            page_size=500,
        )
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-hops", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show", type=int, default=5)
    ap.add_argument("--cap", type=int, default=0, help="cap total paths (0=no cap)")
    args = ap.parse_args()

    conn = pg_connect()
    try:
        nodes, edges = load_graph(conn)
        print(f"Loaded {len(nodes)} nodes, {len(edges)} unique edges from Postgres.")

        print(f"Enumerating simple paths of length 1..{args.max_hops} ...")
        paths = enumerate_paths(nodes, edges, args.max_hops)
        by_hops: dict[int, int] = defaultdict(int)
        for p in paths:
            by_hops[p["hops"]] += 1
        print(f"Found {len(paths)} paths: " + ", ".join(f"{h}-hop={by_hops[h]}" for h in sorted(by_hops)))

        if args.cap and len(paths) > args.cap:
            paths = paths[: args.cap]
            print(f"Capped to {len(paths)}.")

        # Linearize
        for p in paths:
            text, labels = linearize(p, nodes)
            p["composed_text"] = text
            p["node_labels"] = labels

        for p in paths[: args.show]:
            print(f"  [{p['hops']}h] {p['composed_text']}")

        if args.dry_run:
            print("[dry-run] Skipped embedding + write.")
            return 0

        client = OpenAI(api_key=OPENAI_API_KEY)
        for i in range(0, len(paths), BATCH_SIZE):
            batch = paths[i : i + BATCH_SIZE]
            vectors = embed_batch(client, [p["composed_text"] for p in batch])
            for p, v in zip(batch, vectors):
                p["embedding"] = v
            print(f"  embedded {min(i + BATCH_SIZE, len(paths))}/{len(paths)}")

        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
        write_paths(conn, paths)

        with conn.cursor() as cur:
            cur.execute("SELECT count(*), avg(array_length(embedding,1)) FROM paths")
            n, dim = cur.fetchone()
        print(f"Wrote {n} paths ({int(dim)}-d embeddings) to `paths`.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
