"""Neo4j Knowledge Graph tools for the Reporting Agent."""
from __future__ import annotations

import time
import logging
from typing import Any, Optional

from neo4j import GraphDatabase, Driver
from config.settings import config

logger = logging.getLogger(__name__)
logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
logging.getLogger("neo4j.io").setLevel(logging.WARNING)


class Neo4jTool:
    """Manages Neo4j connections and query execution."""

    def __init__(self):
        self._driver: Optional[Driver] = None

    @property
    def driver(self) -> Driver:
        if self._driver is None:
            cfg = config.neo4j
            self._driver = GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password))
            self._driver.verify_connectivity()
        return self._driver

    def close(self):
        if self._driver:
            self._driver.close()
            self._driver = None

    def get_schema(self) -> str:
        """Discover the full KG schema: node labels, properties, relationships,
        all BKGNode instances grouped by entity_type, and node-to-node relationship map."""
        db = config.neo4j.database

        with self.driver.session(database=db) as session:
            node_info = session.run(
                "CALL db.schema.nodeTypeProperties() "
                "YIELD nodeType, propertyName, propertyTypes, mandatory "
                "RETURN nodeType, collect({name: propertyName, types: propertyTypes, mandatory: mandatory}) AS properties"
            ).data()

            rel_info = session.run(
                "CALL db.schema.relTypeProperties() "
                "YIELD relType, propertyName, propertyTypes, mandatory "
                "RETURN relType, collect({name: propertyName, types: propertyTypes, mandatory: mandatory}) AS properties"
            ).data()

            rel_patterns = session.run(
                "MATCH (a)-[r]->(b) "
                "WITH labels(a) AS srcLabels, type(r) AS relType, labels(b) AS tgtLabels "
                "RETURN DISTINCT srcLabels, relType, tgtLabels ORDER BY relType"
            ).data()

            node_instances = session.run(
                "MATCH (n:BKGNode) "
                "RETURN n.entity_type AS entity_type, n.node_id AS node_id, n.label AS label "
                "ORDER BY n.entity_type, n.node_id"
            ).data()

            node_relationships = session.run(
                "MATCH (a:BKGNode)-[r:RELATES_TO]->(b:BKGNode) "
                "RETURN a.node_id AS source, r.relationship_type AS rel_type, b.node_id AS target "
                "ORDER BY a.entity_type, a.node_id"
            ).data()

        # Build formatted output
        schema_lines = ["=== KNOWLEDGE GRAPH SCHEMA ===\n"]

        schema_lines.append("── Node Labels & Properties ──")
        for row in node_info:
            node_type = row["nodeType"]
            props_list = []
            for p in row["properties"]:
                if not p["name"]:
                    continue
                types_str = "/".join(p["types"]) if p["types"] else "Unknown"
                req = " (required)" if p.get("mandatory") else ""
                props_list.append(f"{p['name']}: {types_str}{req}")
            schema_lines.append(f"  {node_type}")
            for prop in props_list:
                schema_lines.append(f"    - {prop}")

        schema_lines.append("\n── Relationship Types & Properties ──")
        for row in rel_info:
            rel_type = row["relType"]
            props_list = []
            for p in row["properties"]:
                if not p["name"]:
                    continue
                types_str = "/".join(p["types"]) if p["types"] else "Unknown"
                req = " (required)" if p.get("mandatory") else ""
                props_list.append(f"{p['name']}: {types_str}{req}")
            schema_lines.append(f"  {rel_type}")
            for prop in props_list:
                schema_lines.append(f"    - {prop}")

        schema_lines.append("\n── Relationship Patterns ──")
        for row in rel_patterns:
            src = ":".join(row["srcLabels"])
            tgt = ":".join(row["tgtLabels"])
            schema_lines.append(f"  (:{src})-[:{row['relType']}]->(:{tgt})")

        schema_lines.append("\n── BKG Nodes (by entity type) ──")
        current_type = None
        for row in node_instances:
            et = row.get("entity_type", "unknown")
            if et != current_type:
                current_type = et
                schema_lines.append(f"\n  [{et}]")
            label_str = f" — {row['label']}" if row.get("label") else ""
            schema_lines.append(f"    • {row['node_id']}{label_str}")

        schema_lines.append("\n── Node Relationships ──")
        for row in node_relationships:
            rel = row.get("rel_type") or "RELATES_TO"
            schema_lines.append(f"  ({row['source']}) —[{rel}]→ ({row['target']})")

        return "\n".join(schema_lines)

    def run_cypher(self, query: str, params: dict[str, Any] | None = None) -> dict:
        db = config.neo4j.database
        params = params or {}
        start = time.perf_counter()
        try:
            with self.driver.session(database=db) as session:
                result = session.run(query, params)
                records = [record.data() for record in result]
                summary = result.consume()
            elapsed_ms = (time.perf_counter() - start) * 1000
            return {
                "status": "success",
                "records": records,
                "count": len(records),
                "elapsed_ms": round(elapsed_ms, 2),
                "query": query,
            }
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            return {
                "status": "error",
                "error": str(e),
                "elapsed_ms": round(elapsed_ms, 2),
                "query": query,
                "records": [],
                "count": 0,
            }

    def run_cypher_safe(self, query: str, params: dict[str, Any] | None = None) -> dict:
        upper = query.upper().strip()
        write_keywords = ["CREATE", "MERGE", "DELETE", "DETACH", "SET ", "REMOVE "]
        for kw in write_keywords:
            if kw in upper and not upper.startswith("//"):
                return {
                    "status": "error",
                    "error": f"Write operations not allowed. Detected: {kw.strip()}",
                    "records": [],
                    "count": 0,
                }
        return self.run_cypher(query, params)


# Module-level singleton
neo4j_tool = Neo4jTool()
