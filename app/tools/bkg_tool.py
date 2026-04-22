# bkg_tool.py
import re
import json
import logging
from neo4j import GraphDatabase
from config.settings import config

logger = logging.getLogger(__name__)


class BKGTool:
    """
    Agent's understanding layer — backed by Neo4j.

    All nodes in Neo4j use the unified `BKGNode` label with a `node_id` property.
    The `entity_type` property distinguishes node categories:
      - core        -> business entities with `map_*` database mapping properties
      - context     -> contextual/reference entities
      - transaction -> transactional entities
      - reference   -> reference/lookup entities
      - kpi         -> computed KPI metrics with `kpi_*` calculation properties

    Relationships use `RELATES_TO` edges with `relationship_type` property.
    """

    # Common aliases for quick node lookup
    STATIC_ALIASES = {
        "GC": "general_contractor",
        "gc": "general_contractor",
        "generalcontractor": "general_contractor",
        "NAS": "nas_session",
        "nas": "nas_session",
        "IX": "integration",
        "ix": "integration",
        "CX": "construction_progress",
        "cx": "construction_progress",
        "BOM": "bill_of_materials",
        "bom": "bill_of_materials",
        "NTP": "ntp",
        "SSV": "acceptance",
        "COP": "acceptance",
    }

    def __init__(self):
        cfg = config.neo4j
        self._driver = GraphDatabase.driver(
            cfg.uri,
            auth=(cfg.user, cfg.password),
        )
        self._driver.verify_connectivity()
        self.aliases = self.STATIC_ALIASES.copy()

        # Startup diagnostics
        counts = self._run(
            """
            MATCH (n:BKGNode)
            RETURN n.entity_type AS entity_type, count(n) AS cnt
            """
        )
        total = sum(r["cnt"] for r in counts)
        self._node_count = total

        if total > 0:
            breakdown = ", ".join(f"{r['entity_type']}={r['cnt']}" for r in counts)
            print(f"Neo4j: Found {total} BKGNodes ({breakdown})")
        else:
            print("Neo4j: BKGNode label exists but NO NODES FOUND — check your database")
            print(f"  Connected to: {cfg.uri}")

    @property
    def nodes(self):
        """Compatibility shim for health check: returns count as len-able."""
        return range(self._node_count)

    def close(self):
        self._driver.close()

    def resolve_id(self, raw_id: str) -> str:
        """Resolve alias to canonical node_id. Returns as-is if already canonical."""
        return self.aliases.get(raw_id, self.aliases.get(raw_id.upper(), raw_id))

    def query(self, request: dict) -> dict:
        mode = request.get("mode")
        try:
            if mode == "get_node":
                return self._get_node(request.get("node_id", ""))
            elif mode == "find_relevant":
                return self._find_relevant(request.get("question", ""))
            elif mode == "traverse":
                return self._traverse(
                    request.get("start", ""),
                    request.get("depth", 2),
                    request.get("rel_type"),
                )
            elif mode == "get_kpi":
                return self._get_kpi(request.get("node_id", ""))
            elif mode == "schema":
                return self._get_schema(request.get("table_name"))
            else:
                return {"error": f"Unknown mode: {mode}"}
        except Exception as e:
            logger.error("BKGTool.query error (mode=%s): %s", mode, e)
            return {"error": str(e)}

    # ── Private helpers ──────────────────────────────────────────────────────

    def _run(self, cypher: str, **params) -> list:
        with self._driver.session(database=config.neo4j.database) as session:
            result = session.run(cypher, **params)
            return [r.data() for r in result]

    def _parse_json_props(self, props: dict) -> dict:
        """
        Clean up raw Neo4j property bag:
        - Parse JSON strings back to dicts/lists.
        - Leave native arrays (string[]) untouched.
        """
        out = {}
        for k, v in props.items():
            if isinstance(v, str) and v.startswith(("{", "[")):
                try:
                    v = json.loads(v)
                except (json.JSONDecodeError, ValueError):
                    pass
            out[k] = v
        return out

    # ── Mode: get_node ───────────────────────────────────────────────────────

    def _get_node(self, raw_id: str) -> dict:
        node_id = self.resolve_id(raw_id)

        rows = self._run(
            """
            MATCH (n:BKGNode {node_id: $nid})
            RETURN
                n.node_id            AS node_id,
                n.name               AS name,
                n.label              AS label,
                n.entity_type        AS entity_type,
                n.definition         AS definition,
                n.nl_description     AS nl_description,
                n.map_table_name     AS map_table_name,
                n.map_database_name  AS map_database_name,
                n.map_key_column     AS map_key_column,
                n.map_label_column   AS map_label_column,
                n.map_python_function AS map_python_function,
                n.map_contract       AS map_contract
            """,
            nid=node_id,
        )
        if not rows:
            return {
                "error": (
                    f"'{raw_id}' not found (resolved to '{node_id}'). "
                    "Try find_relevant to search by keyword."
                )
            }

        node = self._parse_json_props(rows[0])

        # Outgoing relationships
        out_rows = self._run(
            """
            MATCH (n:BKGNode {node_id: $nid})-[r]->(t:BKGNode)
            RETURN type(r) AS rel_label,
                   r.relationship_type AS relationship_type,
                   r.relationship AS relationship,
                   t.node_id AS target,
                   t.label AS target_label,
                   t.entity_type AS target_entity_type
            """,
            nid=node_id,
        )
        node["outgoing"] = [
            {
                "relationship": r.get("relationship") or r.get("relationship_type") or r["rel_label"],
                "target": r["target"],
                "target_label": r.get("target_label"),
                "target_entity_type": r.get("target_entity_type"),
            }
            for r in out_rows
        ]

        # Incoming relationships
        in_rows = self._run(
            """
            MATCH (s:BKGNode)-[r]->(n:BKGNode {node_id: $nid})
            RETURN type(r) AS rel_label,
                   r.relationship_type AS relationship_type,
                   r.relationship AS relationship,
                   s.node_id AS source,
                   s.label AS source_label,
                   s.entity_type AS source_entity_type
            """,
            nid=node_id,
        )
        node["incoming"] = [
            {
                "relationship": r.get("relationship") or r.get("relationship_type") or r["rel_label"],
                "source": r["source"],
                "source_label": r.get("source_label"),
                "source_entity_type": r.get("source_entity_type"),
            }
            for r in in_rows
        ]

        return node

    # ── Mode: find_relevant ──────────────────────────────────────────────────

    def _find_relevant(self, question: str) -> dict:
        """
        Keyword search over BKGNode properties.
        Returns up to 10 nodes ranked by relevance score.
        """
        q_words = list(set(re.findall(r"\w+", question.lower())))
        if not q_words:
            return {"relevant_nodes": []}

        rows = self._run(
            """
            MATCH (n:BKGNode)
            WHERE any(w IN $words
                WHERE toLower(coalesce(n.node_id, ''))          CONTAINS w
                   OR toLower(coalesce(n.name, ''))             CONTAINS w
                   OR toLower(coalesce(n.label, ''))            CONTAINS w
                   OR toLower(coalesce(n.definition, ''))       CONTAINS w
                   OR toLower(coalesce(n.nl_description, ''))   CONTAINS w
                   OR toLower(coalesce(n.entity_type, ''))      CONTAINS w
                   OR toLower(coalesce(n.kpi_name, ''))         CONTAINS w
                   OR toLower(coalesce(n.kpi_description, ''))  CONTAINS w
                   OR toLower(coalesce(n.kpi_formula_description, ''))  CONTAINS w
            )
            RETURN
                n.node_id          AS node_id,
                n.name             AS name,
                n.label            AS label,
                n.entity_type      AS entity_type,
                n.definition       AS definition,
                n.nl_description   AS nl_description,
                n.map_table_name   AS map_table_name,
                n.kpi_name         AS kpi_name,
                n.kpi_description  AS kpi_description,
                n.kpi_formula_description  AS kpi_formula_description
            LIMIT 15
            """,
            words=q_words,
        )

        results = []
        for r in rows:
            text = " ".join(
                filter(None, [
                    str(r.get("node_id") or ""),
                    str(r.get("name") or ""),
                    str(r.get("label") or ""),
                    str(r.get("definition") or ""),
                    str(r.get("nl_description") or ""),
                    str(r.get("kpi_name") or ""),
                    str(r.get("kpi_description") or ""),
                ])
            ).lower()
            score = sum(1 for w in q_words if w in text)

            neighbors = self._run(
                """
                MATCH (n:BKGNode {node_id: $nid})-[r]->(t:BKGNode)
                RETURN t.node_id AS target,
                       r.relationship_type AS rel_type
                LIMIT 5
                """,
                nid=r["node_id"],
            )

            entry = {
                "node_id": r["node_id"],
                "name": r.get("name"),
                "label": r.get("label"),
                "entity_type": r.get("entity_type"),
                "definition": r.get("definition") or "",
                "relevance_score": score,
                "neighbors": [
                    {"target": n["target"], "rel_type": n.get("rel_type")}
                    for n in neighbors
                ],
            }

            if r.get("entity_type") == "kpi":
                entry["kpi_name"] = r.get("kpi_name")
                entry["kpi_description"] = r.get("kpi_description") or ""
            elif r.get("map_table_name"):
                entry["map_table_name"] = r.get("map_table_name")

            results.append(entry)

        results.sort(key=lambda x: x["relevance_score"], reverse=True)
        return {"relevant_nodes": results[:10]}

    # ── Mode: traverse ───────────────────────────────────────────────────────

    def _traverse(self, raw_start: str, depth: int = 2, rel_type: str = None) -> dict:
        node_id = self.resolve_id(raw_start)
        depth = min(max(depth, 1), 4)

        exists = self._run(
            "MATCH (n:BKGNode {node_id: $nid}) RETURN n LIMIT 1",
            nid=node_id,
        )
        if not exists:
            return {"error": f"Node '{raw_start}' (resolved: '{node_id}') not found"}

        if rel_type:
            rows = self._run(
                f"""
                MATCH (start:BKGNode {{node_id: $nid}})-[r*1..{depth}]->(end:BKGNode)
                WHERE all(rel IN r WHERE rel.relationship_type = $rtype)
                WITH start, r, end
                UNWIND r AS rel
                WITH startNode(rel) AS src, rel, endNode(rel) AS tgt
                RETURN
                    src.node_id                       AS from_node,
                    coalesce(rel.relationship_type,
                             rel.relationship,
                             type(rel))               AS relationship,
                    tgt.node_id                       AS to_node,
                    tgt.label                         AS to_label,
                    tgt.entity_type                   AS to_entity_type,
                    tgt.definition                    AS definition,
                    tgt.map_table_name                AS map_table_name,
                    tgt.kpi_name                      AS kpi_name
                LIMIT 30
                """,
                nid=node_id,
                rtype=rel_type,
            )
        else:
            rows = self._run(
                f"""
                MATCH (start:BKGNode {{node_id: $nid}})-[r*1..{depth}]->(end:BKGNode)
                WITH start, r, end
                UNWIND r AS rel
                With startNode(rel) AS src, rel, endNode(rel) AS tgt
                RETURN
                    src.node_id                       AS from_node,
                    coalesce(rel.relationship_type,
                             rel.relationship,
                             type(rel))               AS relationship,
                    tgt.node_id                       AS to_node,
                    tgt.label                         AS to_label,
                    tgt.entity_type                   AS to_entity_type,
                    tgt.definition                    AS definition,
                    tgt.map_table_name                AS map_table_name,
                    tgt.kpi_name                      AS kpi_name
                LIMIT 30
                """,
                nid=node_id,
            )

        paths = []
        discovered = {}
        for r in rows:
            if not r.get("to_node"):
                continue
            paths.append({
                "from": r["from_node"],
                "relationship": r["relationship"],
                "to": r["to_node"],
            })
            tgt = r["to_node"]
            if tgt not in discovered:
                discovered[tgt] = {
                    "node_id": tgt,
                    "label": r.get("to_label"),
                    "entity_type": r.get("to_entity_type"),
                    "definition": r.get("definition") or "",
                    "map_table_name": r.get("map_table_name"),
                    "kpi_name": r.get("kpi_name"),
                }

        return {"paths": paths, "discovered_nodes": discovered}

    # ── Mode: get_kpi ─────────────────────────────────────────────────────

    def _get_kpi(self, raw_id: str) -> dict:
        """Fetch KPI details for a node with entity_type='kpi'."""
        node_id = self.resolve_id(raw_id)

        rows = self._run(
            """
            MATCH (n:BKGNode {node_id: $nid})
            WHERE n.entity_type = 'kpi'
            RETURN
                n.node_id                   AS node_id,
                n.name                      AS name,
                n.label                     AS label,
                n.entity_type               AS entity_type,
                n.definition                AS definition,
                n.nl_description            AS nl_description,
                n.kpi_name                  AS kpi_name,
                n.kpi_kpi_id                AS kpi_kpi_id,
                n.kpi_description           AS kpi_description,
                n.kpi_formula_description   AS kpi_formula_description,
                n.kpi_business_logic        AS kpi_business_logic,
                n.kpi_python_function       AS kpi_python_function,
                n.kpi_contract              AS kpi_contract,
                n.kpi_relationship_type     AS kpi_relationship_type,
                n.kpi_related_core_node_ids AS kpi_related_core_node_ids,
                n.kpi_source_tables         AS kpi_source_tables,
                n.kpi_source_columns        AS kpi_source_columns,
                n.kpi_dimensions            AS kpi_dimensions,
                n.kpi_filters               AS kpi_filters,
                n.kpi_output_schema         AS kpi_output_schema
            """,
            nid=node_id,
        )
        if rows:
            kpi_data = self._parse_json_props(rows[0])

            related = self._run(
                """
                MATCH (n:BKGNode {node_id: $nid})-[r]->(t:BKGNode)
                WHERE t.entity_type IN ['core', 'context', 'transaction']
                RETURN t.node_id AS node_id,
                       t.label AS label,
                       t.map_table_name AS map_table_name,
                       r.relationship_type AS relationship_type
                """,
                nid=node_id,
            )
            kpi_data["related_core_nodes"] = [
                {
                    "node_id": r["node_id"],
                    "label": r.get("label"),
                    "map_table_name": r.get("map_table_name"),
                    "relationship_type": r.get("relationship_type"),
                }
                for r in related
            ]
            return kpi_data

        # If not a KPI node, check if it's a core node and find KPIs referencing it
        core_check = self._run(
            "MATCH (n:BKGNode {node_id: $nid}) RETURN n.entity_type AS et",
            nid=node_id,
        )
        if core_check:
            related_kpis = self._run(
                """
                MATCH (k:BKGNode)-[r]->(n:BKGNode {node_id: $nid})
                WHERE k.entity_type = 'kpi'
                RETURN k.node_id AS node_id,
                       k.kpi_name AS kpi_name,
                       k.kpi_description AS kpi_description
                UNION
                MATCH (k:BKGNode)
                WHERE k.entity_type = 'kpi'
                  AND $nid IN coalesce(k.kpi_related_core_node_ids, [])
                RETURN k.node_id AS node_id,
                       k.kpi_name AS kpi_name,
                       k.kpi_description AS kpi_description
                """,
                nid=node_id,
            )
            if related_kpis:
                return {
                    "note": f"'{node_id}' is a {core_check[0]['et']} node, not a KPI. Related KPIs:",
                    "related_kpis": [
                        {
                            "node_id": r["node_id"],
                            "kpi_name": r.get("kpi_name"),
                            "kpi_description": r.get("kpi_description") or "",
                        }
                        for r in related_kpis
                    ],
                }

        return {"error": f"KPI node '{raw_id}' not found. Try find_relevant to search by keyword."}

    # ── Mode: schema ─────────────────────────────────────────────────────────

    def _get_schema(self, table_name: str = None) -> dict:
        """Return database table mappings from BKGNode `map_*` properties."""
        if table_name:
            rows = self._run(
                """
                MATCH (n:BKGNode)
                WHERE n.map_table_name = $tname
                RETURN
                    n.node_id            AS node_id,
                    n.name               AS name,
                    n.label              AS label,
                    n.entity_type        AS entity_type,
                    n.definition         AS definition,
                    n.map_table_name     AS map_table_name,
                    n.map_database_name  AS map_database_name,
                    n.map_key_column     AS map_key_column,
                    n.map_label_column   AS map_label_column,
                    n.map_sql_template   AS map_sql_template,
                    n.map_python_function AS map_python_function
                """,
                tname=table_name,
            )
            if not rows:
                return {"error": f"No BKGNode found with map_table_name='{table_name}'"}

            nodes = []
            for r in rows:
                node = self._parse_json_props(r)
                nodes.append(node)

            return {"table_name": table_name, "nodes": nodes}

        rows = self._run(
            """
            MATCH (n:BKGNode)
            WHERE n.map_table_name IS NOT NULL
            RETURN
                n.map_table_name     AS table_name,
                n.map_database_name  AS database_name,
                n.node_id            AS node_id,
                n.label              AS label,
                n.map_key_column     AS key_column,
                n.map_label_column   AS label_column
            ORDER BY n.map_table_name, n.node_id
            """
        )

        tables: dict = {}
        for r in rows:
            tname = r["table_name"]
            if tname not in tables:
                tables[tname] = {
                    "table_name": tname,
                    "database_name": r.get("database_name"),
                    "nodes": [],
                }
            tables[tname]["nodes"].append({
                "node_id": r["node_id"],
                "label": r.get("label"),
                "key_column": r.get("key_column"),
                "label_column": r.get("label_column"),
            })

        return {"tables": list(tables.values())}
