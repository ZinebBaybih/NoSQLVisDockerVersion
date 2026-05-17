# app/models/connectors/neo4j_connector.py
from neo4j import GraphDatabase

from config import NEO4J_GRAPH_PREVIEW_LIMIT, PREVIEW_LIMIT


class Neo4jConnector:
    def __init__(self, host="localhost", port=7687, user="neo4j", password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.driver = None

    def connect(self):
        self.driver = GraphDatabase.driver(
            f"bolt://{self.host}:{self.port}",
            auth=(self.user, self.password)
        )

    def disconnect(self):
        if self.driver:
            self.driver.close()
            self.driver = None


    def list_databases(self):
        """Return labels with node counts"""
        with self.driver.session() as session:
            result = session.run("CALL db.labels()")
            data = []

            for record in result:
                label = record["label"]
                count = session.run(
                    f"MATCH (n:{self._quote_identifier(label)}) RETURN count(n) AS c"
                ).single()["c"]

                data.append({
                    "name": label,
                    "count": count
                })

            return data

    def list_collections(self, db_name=None):
        return self.list_databases()

    def get_metadata(self):
        """Return graph-level metadata for the dashboard."""
        with self.driver.session() as session:
            label_count = session.run("CALL db.labels() YIELD label RETURN count(label) AS c").single()["c"]
            node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            relationship_count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
            return {
                "labels": label_count,
                "nodes": node_count,
                "relationships": relationship_count,
            }

    def list_documents(self, db_name, col_name, offset=0, limit=PREVIEW_LIMIT):
        """Return node properties for a label"""
        with self.driver.session() as session:
            query = f"""
            MATCH (n:{self._quote_identifier(col_name)})
            RETURN properties(n) AS props
            SKIP {int(offset)}
            LIMIT {int(limit)}
            """
            return [r["props"] for r in session.run(query)]

    def get_label_properties(self, label, sample_limit=1000):
        """Return property names observed on a bounded sample of nodes for a label."""
        with self.driver.session() as session:
            query = f"""
            MATCH (n:{self._quote_identifier(label)})
            WITH n
            LIMIT {int(sample_limit)}
            UNWIND keys(n) AS property
            RETURN DISTINCT property
            ORDER BY property
            """
            return [r["property"] for r in session.run(query)]

    def get_indexed_filter_properties(self, label=None):
        """Return single-property indexed node properties, optionally scoped to a label."""
        with self.driver.session() as session:
            try:
                result = session.run(
                    """
                    SHOW INDEXES
                    YIELD entityType, labelsOrTypes, properties
                    WHERE entityType = 'NODE'
                      AND ($label IS NULL OR $label IN labelsOrTypes)
                      AND size(properties) = 1
                    RETURN DISTINCT properties[0] AS property
                    ORDER BY property
                    """,
                    label=label,
                )
                return [r["property"] for r in result]
            except Exception:
                return []

    def search_indexed_nodes(self, label, property_name, value, offset=0, limit=PREVIEW_LIMIT):
        indexed_properties = self.get_indexed_filter_properties(label)
        if property_name not in indexed_properties:
            return [], 0, indexed_properties

        values = self._search_value_variants(value)
        with self.driver.session() as session:
            label_id = self._quote_identifier(label)
            property_id = self._quote_identifier(property_name)
            count_query = f"""
            MATCH (n:{label_id})
            WHERE n.{property_id} IN $values
            RETURN count(n) AS total
            """
            data_query = f"""
            MATCH (n:{label_id})
            WHERE n.{property_id} IN $values
            RETURN properties(n) AS props
            SKIP {int(offset)}
            LIMIT {int(limit)}
            """
            total = session.run(count_query, values=values).single()["total"]
            nodes = [r["props"] for r in session.run(data_query, values=values)]
            return nodes, total, indexed_properties


    def list_relationships(self, label, limit=NEO4J_GRAPH_PREVIEW_LIMIT):
        """
        Return a bounded relationship neighborhood for a label.
        This keeps the graph educational: selecting Student can reveal nearby
        Course, Instructor, Branch, and Department nodes without rendering the
        full database.
        """
        with self.driver.session() as session:
            query = f"""
            MATCH (start:{self._quote_identifier(label)})
            WITH start
            ORDER BY elementId(start)
            LIMIT 12
            MATCH path = (start)-[*1..2]-(neighbor)
            UNWIND relationships(path) AS rel
            WITH DISTINCT startNode(rel) AS n, rel AS r, endNode(rel) AS m
            RETURN n, r, m
            LIMIT {int(limit)}
            """
            return list(session.run(query))

    def count_relationships(self, label):
        """Return the total number of relationships touching nodes with a label."""
        with self.driver.session() as session:
            query = f"""
            MATCH (n:{self._quote_identifier(label)})-[r]-()
            RETURN count(r) AS c
            """
            return session.run(query).single()["c"]

    def _quote_identifier(self, value):
        return "`{}`".format(str(value).replace("`", "``"))

    def _search_value_variants(self, value):
        raw_value = str(value).strip()
        values = [raw_value]
        lowered = raw_value.lower()

        if lowered in ("true", "false"):
            values.append(lowered == "true")

        try:
            values.append(int(raw_value))
        except ValueError:
            pass

        try:
            values.append(float(raw_value))
        except ValueError:
            pass

        unique_values = []
        for item in values:
            if item not in unique_values:
                unique_values.append(item)
        return unique_values
