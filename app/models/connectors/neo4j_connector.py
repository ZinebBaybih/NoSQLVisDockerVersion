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
                    f"MATCH (n:`{label}`) RETURN count(n) AS c"
                ).single()["c"]

                data.append({
                    "name": label,
                    "count": count
                })

            return data

    def list_collections(self, db_name=None):
        return self.list_databases()

    def list_documents(self, db_name, col_name, offset=0, limit=PREVIEW_LIMIT):
        """Return node properties for a label"""
        with self.driver.session() as session:
            query = f"""
            MATCH (n:`{col_name}`)
            RETURN properties(n) AS props
            SKIP {int(offset)}
            LIMIT {int(limit)}
            """
            return [r["props"] for r in session.run(query)]


    def list_relationships(self, label, limit=NEO4J_GRAPH_PREVIEW_LIMIT):
        """
        Return a bounded relationship neighborhood for a label.
        This keeps the graph educational: selecting Student can reveal nearby
        Course, Instructor, Branch, and Department nodes without rendering the
        full database.
        """
        with self.driver.session() as session:
            query = f"""
            MATCH (start:`{label}`)
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
            MATCH (n:`{label}`)-[r]-()
            RETURN count(r) AS c
            """
            return session.run(query).single()["c"]
