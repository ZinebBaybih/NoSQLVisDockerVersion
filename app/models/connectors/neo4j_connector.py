# app/models/connectors/neo4j_connector.py
from neo4j import GraphDatabase

class Neo4jConnector:
    def __init__(self, host="localhost", port=7687, user="neo4j", password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.driver = None

    def connect(self):
        """Initialize Neo4j driver."""
        self.driver = GraphDatabase.driver(
            f"bolt://{self.host}:{self.port}", 
            auth=(self.user, self.password)
        )

    def disconnect(self):
        """Close the driver connection."""
        if self.driver:
            self.driver.close()
            self.driver = None

    # ----------------------------------------------------------------
    # BASIC DATA ACCESS
    # ----------------------------------------------------------------
    def list_labels(self):
        """Return all node labels."""
        with self.driver.session() as session:
            result = session.run("CALL db.labels()")
            return [r["label"] for r in result]

    def list_nodes(self, label_name, limit=50):
        """Return nodes for a given label (as dicts)."""
        with self.driver.session() as session:
            query = f"MATCH (n:`{label_name}`) RETURN properties(n) AS props LIMIT {limit}"
            result = session.run(query)
            return [r["props"] for r in result]

    def list_relationships(self, label_name, limit=25):
        """Return basic relationships for a given label."""
        with self.driver.session() as session:
            query = f"MATCH (n:`{label_name}`)-[r]->(m) RETURN n, r, m LIMIT {limit}"
            return list(result for result in session.run(query))

    # ----------------------------------------------------------------
    # Compatibility layer for backend
    # ----------------------------------------------------------------
    def list_databases(self):
        """Used by backend: treat labels as 'collections'."""
        with self.driver.session() as session:
            result = session.run("CALL db.labels()")
            data = []
            for record in result:
                label = record["label"]
                count = session.run(
                    f"MATCH (n:`{label}`) RETURN count(n) AS c"
                ).single()["c"]
                data.append({"name": label, "count": count})
            return data

    def list_collections(self, db_name=None):
        """Each label is like a collection."""
        return self.list_databases()

    def list_documents(self, db_name, col_name):
        """Documents correspond to node properties."""
        return self.list_nodes(col_name)
