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
        self.driver = GraphDatabase.driver(
            f"bolt://{self.host}:{self.port}",
            auth=(self.user, self.password)
        )

    def disconnect(self):
        if self.driver:
            self.driver.close()
            self.driver = None

    # ------------------------------------------------------------------
    def list_databases(self):
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
        return self.list_databases()

    def list_documents(self, db_name, col_name):
        with self.driver.session() as session:
            query = f"MATCH (n:`{col_name}`) RETURN properties(n) AS props LIMIT 50"
            return [r["props"] for r in session.run(query)]
