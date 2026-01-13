# app/models/connectors/cassandra_connector.py

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from typing import Optional, List, Dict, Any
import time


class CassandraConnector:
    def __init__(
        self,
        host="localhost",
        port=9042,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.cluster = None
        self.session = None

    # ------------------------------------------------------------------
    def connect(self):
        auth_provider = None
        if self.user and self.password:
            auth_provider = PlainTextAuthProvider(
                username=self.user, password=self.password
            )

        # Cassandra in Docker needs time to boot
        for _ in range(10):
            try:
                self.cluster = Cluster(
                    [self.host],
                    port=self.port,
                    auth_provider=auth_provider,
                    load_balancing_policy=RoundRobinPolicy()
                )
                self.session = self.cluster.connect()
                self.session.execute("SELECT now() FROM system.local")
                return
            except Exception:
                time.sleep(5)

        raise Exception("Unable to connect to Cassandra")

    # ------------------------------------------------------------------
    def disconnect(self):
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

    # ------------------------------------------------------------------
    def list_keyspaces(self) -> List[str]:
        return list(self.cluster.metadata.keyspaces.keys())

    def list_tables(self, keyspace: str) -> List[str]:
        ks_meta = self.cluster.metadata.keyspaces.get(keyspace)
        if not ks_meta:
            return []
        return list(ks_meta.tables.keys())

    def fetch_sample(self, keyspace, table, limit=1000):
        rows = self.session.execute(
            f"SELECT * FROM {keyspace}.{table} LIMIT {limit}"
        )
        return [dict(r._asdict()) for r in rows]

    def fetch_all(self, keyspace, table):
        rows = self.session.execute(
            f"SELECT * FROM {keyspace}.{table}"
        )
        return [dict(r._asdict()) for r in rows]

    def search_table(self, keyspace, table, column, operator, value):
        query = f"SELECT * FROM {keyspace}.{table} ALLOW FILTERING"
        rows = self.session.execute(query)

        def match(row):
            v = getattr(row, column)
            if operator == "=":
                return v == value
            if operator == "!=":
                return v != value
            if operator == ">":
                return v > value
            if operator == "<":
                return v < value
            if operator == ">=":
                return v >= value
            if operator == "<=":
                return v <= value
            return False

        return [dict(r._asdict()) for r in rows if match(r)]
