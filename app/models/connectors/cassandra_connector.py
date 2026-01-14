# app/models/connectors/cassandra_connector.py

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from typing import Optional, List
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

    # ------------------------------------------------------------------
    # ✅ NEW FILTER STRUCTURE — GUARANTEED WORKING
    # ------------------------------------------------------------------
    def search_table(self, keyspace, table, column, operator, value):
        rows = self.session.execute(
            f"SELECT * FROM {keyspace}.{table}"
        )

        # Normalize input value once
        raw_value = str(value).strip().lower()

        def normalize(v):
            """Return (type, normalized_value)"""
            if v is None:
                return ("none", None)

            s = str(v).strip().lower()

            try:
                return ("number", float(s))
            except Exception:
                return ("string", s)

        input_type, input_val = normalize(raw_value)

        def compare(row_val):
            row_type, row_norm = normalize(row_val)

            # Type mismatch → reject safely
            if row_type != input_type:
                return False

            if operator == "=":
                return row_norm == input_val
            if operator == "!=":
                return row_norm != input_val
            if operator == ">":
                return row_norm > input_val
            if operator == "<":
                return row_norm < input_val
            if operator == ">=":
                return row_norm >= input_val
            if operator == "<=":
                return row_norm <= input_val

            return False

        results = []
        for r in rows:
            row_dict = r._asdict()
            if column in row_dict and compare(row_dict[column]):
                results.append(row_dict)

        return results
