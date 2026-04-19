from typing import List, Optional
import time

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy


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

    def connect(self):
        auth_provider = None
        if self.user and self.password:
            auth_provider = PlainTextAuthProvider(
                username=self.user,
                password=self.password,
            )

        last_error = None
        for _ in range(10):
            try:
                self.cluster = Cluster(
                    [self.host],
                    port=self.port,
                    auth_provider=auth_provider,
                    load_balancing_policy=RoundRobinPolicy(),
                    connect_timeout=5,
                    control_connection_timeout=5,
                )
                self.session = self.cluster.connect()
                self.session.execute("SELECT release_version FROM system.local")
                return
            except Exception as exc:
                last_error = exc
                self.disconnect()
                time.sleep(2)

        raise Exception(f"Unable to connect to Cassandra: {last_error}")

    def disconnect(self):
        if self.session:
            try:
                self.session.shutdown()
            except Exception:
                pass
        if self.cluster:
            try:
                self.cluster.shutdown()
            except Exception:
                pass
        self.session = None
        self.cluster = None

    def list_keyspaces(self) -> List[str]:
        if not self.cluster:
            return []
        return list(self.cluster.metadata.keyspaces.keys())

    def list_tables(self, keyspace: str) -> List[str]:
        if not self.cluster:
            return []
        ks_meta = self.cluster.metadata.keyspaces.get(keyspace)
        if not ks_meta:
            return []
        return list(ks_meta.tables.keys())

    def fetch_sample(self, keyspace, table, limit=1000):
        rows = self.session.execute(
            f"SELECT * FROM {keyspace}.{table} LIMIT {int(limit)}"
        )
        return [dict(row._asdict()) for row in rows]

    def fetch_all(self, keyspace, table):
        rows = self.session.execute(f"SELECT * FROM {keyspace}.{table}")
        return [dict(row._asdict()) for row in rows]

    def search_table(self, keyspace, table, column, operator, value):
        rows = self.fetch_all(keyspace, table)
        raw_value = str(value).strip().lower()

        def normalize(v):
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
        for row in rows:
            if column in row and compare(row[column]):
                results.append(row)
        return results
