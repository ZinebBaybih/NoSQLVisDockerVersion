from typing import List, Optional
import time

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from config import PREVIEW_LIMIT

CASSANDRA_SYSTEM_KEYSPACES = {
    "system",
    "system_schema",
    "system_auth",
    "system_traces",
    "system_distributed",
    "system_virtual_schema",
    "system_views",
}


def is_system_keyspace(keyspace: str) -> bool:
    return keyspace in CASSANDRA_SYSTEM_KEYSPACES or keyspace.startswith("system_")


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
        return [
            keyspace
            for keyspace in self.cluster.metadata.keyspaces.keys()
            if not is_system_keyspace(keyspace)
        ]

    def list_tables(self, keyspace: str) -> List[str]:
        if not self.cluster:
            return []
        ks_meta = self.cluster.metadata.keyspaces.get(keyspace)
        if not ks_meta:
            return []
        return list(ks_meta.tables.keys())

    def fetch_sample(self, keyspace, table, limit=PREVIEW_LIMIT):
        rows = self.session.execute(
            f"SELECT * FROM {keyspace}.{table} LIMIT {int(limit)}"
        )
        return [dict(row._asdict()) for row in rows]

    def count_rows(self, keyspace: str, table: str) -> int:
        try:
            row = self.session.execute(
                f"SELECT COUNT(*) FROM {keyspace}.{table}"
            ).one()
            if row is None:
                return 0
            return int(getattr(row, "count", row[0]))
        except Exception:
            return -1

    def fetch_all(self, keyspace, table):
        rows = self.session.execute(f"SELECT * FROM {keyspace}.{table}")
        return [dict(row._asdict()) for row in rows]

    def get_queryable_filter_columns(self, keyspace, table):
        table_meta = self._get_table_metadata(keyspace, table)
        if not table_meta:
            return []

        columns = []
        partition_key = getattr(table_meta, "partition_key", [])
        if len(partition_key) == 1:
            columns.append(partition_key[0].name)

        for index in getattr(table_meta, "indexes", {}).values():
            target = getattr(index, "index_options", {}).get("target", "")
            target = target.strip().strip('"')
            if target and "(" not in target and target not in columns:
                columns.append(target)

        return columns

    def search_indexed_table(self, keyspace, table, column, value, limit=PREVIEW_LIMIT):
        queryable_columns = self.get_queryable_filter_columns(keyspace, table)
        if column not in queryable_columns:
            return [], queryable_columns

        typed_value = self._convert_column_value(keyspace, table, column, value)
        rows = self.session.execute(
            "SELECT * FROM {}.{} WHERE {} = %s LIMIT {}".format(
                self._quote_identifier(keyspace),
                self._quote_identifier(table),
                self._quote_identifier(column),
                int(limit),
            ),
            (typed_value,),
        )
        return [dict(row._asdict()) for row in rows], queryable_columns

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

    def _get_table_metadata(self, keyspace, table):
        if not self.cluster:
            return None
        keyspace_meta = self.cluster.metadata.keyspaces.get(keyspace)
        if not keyspace_meta:
            return None
        return keyspace_meta.tables.get(table)

    def _convert_column_value(self, keyspace, table, column, value):
        table_meta = self._get_table_metadata(keyspace, table)
        column_meta = None
        if table_meta:
            column_meta = table_meta.columns.get(column)
        column_type = str(getattr(column_meta, "cql_type", "") or getattr(column_meta, "typestring", "")).lower()
        raw_value = str(value).strip()

        if column_type in ("int", "smallint", "tinyint"):
            return int(raw_value)
        if column_type == "bigint":
            return int(raw_value)
        if column_type in ("float", "double", "decimal"):
            return float(raw_value)
        if column_type == "boolean":
            return raw_value.lower() == "true"
        return raw_value

    def _quote_identifier(self, identifier):
        return '"{}"'.format(str(identifier).replace('"', '""'))
