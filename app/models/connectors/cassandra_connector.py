# app/models/connectors/cassandra_connector.py

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from typing import Optional, List, Dict, Any


class CassandraConnector:
    """
    Connector for Cassandra 3.x (local or remote)
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9042,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.cluster = None
        self.session = None

    # ---------------------------------------------------------------------
    # CONNECT
    # ---------------------------------------------------------------------
    def connect(self):
        """Connect to Cassandra cluster."""

        # If user/password provided, create auth provider
        auth_provider = None
        if self.user and self.password:
            auth_provider = PlainTextAuthProvider(username=self.user, password=self.password)

        # Create the cluster object
        self.cluster = Cluster(
            [self.host],
            port=self.port,
            auth_provider=auth_provider,
            load_balancing_policy=RoundRobinPolicy()
        )

        # Connect to the cluster (default keyspace = None)
        self.session = self.cluster.connect()

        # Test connection: should return one row with current timestamp
        self.session.execute("SELECT now() FROM system.local")

    # ---------------------------------------------------------------------
    # DISCONNECT
    # ---------------------------------------------------------------------
    def disconnect(self):
        """Close session and cluster connections."""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        self.session = None
        self.cluster = None

    # ---------------------------------------------------------------------
    # LIST KEYSPACES
    # ---------------------------------------------------------------------
    def list_keyspaces(self) -> List[str]:
        """Return all keyspaces in the cluster."""
        if not self.session:
            raise Exception("Not connected")
        return list(self.cluster.metadata.keyspaces.keys())

    # ---------------------------------------------------------------------
    # GET TABLES IN KEYSPACE
    # ---------------------------------------------------------------------
    def list_tables(self, keyspace: str) -> List[str]:
        """Return all tables in a given keyspace."""
        if not self.session:
            raise Exception("Not connected")
        ks_meta = self.cluster.metadata.keyspaces.get(keyspace)
        if not ks_meta:
            raise Exception(f"Keyspace '{keyspace}' does not exist")
        return list(ks_meta.tables.keys())

    # ---------------------------------------------------------------------
    # FETCH SAMPLE DATA
    # ---------------------------------------------------------------------
    def fetch_sample(self, keyspace: str, table: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch sample rows from a table."""
        if not self.session:
            raise Exception("Not connected")
        query = f"SELECT * FROM {keyspace}.{table} LIMIT {limit}"
        rows = self.session.execute(query)
        return [dict(row._asdict()) for row in rows]
    
    def fetch_all(self, keyspace, table):
        """
        Fetch ALL rows from a Cassandra table.
        WARNING: For huge tables, this may take long.
        """
        query = f"SELECT * FROM {keyspace}.{table};"
        rows = self.session.execute(query)
        return list(rows)

    # def search_table(self, keyspace, table, column, operator, value):
    #     """
    #     Return rows from `table` in `keyspace` filtered by column/operator/value.
    #     Example: SELECT * FROM keyspace.table WHERE column operator value
    #     """
    #     if not keyspace or not table or not column or not operator or value is None:
    #         return []

    #     query = f"SELECT * FROM {keyspace}.{table} WHERE {column} {operator} %s ALLOW FILTERING"
        
    #     try:
    #         rows = self.session.execute(query, [value])
    #         # Convert to list of lists for the Sheet widget
    #         return [list(row._asdict().values()) for row in rows]
    #     except Exception as e:
    #         print("Cassandra search error:", e)
    #         return []


    
    def get_column_type(self, keyspace, table, column):
        """
        Returns the Cassandra type of a column dynamically.
        """
        try:
            table_meta = self.session.cluster.metadata.keyspaces[keyspace].tables[table]
            col_meta = table_meta.columns[column]
            return col_meta.cql_type  # returns string like 'int', 'text', 'decimal', etc.
        except KeyError:
            return "text"  # fallback if column or table not found

    def search_table(self, keyspace, table, column, operator, value):
        if not keyspace or not table or not column or not operator or value is None:
            return []

        # --- Get column type dynamically ---
        col_type = self.get_column_type(keyspace, table, column)

        # --- Convert value based on column type ---
        try:
            if col_type in ["int", "bigint"]:
                typed_value = int(value)
            elif col_type in ["float", "decimal", "double"]:
                typed_value = float(value)
            else:
                typed_value = value  # string/text
        except ValueError:
            print(f"Value '{value}' is invalid for column type {col_type}")
            return []

        # --- Build query ---
        if operator != "=":
            query = f"SELECT * FROM {keyspace}.{table} ALLOW FILTERING"
            try:
                rows = self.session.execute(query)
                # Python-side filtering
                if operator == "!=":
                    rows = [r for r in rows if getattr(r, column) != typed_value]
                elif operator == ">":
                    rows = [r for r in rows if getattr(r, column) > typed_value]
                elif operator == "<":
                    rows = [r for r in rows if getattr(r, column) < typed_value]
                elif operator == ">=":
                    rows = [r for r in rows if getattr(r, column) >= typed_value]
                elif operator == "<=":
                    rows = [r for r in rows if getattr(r, column) <= typed_value]
            except Exception as e:
                print("Cassandra search error:", e)
                return []
        else:
            # = operator
            query = f"SELECT * FROM {keyspace}.{table} WHERE {column} = %s ALLOW FILTERING"
            try:
                rows = self.session.execute(query, [typed_value])
            except Exception as e:
                print("Cassandra search error:", e)
                return []

        return [list(r._asdict().values()) for r in rows]
