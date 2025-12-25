# app/models/backend.py
from models.connectors.mongo_connector import MongoConnector
from models.connectors.redis_connector import RedisConnector
from models.connectors.neo4j_connector import Neo4jConnector
from models.connectors.cassandra_connector import CassandraConnector



class NoSQLBackend:
    def __init__(self, db_type, host="localhost", port=None, user=None, password=None, db_index=0):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_index = db_index
        self.client = None

    def connect(self):
        if self.db_type == "MongoDB":
            self.client = MongoConnector(self.host, self.port, self.user, self.password)
        elif self.db_type == "Redis":
            self.client = RedisConnector(self.host, self.port, password=self.password, db=self.db_index)
        elif self.db_type == "Neo4j":
            self.client = Neo4jConnector(self.host, self.port, self.user, self.password)
        elif self.db_type == "Cassandra":
            self.client = CassandraConnector(self.host, self.port, self.user, self.password)
        else:
            raise ValueError("Unsupported database type")

        self.client.connect()

    def disconnect(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass
        self.client = None

    # Proxy methods
    def list_databases(self):
        return self.client.list_databases()

    def list_collections(self, db_name):
        return self.client.list_collections(db_name)

    def list_documents(self, db_name, col_name):
        return self.client.list_documents(db_name, col_name)

    def get_metadata(self):
        return self.client.get_metadata()
    
    # -------------------- Redis-specific proxies --------------------
    def list_keys(self, *args, **kwargs):
        return self.client.list_keys(*args, **kwargs)

    def get_key_value(self, *args, **kwargs):
        return self.client.get_key_value(*args, **kwargs)

    def export_keys_to_csv(self, *args, **kwargs):
        return self.client.export_keys_to_csv(*args, **kwargs)
    
    # -------------------- Cassandra-specific proxies --------------------
    def list_keyspaces(self):
        if self.db_type != "Cassandra":
            raise ValueError("list_keyspaces is only available for Cassandra")
        return self.client.list_keyspaces()

    def list_tables(self, keyspace):
        if self.db_type != "Cassandra":
            raise ValueError("list_tables is only available for Cassandra")
        return self.client.list_tables(keyspace)

    def fetch_sample(self, keyspace, table, limit=1000):
        if self.db_type != "Cassandra":
            raise ValueError("fetch_sample is only available for Cassandra")
        return self.client.fetch_sample(keyspace, table, limit)
    
    def fetch_all(self, keyspace, table):
        if self.db_type != "Cassandra":
            raise ValueError("fetch_all is only available for Cassandra")
        return self.client.fetch_all(keyspace, table)
    
    def search_table(self, keyspace, table, column, operator, value):
        if self.db_type != "Cassandra":
            raise ValueError("search_table is only available for Cassandra")
        return self.client.search_table(keyspace, table, column, operator, value)
    


    