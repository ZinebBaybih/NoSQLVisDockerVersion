# app/models/connectors/mongo_connector.py
from pymongo import MongoClient

from config import PREVIEW_LIMIT

MONGODB_SYSTEM_DATABASES = {"admin", "config", "local"}
MONGODB_SYSTEM_COLLECTION_PREFIXES = ("system.",)


class MongoConnector:
    def __init__(self, host="localhost", port=27017, user=None, password=None):
        if user and password:
            self.uri = f"mongodb://{user}:{password}@{host}:{port}"
        else:
            self.uri = f"mongodb://{host}:{port}"

        self.client = None

    def connect(self):
        self.client = MongoClient(self.uri)

    def disconnect(self):
        if self.client:
            self.client.close()

    def list_databases(self):
        return [
            {
                "name": db,
                "count": len(self.list_collection_names(db))
            }
            for db in self.client.list_database_names()
            if db not in MONGODB_SYSTEM_DATABASES
        ]

    def list_collection_names(self, db_name):
        return [
            collection
            for collection in self.client[db_name].list_collection_names()
            if not collection.startswith(MONGODB_SYSTEM_COLLECTION_PREFIXES)
        ]

    def list_collections(self, db_name):
        db = self.client[db_name]
        return [
            {"name": c, "count": db[c].count_documents({})}
            for c in self.list_collection_names(db_name)
        ]

    def list_documents(self, db_name, col_name, offset=0, limit=PREVIEW_LIMIT):
        return list(
            self.client[db_name][col_name]
            .find({}, {"_id": 0})
            .skip(int(offset))
            .limit(int(limit))
        )

    def get_indexed_search_fields(self, db_name, col_name, max_depth=5):
        fields = []
        indexes = self.client[db_name][col_name].index_information()

        for index in indexes.values():
            keys = index.get("key", [])
            if not keys:
                continue

            field, direction = keys[0]
            if field == "_id" or len(field.split(".")) > int(max_depth):
                continue
            if direction not in (1, -1, "hashed"):
                continue
            if field not in fields:
                fields.append(field)

        return fields

    def search_indexed_documents(self, db_name, col_name, value, offset=0, limit=PREVIEW_LIMIT):
        fields = self.get_indexed_search_fields(db_name, col_name)
        if not fields:
            return [], 0, []

        values = self._search_value_variants(value)
        query = {
            "$or": [
                {field: variant}
                for field in fields
                for variant in values
            ]
        }

        collection = self.client[db_name][col_name]
        total = collection.count_documents(query)
        docs = list(
            collection
            .find(query, {"_id": 0})
            .skip(int(offset))
            .limit(int(limit))
        )
        return docs, total, fields

    def _search_value_variants(self, value):
        raw_value = str(value).strip()
        values = [raw_value]
        lowered = raw_value.lower()

        if lowered in ("true", "false"):
            values.append(lowered == "true")

        try:
            int_value = int(raw_value)
            values.append(int_value)
        except ValueError:
            pass

        try:
            float_value = float(raw_value)
            values.append(float_value)
        except ValueError:
            pass

        unique_values = []
        for item in values:
            if item not in unique_values:
                unique_values.append(item)
        return unique_values
