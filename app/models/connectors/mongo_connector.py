# app/models/connectors/mongo_connector.py
from pymongo import MongoClient


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
                "count": len(self.client[db].list_collection_names())
            }
            for db in self.client.list_database_names()
        ]

    def list_collections(self, db_name):
        db = self.client[db_name]
        return [
            {"name": c, "count": db[c].count_documents({})}
            for c in db.list_collection_names()
        ]

    def list_documents(self, db_name, col_name):
        return list(
            self.client[db_name][col_name]
            .find({}, {"_id": 0})
            .limit(50)
        )
