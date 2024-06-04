from pymongo import MongoClient
from contextlib import suppress
from classes.config import Config

config = Config()

class Database:
    def __init__(self):
        self.client = MongoClient(config.MONGODB_URI)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        with suppress(Exception):
            self.client.close()

    def __del__(self):
        with suppress(Exception):
            self.client.close()

    def get_collection(self, db_name, collection_name):
        db = self.client[db_name]
        collection = db[collection_name]
        return collection
