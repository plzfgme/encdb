from db_index import *
from pymongo import MongoClient
from ruamel.yaml import YAML
import rocksdb
import os

class DBClient:
    def __init__(self, config_path, doc_keys_db_path, index_db_path, index_keys_path=None) -> None:
        self.index_client = DBIndexClient(index_db_path, index_keys_path)
        self.mongodb_client = MongoClient('mongo://gytadmin:123456@localhost:27017').test_encdb
        self.doc_keys_db = rocksdb.DB(doc_keys_db_path, rocksdb.Options(create_if_missing=True))
        self.config = YAML(typ='safe').load(open(config_path))

    def insert(self, collection_name, document: dict):
        document.keys
            
        
