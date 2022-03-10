from db_index import *
from pymongo import MongoClient
from ruamel.yaml import YAML
import rocksdb
import bson
from bson.objectid import ObjectId
from os import urandom
from crypto_utils import dec_doc, enc_doc

class DBClient:
    def __init__(self, config_path) -> None:
        with open(config_path) as f:
            self.config = YAML(typ='safe').load(f)
        self.index_client = DBIndexClient(self.config['index_db_path'])
        self.keys_storage = rocksdb.DB(self.config['keys_db_path'], rocksdb.Options(create_if_missing=True))
        self.doc_db = MongoClient(self.config['mongodb']['uri'])[self.config['mongodb']['db_name']]


    def insert_doc(self, collection_name, document: dict):
        key_iv = self.keys_storage.get(self._collection_keys_key(collection_name)) 
        if key_iv is None:
            key = urandom(16) 
            iv = urandom(16)
            self.keys_storage.put(self._collection_keys_key(collection_name), key+iv)
        else:
            key = key_iv[:16]
            iv = key_iv[16:]
        obj_id = self.doc_db[collection_name].insert_one({'binary': enc_doc(key, bson.encode(document), iv)}).inserted_id
        if obj_id is None:
            return None
        return obj_id.binary

    def gen_update_index_tokens(self, id, collection_name, document: dict):
        collection_config = self.config.get('collections')
        if collection_config == None:
            return []
        field_configs = collection_config.get(collection_name)
        if field_configs is not None:
            tokens = []
            for field in document.keys():
                field_config = field_configs.get(field)
                if field_config != None:
                    for func in field_config.keys():
                        if func == 'equal':
                            tokens.append(self.index_client.gen_update_equal_token('add', collection_name, id, field, self._to_bytes(document[field])))
                        if func == 'range':
                            tokens.extend(self.index_client.gen_update_range_tokens('add', collection_name, id, field, document[field], field_config['range']['range_log_2']))
            
            return tokens

        return []

    def insert_doc_and_gen_index_tokens(self, collection_name, document: dict):
        id = self.insert_doc(collection_name, document)
        if id is None:
            return None
        return self.gen_update_index_tokens(id, collection_name, document)

    def gen_equal_search_index_tokens(self, collection_name, field_name, val):
        return self.index_client.gen_search_equal_token(collection_name, field_name, self._to_bytes(val))

    def gen_range_search_index_tokens(self, collection_name, field_name, a, b):
        range_log_2 = self.config['collections'][collection_name][field_name]['range']['range_log_2']
        return self.index_client.gen_search_range_tokens(collection_name, field_name, a, b, range_log_2)

    def retrieve_docs(self, collection_name, ids):
        objids = []
        for id in ids:
            objids.append(ObjectId(id))
        cursor = self.doc_db[collection_name].find({'_id': {'$in': objids}})
        docs = []
        key_iv = self.keys_storage.get(self._collection_keys_key(collection_name)) 
        key = key_iv[:16]
        iv = key_iv[16:]
        for encrypted_doc in cursor:
            docs.append(bson.decode(dec_doc(key, encrypted_doc['binary'], iv)))

        return docs

                            
    def _to_bytes(self, val):
        t = type(val)
        if t is str:
            return bytes(val, 'utf-8')
        elif t is int:
            if val > 0xffff:
                return val.to_bytes(8, 'big')
            else:
                return val.to_bytes(4, 'big')
        elif t is bytes:
            return val
        else:
            return bytes(val)

    def _collection_keys_key(self, collection_name):
        return b'ck:' + bytes(collection_name, 'utf-8')            
        
