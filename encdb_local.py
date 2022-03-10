from db_index import *
from pymongo import MongoClient
from ruamel.yaml import YAML
import rocksdb
import bson
from bson.objectid import ObjectId
from os import urandom
import os
from crypto_utils import *

class EncDB_Local:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = YAML(typ='safe').load(f)
        self.client_keys_db = rocksdb.DB(self._client_keys_path(), rocksdb.Options(create_if_missing=True))
        self.index_client = DBIndexClient(self._client_index_path(), self._get_or_gen_index_client_key())
        self.index_server = DBIndexServer(self._server_index_path(), self.index_client.get_key_for_server())
        self.document_db = MongoClient(self.config['mongodb']['uri'])[self.config['mongodb']['db_name']]

    def insert_one(self, collection_name: str, document: dict):
        # client
        objid = ObjectId()
        key, iv = self._get_or_gen_doc_key_iv(collection_name)
        enced_doc = aes_enc(key, bson.encode(document), iv)
        doc_to_insert = {
            '_id': objid,
            'binary': enced_doc,
        }
        tokens = self._gen_index_insert_tokens(objid, collection_name, document)
        # server
        self.document_db[collection_name].insert_one(doc_to_insert)
        self.index_server.update(tokens)

    def search_equal(self, collection_name, field_name, val):
        # client
        tokens = self.index_client.gen_search_equal_tokens(collection_name, field_name, self._val_to_bytes(val))
        # server
        ids = self.index_server.search_tokens_union(tokens)
        enced_docs = self._retrieve_docs(collection_name, ids)
        # client
        key, iv = self._get_doc_key_iv(collection_name)
        docs = []
        for enced_doc in enced_docs:
            docs.append(bson.decode(aes_dec(key, enced_doc['binary'], iv)))

        return docs

    def search_range(self, collection_name, field_name, a, b):
        # client
        range_log_2 = self.config['collections'][collection_name][field_name]['func:range']['range_log_2']
        tokens = self.index_client.gen_search_range_tokens(collection_name, field_name, a, b, range_log_2)
        # server
        ids = self.index_server.search_tokens_union(tokens)
        enced_docs = self._retrieve_docs(collection_name, ids)
        # client
        key, iv = self._get_doc_key_iv(collection_name)
        docs = []
        for enced_doc in enced_docs: 
            docs.append(bson.decode(aes_dec(key, enced_doc['binary'], iv)))

        return docs
        
    def _retrieve_docs(self, collection_name: str, ids):
        objids = []
        for id in ids:
            objids.append(ObjectId(id))
        cursor = self.document_db[collection_name].find({'_id': {'$in': objids}})
        docs = []
        for doc in cursor:
            docs.append(doc)

        return docs


    def _gen_index_insert_tokens(self, objid: ObjectId, collection_name: str, document: dict):
        id = objid.binary
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
                        if func == 'func:equal':
                            tokens.extend(self.index_client.gen_update_equal_tokens('add', collection_name, id, field, self._val_to_bytes(document[field])))
                        if func == 'func:range':
                            tokens.extend(self.index_client.gen_update_range_tokens('add', collection_name, id, field, document[field], field_config['func:range']['range_log_2']))
            return tokens

        return []

    def _val_to_bytes(self, val):
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

    def _get_or_gen_doc_key_iv(self, collection_name: str):
        key_iv = self.client_keys_db.get(self._collection_key_iv_key(collection_name)) 
        if key_iv is None:
            key = urandom(16) 
            iv = urandom(16)
            self.client_keys_db.put(self._collection_key_iv_key(collection_name), key+iv)
        else:
            key = key_iv[:16]
            iv = key_iv[16:]

        return key, iv

    def _get_doc_key_iv(self, collection_name: str):
        key_iv = self.client_keys_db.get(self._collection_key_iv_key(collection_name)) 
        return key_iv[:16], key_iv[16:]

    def _collection_key_iv_key(self, collection_name):
        return b'ck:' + bytes(collection_name, 'utf-8') 

    def _get_or_gen_index_client_key(self):
        key = self.client_keys_db.get(b'ik')
        if key is None:
            key = urandom(32)
        self.client_keys_db.put(b'ik', key)

        return key

    def _client_index_path(self):
        return os.path.join(self.config['client_storage_path'], 'index.db')

    def _client_keys_path(self):
        return os.path.join(self.config['client_storage_path'], 'keys.db')

    def _server_index_path(self):
        return os.path.join(self.config['server_storage_path'], 'index.db')

