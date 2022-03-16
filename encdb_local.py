from db_index import *
from pymongo import MongoClient
from ruamel.yaml import YAML
import rocksdb
import bson
from bson.objectid import ObjectId
from os import urandom
import os
from crypto_utils import *
from query_parse import *

class EncDB_Local:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = YAML(typ='safe').load(f)
        self.client_keys_db = rocksdb.DB(self._client_keys_path(), rocksdb.Options(create_if_missing=True))
        self.index_client = DBIndexClient(self._client_index_path1(), self._client_index_path2(), self._get_or_gen_index_client_key())
        self.index_server = DBIndexServer(self._server_index_path(), self.index_client.get_key_for_server())
        self.document_db = MongoClient(self.config['mongodb']['uri'])[self.config['mongodb']['db_name']]

    def insert_one(self, collection_name: str, document: dict):
        # client
        objid = ObjectId()
        document['_id'] = objid
        doc_to_insert = self._doc_encrypt(collection_name, document)
        tokens = self._gen_index_insert_tokens(objid, collection_name, document)
        enced_cname = self._cname_encrypt(collection_name)
        # server
        self.document_db[enced_cname].insert_one(doc_to_insert)
        self.index_server.update(tokens)

    def delete_one(self, collection_name: str, document: dict):
        # client
        tokens = self._gen_index_delete_tokens(collection_name, document)
        # server
        self.index_server.update(tokens)

    def find(self, collection_name, query: dict, projection=None):
        # client
        tree = query_to_logical_tree(query)
        self._to_tokens_tree(collection_name, tree)
        if projection is not None:
            enced_proj = self._project_encrypt(collection_name, projection)
        else:
            enced_proj = None
        enced_cname = self._cname_encrypt(collection_name)
        # server
        query = self._to_query(tree)
        enced_docs = self._retrieve_docs_by_query(enced_cname, query, enced_proj)
        # client
        docs = []
        for enced_doc in enced_docs: 
            docs.append(self._doc_decrypt(collection_name, enced_doc))

        return docs

    def _to_tokens_tree(self, collection_name: str, tree: LogicalTree):
        for i, child in enumerate(tree.children):
            if type(child) is LogicalTree:
                self._to_tokens_tree(collection_name, child)
            else:
                op = child[1]
                if op == '$eq':
                    tree.children[i] = ('$in', self.index_client.gen_search_equal_tokens(collection_name, child[0], child[2]))
                elif op == '$ne':
                    tree.children[i] = ('$nin', self.index_client.gen_search_equal_tokens(collection_name, child[0], child[2]))
                elif op == '$lte':
                    tree.children[i] = ('$in', self.index_client.gen_search_range_tokens(collection_name, child[0], b=child[2]))
                elif op == '$gte':
                    tree.children[i] = ('$in', self.index_client.gen_search_range_tokens(collection_name, child[0], a=child[2]))

    def _to_query(self, tree: LogicalTree):
        exp_list = []
        for child in tree.children:
            if type(child) is LogicalTree:
                exp_list.append(self._to_query(child))
            else:
                ids = self.index_server.search_tokens_union(child[1])
                objids = []
                for id in ids:
                    objids.append(ObjectId(id))
                exp_list.append({'_id': {child[0]: objids}})
        if tree.type == '$not':
            return {'$not': exp_list[0]}
        else:
            return {tree.type: exp_list}
        
    def _project_encrypt(self, collection_name: str, projection: dict):
        enced_proj = {}
        for key, val in projection.items():
            enced_proj[self._fname_encrypt(collection_name, key)] = val

        return enced_proj

    def _retrieve_docs_by_query(self, collection_name: str, query: dict, projection):
        cursor = self.document_db[collection_name].find(query, projection=projection)
        docs = []
        for doc in cursor:
            docs.append(doc)

        return docs

    def _retrieve_docs(self, collection_name: str, ids):
        objids = []
        for id in ids:
            objids.append(ObjectId(id))
        cursor = self.document_db[self._cname_encrypt(collection_name)].find({'_id': {'$in': objids}})
        docs = []
        for doc in cursor:
            docs.append(doc)

        return docs

    def _doc_encrypt(self, collection_name: str, document: dict):
        ckey, civ = self._get_or_gen_collection_key_iv(collection_name)
        enced_doc = {}
        enced_doc['_id'] = document.pop('_id')
        for field_name, val in document.items():
            enced_fname = bytes2astr(aes_enc(ckey, bytes(field_name, 'utf-8'), civ))
            iv = urandom(16)
            enced_val = {
                'rnd': aes_enc(self._get_or_gen_field_key(collection_name, field_name), bson.encode({'0': val}), iv),
                'iv': iv
            }
            enced_doc[enced_fname] = enced_val

        return enced_doc

    def _doc_decrypt(self, collection_name: str, enced_doc: dict):
        ckey, civ = self._get_collection_key_iv(collection_name)
        doc = {}
        objid = enced_doc.pop('_id')
        for enced_fname, enced_val in enced_doc.items():
            field_name = str(aes_dec(ckey, astr2bytes(enced_fname), civ), 'utf-8')
            val = bson.decode(aes_dec(self._get_field_key(collection_name, field_name), enced_val['rnd'], enced_val['iv']))['0']
            doc[field_name] = val
        doc['_id'] = objid

        return doc

    def _fname_encrypt(self, collection_name: str, field_name: str):
        ckey, civ = self._get_or_gen_collection_key_iv(collection_name)
        return bytes2astr(aes_enc(ckey, bytes(field_name, 'utf-8'), civ))

    def _cname_encrypt(self, collection_name: str):
        ckey, civ = self._get_or_gen_collection_key_iv(collection_name)
        return bytes2astr(aes_enc(ckey, bytes(collection_name, 'utf-8'), civ))

    def _fname_decrypt(self, collection_name: str, enced_name):
        ckey, civ = self._get_or_gen_collection_key_iv(collection_name)
        return str(aes_dec(ckey, astr2bytes(enced_name), civ), 'utf-8')

    def _gen_index_insert_tokens(self, objid: ObjectId, collection_name: str, document: dict):
        id = objid.binary
        tokens = []
        for field in document.keys():
            tokens.extend(self.index_client.gen_update_tokens('add', collection_name, id, field, document[field]))

        return tokens

    def _gen_index_delete_tokens(self, collection_name: str, document: dict):
        id = document['_id'].binary
        del document['_id']
        tokens = []
        for field in document.keys():
            tokens.extend(self.index_client.gen_update_tokens('del', collection_name, id, field, document[field]))

        return tokens

    def _get_or_gen_collection_key_iv(self, collection_name: str):
        collection_key_iv_key = self._collection_key_iv_key(collection_name)
        key_iv = self.client_keys_db.get(collection_key_iv_key) 
        if key_iv is None:
            key = urandom(16) 
            iv = urandom(16)
            self.client_keys_db.put(collection_key_iv_key, key+iv)
        else:
            key = key_iv[:16]
            iv = key_iv[16:]

        return key, iv

    def _get_collection_key_iv(self, collection_name: str):
        key_iv = self.client_keys_db.get(self._collection_key_iv_key(collection_name)) 
        return key_iv[:16], key_iv[16:]

    def _get_or_gen_field_key(self, collection_name: str, field_name: str):
        field_key_key = self._field_key_key(collection_name, field_name)
        key = self.client_keys_db.get(field_key_key)
        if key is None:
            key = urandom(16)
            self.client_keys_db.put(field_key_key, key)

        return key

    def _get_field_key(self, collection_name: str, field_name: str):
        return self.client_keys_db.get(self._field_key_key(collection_name, field_name))

    def _collection_key_iv_key(self, collection_name):
        return b'c:' + bytes(collection_name, 'utf-8') 

    def _field_key_key(self, collection_name, field_name):
        return b'f:' + bytes(collection_name, 'utf-8') + b':' + bytes(field_name, 'utf-8')

    def _get_or_gen_index_client_key(self):
        key = self.client_keys_db.get(b'i')
        if key is None:
            key = urandom(32)
        self.client_keys_db.put(b'i', key)

        return key

    def _client_index_path1(self):
        return os.path.join(self.config['client_storage_path'], 'index1.db')

    def _client_index_path2(self):
        return os.path.join(self.config['client_storage_path'], 'index2.db')

    def _client_keys_path(self):
        return os.path.join(self.config['client_storage_path'], 'keys.db')

    def _server_index_path(self):
        return os.path.join(self.config['server_storage_path'], 'index.db')

