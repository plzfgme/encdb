from fast import *
import rocksdb
from sortedcontainers import SortedDict
from lru import LRU
import bson

class DBIndexClient:
    def __init__(self, sse_db_path: str, index_db_path: str, key=None):
        if key != None:
            k_s_iv = {
                'k_s': key[:16],
                'iv': key[16:]
            }
        else:
            k_s_iv = None
        self.fast_client = FASTClient(sse_db_path, k_s_iv)
        self.tree_db = rocksdb.DB(index_db_path, rocksdb.Options(create_if_missing=True)) 
        self.tree_cache = LRU(20)

    def get_key(self) -> bytes:
        k_s_iv = self.fast_client.get_keys()
        return k_s_iv['k_s'] + k_s_iv['iv']

    def get_key_for_server(self) -> bytes:
        return self.fast_client.get_keys_for_server()['iv']

    def set_key(self, key: bytes):
        k_s_iv = {
            'k_s': key[:16],
            'iv': key[16:]
        }
        self.fast_client.set_keys(k_s_iv)

    def gen_update_tokens(self, op: str, collection_name: str, id: bytes, field_name: str, val):
        t = type(val)
        if t is int or t is float:
            tree = self._get_tree(collection_name, field_name)
            if op == 'add':
                self._tree_insert(tree, val)
            else:
                self._tree_delete(tree, val)
            self._set_tree(collection_name, field_name, tree)
        return [self.fast_client.gen_update_tokens(id, self._keyword(collection_name, field_name, self._val_encode(val)), op)]

    def gen_search_equal_tokens(self, collection_name: str, field_name: str, val):
        return [self.fast_client.gen_search_tokens(self._keyword(collection_name, field_name, self._val_encode(val)))]

    def gen_search_range_tokens(self, collection_name: str, field_name: str, a, b):
        tree = self._get_tree(collection_name, field_name)
        tokens = []
        for val in tree.irange(a, b):
            tokens.extend(self.gen_search_equal_tokens(collection_name, field_name, val))

        return tokens

    def _val_encode(self, val):
        return bson.encode({'0': val})

    def _val_decode(self, b):
        return bson.decode(b)['0']

    def _get_tree(self, collection_name: str, field_name: str):
        key = collection_name + ':' + field_name
        if self.tree_cache.has_key(key):
            return self.tree_cache[key]
        raw_tree = self.tree_db.get(bytes(key, 'utf-8'))
        if raw_tree is None:
            return SortedDict()
        tree = SortedDict(bson.decode(raw_tree)['0'])
        self.tree_cache[key] = tree
        return tree

    def _tree_insert(self, tree: SortedDict, val):
        if val in tree.keys():
            tree[val] += 1
        else:
            tree[val] = 1

    def _tree_delete(self, tree: SortedDict, val):
        if val not in tree.keys():
            return
        tree[val] -= 1
        if tree[val] < 1:
            del tree[val]
            
    def _set_tree(self, collection_name: str, field_name: str, tree: SortedDict):
        key = collection_name + ':' +field_name
        if self.tree_cache.has_key(key):
            self.tree_cache[key] = tree
        self.tree_db.put(bytes(key, 'utf-8'), bson.encode({'0': list(tree.items())}))

    def _keyword(self, collection_name: str, field_name: str, val: bytes):
        return bytes(collection_name, 'utf-8')+b':'+bytes(field_name, 'utf-8')+b':'+val

class DBIndexServer:
    def __init__(self, db_path, key):
        self.fast_server = FASTServer(db_path, {'iv': key})

    def get_key(self) -> bytes:
        return self.fast_server.get_keys()['iv']

    def set_key(self, keys: bytes):
        self.fast_server.set_keys({'iv': keys})

    def update_one_token(self, token):
        self.fast_server.update(*token)

    def update(self, token_list):
        for token in token_list:
            self.fast_server.update(*token)

    def search_one_token(self, token):
        return self.fast_server.search(*token)

    def search_tokens_union(self, token_list):
        rset = set()
        for token in token_list:
            rset = rset.union(self.fast_server.search(*token))
        
        return rset
