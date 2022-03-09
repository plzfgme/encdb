from .utils import *
import os
from itertools import cycle
import rocksdb

class FASTClient:
    def __init__(self, db_path, keys=None):
        if keys is None:
            self.k_s = os.urandom(16)
            self.iv = os.urandom(16)
        else:
            self.set_keys(keys)
        self.db = rocksdb.DB(db_path, rocksdb.Options(create_if_missing=True))

    def get_keys(self):
        return {'k_s': self.k_s, 'iv': self.iv}

    def get_keys_for_server(self):
        return {'iv': self.iv}

    def set_keys(self, keys: dict):
        self.k_s = keys['k_s']
        self.iv = keys['iv']

    def gen_update_tokens(self, ind, w, op):
        t_w = pseudo_function_F(self.k_s, primitive_hash_h(w), self.iv)
        raw_val = self.db.get(w)
        if raw_val is not None:
            st_c = raw_val[:16]
            c = int.from_bytes(raw_val[16:], 'big')
        else:
            st_c = os.urandom(16)
            c = 0
        k_c_add_1 = os.urandom(16)
        st_c_add_1 = pseudo_permutation_P(k_c_add_1, st_c, self.iv)
        self.db.put(w, st_c_add_1 + (c+1).to_bytes(16, 'big'))
        if op == 'add':
            op = (0).to_bytes(1, 'big')
        else:
            op = (1).to_bytes(1, 'big')
        e_part1 = op + k_c_add_1 + ind
        e_part2 = primitive_hash_h_2(t_w + st_c_add_1)
        e = bytes(a ^ b for a, b in zip(e_part1, cycle(e_part2)))
        u = primitive_hash_h_1(t_w + st_c_add_1)

        return u, e

    def gen_search_tokens(self, w):
        t_w = pseudo_function_F(self.k_s, primitive_hash_h(w), self.iv)
        raw_val = self.db.get(w)
        if raw_val is None:
            return None
        st_c = raw_val[:16]
        c = int.from_bytes(raw_val[16:], 'big')
        
        return t_w, st_c, c

class FASTServer:
    def __init__(self, db_path, keys):
        self.set_keys(keys)
        self.db = rocksdb.DB(db_path, rocksdb.Options(create_if_missing=True))

    def get_keys(self):
        return {'iv', self.iv}

    def set_keys(self, keys):
        self.iv = keys['iv']

    def update(self, u, e):
        self.db.put(u, e)

    def search(self, t_w, st_c, c):
        ids = set()
        delta = set()
        st_i = st_c
        for _ in range(c, 0, -1):
            u = primitive_hash_h_1(t_w + st_i)
            e = self.db.get(u)
            e_part2 = primitive_hash_h_2(t_w + st_i)
            e_part1 = bytes(a ^ b for a, b in zip(e, cycle(e_part2)))
            raw_op = int.from_bytes(e_part1[:1], 'big')
            op = 'add' if raw_op == 0 else 'del'
            k_i = e_part1[1:17]
            ind = e_part1[17:]
            if op == 'del':
                delta.add(ind)
            else:
                if ind in delta:
                    delta.remove(ind)
                else:
                    ids.add(ind)
            st_i = pseudo_inverse_permutation_P(k_i, st_i, self.iv)

        return ids

