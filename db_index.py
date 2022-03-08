from .fast import *
from bitarray import bitarray
from bitarray.util import ba2int, int2ba

class DBIndexClient:
    def __init__(self, db_path: str, keys_path: str):
        self.fast_client = FASTClient(db_path, keys_path)

    def gen_insert_equal_tokens(self, op: str, table_name: str, id: bytes, field_name: str, val: bytes):
        return self.fast_client.gen_update_tokens(id, self._equal_keyword(table_name, field_name, val), op)

    def gen_insert_range_tokens(self, op: str, table_name: str, id: bytes, field_name: str, val: int, range_log_2: int):
        val_bin = int2ba(val, length=range_log_2, endian='big')
        tokens_list = []
        for i in range(len(val_bin)):
            tokens_list.append(self.fast_client.gen_update_tokens(id, self._range_keyword(table_name, field_name, val_bin[:i].tobytes()), op))

        return tokens_list

    def gen_search_equal_tokens(self, table_name: str, field_name: str, val: bytes):
        return self.fast_client.gen_search_tokens(self._equal_keyword(table_name, field_name, val))

    def gen_search_range_tokens(self, table_name: str, field_name: str, a: int, b: int, range_log_2: int):
        cset = set()
        i = 0
        a_bin = int2ba(a, range_log_2, 'big')
        b_bin = int2ba(b, range_log_2, 'big')
        while len(a_bin) != 0 and ba2int(a_bin) < ba2int(b_bin):
            if a_bin[-1] == 1:
                cset.add(a_bin)
            if b_bin[-1] == 0:
                cset.add(b_bin)
            a_bin = int2ba(ba2int(a_bin) + 1, range_log_2, 'big')
            b_bin = int2ba(ba2int(b_bin) - 1, range_log_2, 'big')

            a_bin >>= 1
            b_bin >>= 1
            i += 1
        if a_bin == b_bin:
            cset.add(a_bin)
        tokens_list = []
        for val_bin in cset:
            tokens_list.append(self.fast_client.gen_search_tokens(self._range_keyword(table_name, field_name, val_bin.tobytes())))
        
        return tokens_list

    def _equal_keyword(self, table_name: str, field_name: str, val: bytes):
        return bytes(table_name, 'utf-8')+b':e:'+bytes(field_name, 'utf-8')+b':'+val

    def _range_keyword(self, table_name: str, field_name: str, val: bytes):
        return bytes(table_name, 'utf-8')+b':r:'+bytes(field_name, 'utf-8')+b':'+val

class DBIndexServer:
    def __init__(self, db_path, client_init_msg):
        self.fast_server = FASTServer(db_path, client_init_msg)

    def insert_equal(self, tokens):
        self.fast_server.update(*tokens)

    def insert_range(self, tokens_list):
        for tokens in tokens_list:
            self.fast_server.update(*tokens)

    def search_equal(self, tokens):
        return self.fast_server.search(*tokens)

    def search_range(self, tokens_list):
        rset = set()
        for tokens in tokens_list:
            rset.union(self.fast_server.search(*tokens))
        
        return rset
