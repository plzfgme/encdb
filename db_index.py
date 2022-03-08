from .fast import *
from bitarray import bitarray

class DBIndexClient:
    def __init__(self, db_path: str, keys_path: str):
        self.fast_client = FASTClient(db_path, keys_path)

    def gen_insert_equal_tokens(self, op: str, table_name: str, id: bytes, field_name: str, val: bytes):
        return self.fast_client.gen_update_tokens(id, self._equal_keyword(table_name, field_name, val), op)

    def gen_insert_range_tokens(self, op: str, table_name: str, id: bytes, field_name: str, val: int, range_log_2: int):
        val_bin = bitarray()
        val_bin.frombytes(val.to_bytes(range_log_2 // 8, 'big'))
        tokens_list = []
        for i in range(len(val_bin)):
            tokens_list.append(self.fast_client.gen_update_tokens(id, self._range_keyword(table_name, field_name, val_bin[:i].tobytes()), op))

        return tokens_list

    def gen_search_equal_tokens(self, table_name: str, field_name: str, val: bytes):
        return self.fast_client.gen_search_tokens(bytes(table_name, 'utf-8')+b':e:'+bytes(field_name, 'utf-8')+b':'+val)

    def gen_search_range_tokens(self, table_name: str, field_name: str, lower_bound: int, upper_bound: int):
        cset = set()
        rset = set()
        i = 0

        pass 

    def _equal_keyword(self, table_name: str, field_name: str, val: bytes):
        return bytes(table_name, 'utf-8')+b':e:'+bytes(field_name, 'utf-8')+b':'+val

    def _range_keyword(self, table_name: str, field_name: str, val: bytes):
        return bytes(table_name, 'utf-8')+b':r:'+bytes(field_name, 'utf-8')+b':'+val

class DBIndexServer:
    def __init__(self, db_path, client_init_msg):
        self.fast_server = FASTServer(db_path, client_init_msg)

    def insert_equal(self, tokens):
        self.fast_server.update(*tokens)

    def insert_range(self, tokens):
        for token_pair in tokens:
            self.fast_server.update(*token_pair)

    def search_equal(self, tokens):
        return self.fast_server.search(*tokens)

