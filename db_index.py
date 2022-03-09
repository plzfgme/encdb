from fast import *
from bitarray.util import ba2int, int2ba

class DBIndexClient:
    def __init__(self, db_path: str, keys_path=None):
        self.fast_client = FASTClient(db_path, keys_path)

    def store_keys(self, keys_path):
        self.fast_client.store_keys(keys_path)

    def get_iv(self):
        return self.fast_client.get_iv()

    def gen_insert_equal_token(self, op: str, table_name: str, id: bytes, field_name: str, val: bytes):
        return self.fast_client.gen_update_tokens(id, self._equal_keyword(table_name, field_name, val), op)

    def gen_insert_range_tokens(self, op: str, table_name: str, id: bytes, field_name: str, val: int, range_log_2: int):
        val_bin = int2ba(val, length=range_log_2, endian='big')
        tokens_list = []
        for i in range(range_log_2+1):
            tokens_list.append(self.fast_client.gen_update_tokens(id, self._range_keyword(table_name, field_name, val_bin[:i].tobytes(), i), op))

        return tokens_list

    def gen_search_equal_token(self, table_name: str, field_name: str, val: bytes):
        return self.fast_client.gen_search_tokens(self._equal_keyword(table_name, field_name, val))

    def gen_search_range_tokens(self, table_name: str, field_name: str, a: int, b: int, range_log_2: int):
        cset = set()
        i = 0
        a_bin = int2ba(a, range_log_2, 'big')
        b_bin = int2ba(b, range_log_2, 'big')
        while len(a_bin) != 0 and ba2int(a_bin) < ba2int(b_bin):
            if a_bin[-1] == 1:
                cset.add((a_bin.tobytes(), range_log_2-i))
            if b_bin[-1] == 0:
                cset.add((b_bin.tobytes(), range_log_2-i))
            a_bin = int2ba(ba2int(a_bin) + 1, range_log_2-i, 'big')
            b_bin = int2ba(ba2int(b_bin) - 1, range_log_2-i, 'big')

            a_bin = a_bin[:-1] 
            b_bin = b_bin[:-1]
            i += 1
        if a_bin == b_bin:
            cset.add((a_bin.tobytes(), range_log_2-i))
        tokens_list = []
        for (val, bit_len) in cset:
            tokens = self.fast_client.gen_search_tokens(self._range_keyword(table_name, field_name, val, bit_len))
            if tokens is not None:
                tokens_list.append(self.fast_client.gen_search_tokens(self._range_keyword(table_name, field_name, val, bit_len)))
        
        return tokens_list

    def _equal_keyword(self, table_name: str, field_name: str, val: bytes):
        return bytes(table_name, 'utf-8')+b':e:'+bytes(field_name, 'utf-8')+b':'+val

    def _range_keyword(self, table_name: str, field_name: str, val: bytes, bit_len: int):
        return bytes(table_name, 'utf-8')+b':r:'+bytes(field_name, 'utf-8')+b':'+bytes(str(bit_len), 'utf-8')+b':'+val

class DBIndexServer:
    def __init__(self, db_path, iv):
        self.fast_server = FASTServer(db_path, iv)

    def insert_one_token(self, token):
        self.fast_server.update(*token)

    def insert_tokens(self, token_list):
        for token in token_list:
            self.fast_server.update(*token)

    def search_one_token(self, token):
        return self.fast_server.search(*token)

    def search_tokens_union(self, token_list):
        rset = set()
        for token in token_list:
            rset = rset.union(self.fast_server.search(*token))
        
        return rset
