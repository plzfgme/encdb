#!/usr/bin/env python
from db_index import *

if __name__ == "__main__":
    client = DBIndexClient('client.db')
    server = DBIndexServer('server.db', client.get_keys())
    token = client.gen_insert_equal_token('add', 'people', b'12345', 'age', (49).to_bytes(1, 'big'))
    server.insert_one_token(token)
    token = client.gen_insert_equal_token('add', 'people', b'56432', 'age', (49).to_bytes(1, 'big'))
    server.insert_one_token(token)

    token = client.gen_search_equal_token('people', 'age', (49).to_bytes(1, 'big'))
    print(server.search_one_token(token))

    tokens = client.gen_insert_range_tokens('add', 'people', b'56432', 'age', 49, 8)
    server.insert_tokens(tokens)
    tokens = client.gen_insert_range_tokens('add', 'people', b'b(Q\xb2C\x05\xe4\x80\xcf*\x8f\x15', 'age', 50, 8)
    server.insert_tokens(tokens)

    tokens = client.gen_search_range_tokens('people', 'age', 45, 53, 8)
    print(server.search_tokens_union(tokens))

    # client.store_keys('keys.pickle')

