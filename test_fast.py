#!/usr/bin/env python
from fast import *

if __name__ == "__main__":
    client = FASTClient('client.db')
    server = FASTServer('server.db', client.get_keys())
    u, e = client.gen_update_tokens(b'123456', b'gyt', 'add')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'123456', b'gyt', 'del')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'123456', b'gyt', 'add')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'1234567', b'gyt', 'add')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'1234567', b'gyt', 'del')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'1234567', b'gyt2er45672sdcxfdxs333e', 'add')
    server.update(u, e)
    result = client.gen_search_tokens(b'gyt2er45672sdcxfdxs333e')
    if result is not None:
        t_w, st_c, c = result
        print(server.search(t_w, st_c, c))

