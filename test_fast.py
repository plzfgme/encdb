from fast import *

if __name__ == "__main__":
    client = FASTClient('client.db', 'client_keys.pickle')
    server = FASTServer('server.db', client.get_iv())
    u, e = client.gen_update_tokens(b'123456', 'gyt', 'add')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'123456', 'gyt', 'del')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'123456', 'gyt', 'add')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'1234567', 'gyt', 'add')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'1234567', 'gyt', 'del')
    server.update(u, e)
    u, e = client.gen_update_tokens(b'1234567', 'gyt2er45672sdcxfdxs333e', 'add')
    server.update(u, e)
    result = client.gen_search_tokens('gyt2er45672sdcxfdxs333e')
    if result is not None:
        t_w, st_c, c = result
        print(server.search(t_w, st_c, c))
    # client.store_keys('client_keys.pickle')

