__all__ = ['LogicalTree', 'query_to_logical_tree']

class LogicalTree:
    def __init__(self, type):
        self.type = type
        self.children = []

    def add_child(self, child):
        self.children.append(child)

def query_to_logical_tree(query: dict):
    tree = LogicalTree('$and')
    fnames = []

    process_dict(tree, fnames, query)

    return tree

def process_dict(tree: LogicalTree, fnames: list, query: dict):
    for key in query.keys():
        val = query[key]
        if key in ('$and', '$or', '$nor', '$not'):
            sub_tree = LogicalTree(key)
            if key != '$not':
                process_list(sub_tree, fnames, val)
            else:
                process_dict(sub_tree, fnames, val)
            tree.add_child(sub_tree)
        elif type(val) is not dict:
            if key.startswith('$'):
                tree.add_child(('.'.join(fnames), key, val))
            else:
                tree.add_child(('.'.join(fnames + [key]), '$eq', val))
        else:
            sub_tree = LogicalTree('$and')
            fnames.append(key)
            process_dict(sub_tree, fnames, val)
            fnames.pop()
            tree.add_child(sub_tree)

def process_list(tree: LogicalTree, fnames: list, query_list: list):
    for query in query_list:
        sub_tree = LogicalTree('$and')
        process_dict(sub_tree, fnames, query)
        tree.add_child(sub_tree)

