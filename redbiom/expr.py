import ast
import operator


def Expression(body):
    return body


def Name(id, ctx):
    return ctx('metadata', 'GET', id)


def make_Load(get):
    def Load():
        return get
    return Load


def BitAnd():
    return operator.and_


def BitOr():
    return operator.or_


def BitXor():
    return operator.xor


def Sub():
    return operator.sub


def BinOp(left, op, right):
    return op(left, right)


def seteval(str_, get=None):
    """Evaluate a set operation string, where each Name is fetched"""
    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    # Load is subject to indirection to simplify testing
    globals()['Load'] = make_Load(get)

    formed = ast.parse(str_, mode='eval')

    node_types = (ast.BitAnd, ast.BitOr, ast.BitXor, ast.Name, ast.Sub,
                  ast.Expression, ast.BinOp, ast.Load)

    for node in ast.walk(formed):
        if not isinstance(node, node_types):
            raise TypeError("Unsupported node type: %s" % ast.dump(node))

    result = eval(ast.dump(formed))

    # clean up
    global Load
    del Load

    return result
