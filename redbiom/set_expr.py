import ast
import operator


def Expression(body):
    return body


def Name(id, ctx):
    global stemmer
    global target

    try:
        stem = next(stemmer(id))
    except StopIteration:
        raise ValueError("No usable search stem found for: %s" % id)

    return set(ctx('metadata:%s' % target, 'SMEMBERS', stem))


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


def passthrough(s, *args, **kwargs):
    yield s


def seteval(str_, get=None, stemmer=None, target=None):
    """Evaluate a set operation string, where each Name is fetched

    Parameters
    ----------
    str_ : str
        The query to evaluate
    get : function, optional
        A getting method, defaults to instatiating one from _requests
    stemmer : function, optional
        A method to stem a query Name. If None, defaults to passthrough.
    target : str, optional
        A subcontext to query against. If None, defaults to text-search.
    """
    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    if stemmer is None:
        stemmer = passthrough

    if target is None:
        target = 'text-search'

    # Load is subject to indirection to simplify testing
    globals()['Load'] = make_Load(get)

    # this seems right now to be the easiest way to inject parameters
    # into Name
    globals()['stemmer'] = stemmer
    globals()['target'] = target

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
    del stemmer
    del target

    return result
