import sys
import ast
import operator
import functools

import pandas as pd


def Expression(body):
    return body


def Name(id, ctx):
    if id in {'None', 'none', None}:
        return None
    return pd.Series(dict(ctx('metadata:category', 'HGETALL', id)), name=id)


def Num(n):
    return float(n)


def Str(s):
    return s


def Tuple(elts, ctx):
    return tuple(elts)


def _cast_retain_numeric(series):
    series = pd.to_numeric(series, errors='coerce')
    series = series[~series.isnull()]
    return series


def _left_and_right(left, right):
    if isinstance(left, pd.Series) and isinstance(right, pd.Series):
        left = _cast_retain_numeric(left)
        right = _cast_retain_numeric(right)

        left, right = left.align(right, join='inner')
        base = pd.concat([left, right], axis=1)
    elif isinstance(left, pd.Series):
        if isinstance(right, float):
            left = _cast_retain_numeric(left)
        base = left
    elif isinstance(right, pd.Series):
        if isinstance(left, float):
            right = _cast_retain_numeric(right)
        base = right
    else:
        raise ValueError("Can only handle pd.Series or numeric types")

    return (base, left, right)


def _compare(op, left, right):
    base, left, right = _left_and_right(left, right)
    return base[op(left, right)]


def Lt():
    return functools.partial(_compare, operator.lt)


def LtE():
    return functools.partial(_compare, operator.le)


def Gt():
    return functools.partial(_compare, operator.gt)


def GtE():
    return functools.partial(_compare, operator.ge)


def Eq():
    return functools.partial(_compare, operator.eq)


Is = Eq


def NotEq():
    return functools.partial(_compare, operator.ne)


IsNot = NotEq


def _in(left, right):
    return left[left.isin(right)]


def _notin(left, right):
    return left[~left.isin(right)]


def Or():
    return operator.or_


def And():
    return operator.and_


def BoolOp(op, values):
    if len(values) != 2:
        raise ValueError("Can only support two comparisons")

    left, right = values
    if op is operator.and_:
        return left.align(right, join='inner')[0]
    elif op is operator.or_:
        return left.align(right, join='outer')[0]
    else:
        raise ValueError("Unknown operator")


def In():
    return _in


def NotIn():
    return _notin


def make_Load(get):
    def Load():
        return get
    return Load


def Compare(left, ops, comparators):
    for op, comp in zip(ops, comparators):
        left = op(left, comp)
    return left


if sys.version_info.major == 3:
    # In Python 3, None is parsed as NameConstant
    # In Python 2, None is parsed as a Name
    def NameConstant(value=None):
        if value in {'None', 'none', None}:
            return None
        else:
            raise TypeError("Unknown NameConstant: %s" % value)


def whereeval(str_, get=None):
    """Evaluate a set operation string, where each Name is fetched"""
    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    # Load is subject to indirection to simplify testing
    globals()['Load'] = make_Load(get)

    formed = ast.parse(str_, mode='eval')

    node_types = [ast.Compare, ast.In, ast.NotIn, ast.BoolOp, ast.And,
                  ast.Name, ast.Or, ast.Eq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
                  ast.NotEq, ast.Str, ast.Num, ast.Load, ast.Expression,
                  ast.Tuple, ast.Is, ast.IsNot]

    if sys.version_info.major == 3:
        node_types.append(ast.NameConstant)

    node_types = tuple(node_types)

    for node in ast.walk(formed):
        if not isinstance(node, node_types):
            raise TypeError("Unsupported node type: %s" % ast.dump(node))

    result = eval(ast.dump(formed))

    # clean up
    global Load
    del Load

    return result
