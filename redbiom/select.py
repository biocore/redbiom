def samples(sample_values, criteria):
    """Select samples based on specified criteria

    Parameters
    ----------
    sample_values : pandas.Series
        A series indexed by the Sample ID and valued by something.
    criteria : str
        Selection criteria. Simple logic can be specified, but cannot be
        chained. The following operators are possible available:

            {<, >, in, notin}

        For example, to keep samples with a value less than 5, the following
        form works: "< 5". To keep samples matching a discrete set of possible
        states, use the "in" operator and denote the valid states with a comma.
        Quotes are possible as well, for instance, "in foo,'bar baz" will keep
        samples whose value are either "foo" or "bar baz".

        If no operator is specified, it is assumed an exact string match of the
        value is to be performed.

    Returns
    -------
    generator of str
        Yields the sample IDs which meet the criteria.

    Raises
    ------
    ValueError
        If the criteria cannot be parsed.
        If the > or < operator is used, and the right hand side of the
            criteria do not appear to be numeric.
    """
    import shlex
    from redbiom.util import float_or_nan
    tokens = list(shlex.shlex(criteria))
    if len(tokens) > 1:
        # < 5
        # in "foo bar",blah
        # notin thing,"other thing"

        op = {'in': lambda a, b: a in b,
              'notin': lambda a, b: a not in b,
              '<': lambda a, b: a <= b,
              '>': lambda a, b: a >= b}
        operator = op.get(tokens[0])
        if operator is None:
            func = lambda to_test: to_test == criteria
        elif tokens[0] in ('in', 'notin'):
            rh = [t.strip("'").strip('"') for t in tokens[1:] if t != ',']
            tokens = set(rh)
            func = lambda to_test: operator(to_test, rh)
        else:
            rh = tokens[1]
            if len(tokens) > 2:
                raise ValueError("Unexpected criteria: %s" % criteria)
            try:
                rh = float(rh)
            except TypeError:
                if operator in {'<=', '>='}:
                    raise ValueError("Right hand does not look numeric")

            func = lambda to_test: operator(float_or_nan(to_test), rh)
    else:
        func = lambda to_test: to_test == criteria

    for s, v in zip(sample_values.index, sample_values.values):
        if func(v):
            yield s
