def metadata_full(query, categories=False, get=None):
    """Find samples or categories

    Parameters
    ----------
    query : str
        The query to execute
    categories : boolean, optional
        Whether to search for categories (True) or samples (False, default).
    get : function
        A getter

    Raises
    ------
    TypeError
        When unexpected operators are used
    ValueError
        When a where query is used with a categories search

    Returns
    -------
    set
        The observed sample IDs
    """
    import redbiom
    import redbiom.set_expr
    import redbiom.where_expr
    import redbiom._requests
    import redbiom.util
    import functools
    import nltk

    if get is None:
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    if categories:
        target = 'category-search'
    else:
        target = 'text-search'

    stemmer = nltk.PorterStemmer(nltk.PorterStemmer.MARTIN_EXTENSIONS)
    stops = frozenset(nltk.corpus.stopwords.words('english'))
    stem_f = functools.partial(redbiom.util.stems, stops, stemmer)

    samples = set()
    for plan_type, q in query_plan(query):
        if plan_type == 'set':
            samples.update(redbiom.set_expr.seteval(q, get=get,
                                                    target=target,
                                                    stemmer=stem_f))
        elif plan_type == 'where':
            if categories:
                raise ValueError("where clauses not allowed with a category "
                                 "search")
            obs = set(redbiom.where_expr.whereeval(q, get=get).index)
            if samples:
                samples &= obs
            else:
                samples = obs

    return samples


def query_plan(query):
    """Light sanity checking and query partitioning

    Parameters
    ----------
    query : str
        The query to operate on

    Returns
    -------
    list of tuple
        The (query type, query).

    Raises
    ------
    ValueError
       When there are no queries
    """
    if query.startswith('where'):
        part = query.split('where', 1)[1].strip()

        if not part:
            raise ValueError('No query')

        return [('where', part)]

    parts = query.split('where', 1)
    for part in parts:
        if not part:
            raise ValueError('No query')

    if len(parts) == 1:
        return [('set', parts[0].strip())]
    else:
        return [('set', parts[0].strip()), ('where', parts[1].strip())]
