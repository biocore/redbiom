def _parse_validate_request(req, command):
    import requests
    if req.status_code != 200:
        raise requests.HTTPError("%s : %s" % (command, req.content))
    return req.json()[command]


def _format_request(context, command, other):
    if context is None:
        return "%s/%s" % (command, other)
    else:
        return "%s/%s:%s" % (command, context, other)


def make_post(config):
    import requests
    import redbiom
    s = requests.Session()
    redbiom.active_sessions.append(s)
    s.auth = config['auth']

    def f(context, cmd, payload):
        req = s.post(config['hostname'],
                     data=_format_request(context, cmd, payload))
        return _parse_validate_request(req, cmd)
    return f


def make_put(config):
    import requests
    import redbiom
    s = requests.Session()
    redbiom.active_sessions.append(s)
    s.auth = config['auth']

    def f(context, cmd, key, data):
        url = '/'.join([config['hostname'],
                        _format_request(context, cmd, key)])
        req = s.put(url, data=data)
        return _parse_validate_request(req, cmd)
    return f


def make_get(config):
    import requests
    import redbiom
    s = requests.Session()
    redbiom.active_sessions.append(s)
    s.auth = config['auth']

    def f(context, cmd, data):
        payload = _format_request(context, cmd, data)
        url = '/'.join([config['hostname'], payload])
        return _parse_validate_request(s.get(url), cmd)
    return f


def buffered(it, prefix, cmd, context, get=None, buffer_size=10,
             multikey=None):
    """Bulk fetch data

    Parameters
    ----------
    it : iterable
        The items to query for
    prefix : string
        A key prefix such as "data"
    cmd : string, a Redis command
        The command to request be executed
    context : string
        The context to operate under (ie another prefix).
    get : function, optional
        An existing get function
    buffer_size: int, optional
        The number of items to query for at once. It is important to avoid
        having a buffer size which may result in a URL exceeding 100kb as in
        testing, that was not well support unsurprisingly.
    multikey: string, optional
        For hashbucket commands, like HMGET, where there is an outer and inner
        key.
    """
    if get is None:
        import redbiom
        import redbiom.requests
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

    if multikey is None:
        prefixer = lambda a, b, c: '%s:%s:%s' % (a, b, c)
    else:
        prefixer = lambda a, b, c: c

    exhausted = False
    while not exhausted:
        items = []
        for i in range(buffer_size):
            try:
                items.append(next(it).strip())
            except StopIteration:
                exhausted = True
                break
        bulk = '/'.join([prefixer(context, prefix, i) for i in items])
        if multikey:
            bulk = "%s:%s/%s" % (context, multikey, bulk)
        yield items, get(None, cmd, bulk)


def valid(context, get=None):
    if get is None:
        import redbiom.requests
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

    if not get('state', 'HEXISTS', 'contexts/%s' % context):
        raise ValueError("Unknown context: %s" % context)
