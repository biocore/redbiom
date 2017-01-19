def from_or_nargs(from_, nargs_variable):
    """In support of buffered: determine whether to use from_ or nargs"""
    import sys
    if from_ is None and not nargs_variable:
        click.echo('Need at least 1 item', err=True)
        sys.exit(1)  # should be doable from click but need ctx i think...?

    if from_ is not None and nargs_variable:
        click.echo("Unable to handle --from as well as cmdline items",
                   err=True)
        sys.exit(1)

    if from_ is not None:
        nargs_variable = from_

    return iter(nargs_variable)


def exists(samples, get=None):
    """Test if any of the samples already exist in the resource"""
    import redbiom.requests
    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

    getter = redbiom.requests.buffered(iter(samples), 'data', 'EXISTS', get=get,
                                       buffer_size=100)
    exists = sum([res for _, res in getter])
    return exists > 0


def samples_from_observations(it, exact, get=None):
    """Grab samples from an iterable of observations"""
    import redbiom.requests

    cmd = 'SINTER' if exact else 'SUNION'
    samples = None
    for _, block in redbiom.requests.buffered(it, 'samples', cmd, get=get):
        block = set(block)

        if not exact:
            if samples is None:
                samples = set()
            samples.update(block)
        else:
            if samples is None:
                samples = block
            else:
                samples = samples.intersection(block)
    return samples


def float_or_nan(t):
    import math
    try:
        return float(t)
    except:
        return math.nan


