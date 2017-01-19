import click

from . import cli


@cli.group()
def summarize():
    """Summarize things."""
    pass


@summarize.command(name='caches')
def summarize_caches():
    """List names of available caches"""
    # db0 needs map of "cachename" -> db idx
    # basically all commands need to accept where to operate in
    pass

@summarize.command(name='metadata-category')
@click.option('--category')
@click.option('--unique')
@click.option('--counter')
@click.option('--histogram')
def summarize_metadata_category():
    """Summarize the values within a metadata category"""
    pass


    ## need a easy means to discover all category:* keys. pack into DB0.
    ## scan/hscan etc does not seem to work with webdis.

@summarize.command(name='observations')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--category', type=str, required=True)
@click.option('--value', type=str, required=False, default=None,
              help="Restrict to a specific value; prints the sample IDs")
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.argument('observations', nargs=-1)
def summarize_observations(from_, category, exact, value, observations):
    """Summarize observations over a metadata category."""
    import redbiom
    import redbiom.requests
    import redbiom.util

    it = redbiom.util.from_or_nargs(from_, observations)

    config = redbiom.get_config()
    get = redbiom.requests.make_get(config)

    samples = redbiom.util.samples_from_observations(it, exact, get=get)
    _summarize_samples(samples, category, value, get)


@summarize.command(name='samples')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--category', type=str, required=True)
@click.option('--value', type=str, required=False, default=None,
              help="Restrict to a specific value; prints the sample IDs")
@click.argument('samples', nargs=-1)
def summarize_samples(from_, category, value, samples):
    """Summarize samples over a metadata category."""
    import redbiom.util
    it = redbiom.util.from_or_nargs(from_, samples)
    _summarize_samples(it, category, value)


def _summarize_samples(samples, category, value, get):
    """Summarize an iterable of samples based on criteria"""
    from redbiom.requests import buffered

    key = 'category:%s' % category
    getter = buffered(iter(samples), None, 'HMGET', get=get, buffer_size=100,
                      multikey=key)

    results = []
    for samples, category_values in getter:
        for sample, observed_value in zip(samples, category_values):
            results.append((sample, observed_value))

    if value is None:
        from collections import Counter
        from operator import itemgetter

        cat_stats = Counter([v for s, v in results])
        for val, count in sorted(cat_stats.items(), key=itemgetter(1),
                                 reverse=True):
            click.echo("%s\t%s" % (val, count))
        click.echo("\n%s\t%s" % ("Total samples", len(results)))
    else:
        import shlex
        tokens = list(shlex.shlex(value))
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
                click.echo("Matching with exact string", err=True)
                func = lambda to_test: to_test == value
            elif tokens[0] in ('in', 'notin'):
                rh = [t.strip("'").strip('"') for t in tokens[1:] if t != ',']
                tokens = set(rh)
                func = lambda to_test: operator(to_test, rh)
            else:
                rh = tokens[1]
                if len(tokens) > 2:
                    raise ValueError("Unexpected criteria: %s" % value)
                try:
                    rh = float(rh)
                except TypeError:
                    if operator in {'<=', '>='}:
                        raise ValueError("Right hand does not look numeric")

                func = lambda to_test: operator(_float_or_nan(to_test), rh)
        else:
            func = lambda to_test: to_test == value

        for s, v in results:
            if func(v):
                click.echo(s)
