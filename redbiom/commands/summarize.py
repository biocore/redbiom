import click

from . import cli


@cli.group()
def summarize():
    """Summarize things."""
    pass


@summarize.command(name='contexts')
def summarize_caches():
    """List names of available caches"""
    import redbiom
    import redbiom.requests
    get = redbiom.requests.make_get(redbiom.get_config())

    contexts = get('state', 'HGETALL', 'contexts')
    if contexts:
        click.echo("Name\tDescription\n")
        for name, desc in sorted(contexts.items()):
            click.echo("%s\t%s" % (name, desc))
    else:
        click.echo("No available contexts")


@summarize.command(name='metadata-category')
@click.option('--category', required=True)
@click.option('--counter', required=False, is_flag=True, default=False)
@click.option('--descending', is_flag=True, required=False, default=False)
@click.option('--dump', required=False, is_flag=True, default=False)
def summarize_metadata_category(category, counter, descending, dump):
    """Summarize the values within a metadata category"""
    import redbiom
    import redbiom.requests
    import pandas as pd

    get = redbiom.requests.make_get(redbiom.get_config())

    keys_vals = get('metadata', 'HGETALL', 'category:%s' % category)
    md = pd.Series(keys_vals)

    if counter:
        click.echo("Category value\tcount")
        counts = md.value_counts(ascending=not descending)
        for idx, val in zip(counts.index, counts):
            click.echo("%s\t%s" % (idx, val))
    elif dump:
        click.echo("#SampleID\t%s" % category)
        for idx, val in zip(md.index, md):
            click.echo("%s\t%s" % (idx, val))
    else:
        click.echo("Please specify either --counter or --dump",
                   err=True)
        import sys
        sys.exit(1)


@summarize.command(name='metadata')
@click.option('--descending', is_flag=True, required=False, default=False)
def summarize_metadata(descending):
    """Get the known metadata categories and associated sample counts"""
    import redbiom
    import redbiom.requests
    import pandas as pd

    get = redbiom.requests.make_get(redbiom.get_config())

    categories = list(get('metadata', 'SMEMBERS', 'categories-represented'))
    results = []
    for category in categories:
        key = 'category:%s' % category
        results.append(int(get('metadata', 'HLEN', key)))

    md = pd.Series(results, index=categories)
    md = md.sort_values(ascending=not descending)

    for idx, val in zip(md.index, md):
        click.echo("%s\t%s" % (idx, val))


@summarize.command(name='observations')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--category', type=str, required=True)
@click.option('--value', type=str, required=False, default=None,
              help="Restrict to a specific value; prints the sample IDs")
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.option('--context', required=True, type=str)
@click.argument('observations', nargs=-1)
def summarize_observations(from_, category, exact, value, context,
                           observations):
    """Summarize observations over a metadata category."""
    import redbiom
    import redbiom.requests
    import redbiom.util

    it = redbiom.util.from_or_nargs(from_, observations)

    config = redbiom.get_config()
    get = redbiom.requests.make_get(config)

    samples = redbiom.util.samples_from_observations(it, exact, context,
                                                     get=get)
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
    getter = buffered(iter(samples), None, 'HMGET', 'metadata', get=get,
                      buffer_size=100, multikey=key)

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
        from redbiom.util import float_or_nan
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

                func = lambda to_test: operator(float_or_nan(to_test), rh)
        else:
            func = lambda to_test: to_test == value

        for s, v in results:
            if func(v):
                click.echo(s)
