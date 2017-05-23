import click

from . import cli


@cli.group()
def select():
    """Select items based on metadata"""
    pass


@select.command(name='samples-from-metadata')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('query', nargs=1)
@click.argument('samples', nargs=-1)
def select_samples_from_metadata(from_, context, query, samples):
    """Given samples, select based on metadata"""
    import redbiom.util
    import redbiom.search

    import redbiom
    import redbiom._requests
    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)

    iterator = redbiom.util.from_or_nargs(from_, samples)

    _, _, ambig, _ = redbiom.util.resolve_ambiguities(context, iterator, get)

    full_search = redbiom.search.metadata_full(query)

    for i in (full_search & set(ambig)):
        for rid in ambig[i]:  # get unambiguous redbiom id
            click.echo(rid)


@select.command(name='features-from-samples')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.option('--exact', is_flag=True, default=False,
              help="All found features must exist in all samples")
@click.argument('samples', nargs=-1)
def features(from_, context, exact, samples):
    """Given samples, select the features associated."""
    import redbiom
    import redbiom._requests
    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)
    import redbiom.util

    iterator = redbiom.util.from_or_nargs(from_, samples)
    _, _, _, rids = redbiom.util.resolve_ambiguities(context, iterator, get)

    for i in redbiom.util.ids_from(rids, exact, 'sample', context):
        click.echo(i)
