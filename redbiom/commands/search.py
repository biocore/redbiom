import click

from . import cli


@cli.group()
def search():
    """Observation and sample search support."""
    pass


@search.command(name="observations")
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides observations to search for',
              default=None)
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('observations', nargs=-1)
def search_observations(from_, exact, context, observations):
    """Find samples containing observations."""
    import redbiom._requests
    import redbiom.util

    redbiom._requests.valid(context)

    it = redbiom.util.from_or_nargs(from_, observations)

    # determine the samples which contain the observations of interest
    samples = redbiom.util.samples_from_observations(it, exact, context)

    for sample in samples:
        click.echo(sample)


@search.command(name='metadata')
@click.option('--categories', is_flag=True, required=False, default=False,
              help="Search for metadata categories instead of metadata values")
@click.argument('query', nargs=1)
def search_metadata(query, categories):
    import redbiom.search
    for i in redbiom.search.metadata_full(query, categories):
        click.echo(i)
