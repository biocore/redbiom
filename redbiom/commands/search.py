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
@click.option('--restrict-to', required=True, type=str,
              help="A comma separated list of categories to restrict the "
                   "retrieval of metadata too. This is strictly done in order "
                   "to limit the expense of obtaining all of the metadata in "
                   "common for the samples under investigation. It is assumed "
                   "that the WHERE clause is applicable to the columns in the "
                   "restricted set.")
@click.option('--where', required=True, type=str,
              help="The WHERE clause to apply")
def search_metadata(restrict_to, where):
    """Find samples by metadata"""
    import redbiom.fetch

    if restrict_to is not None:
        restrict_to = restrict_to.split(',')

    ids = redbiom.fetch.metadata(where=where, restrict_to=restrict_to)
    for i in ids:
        click.echo(i)
