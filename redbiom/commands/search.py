import click

from . import cli


@cli.group()
def search():
    """Observation and sample search support."""
    pass


@search.command(name='metadata')
def search_metadata(category, unique, percentiles, counter):
    """List available metadata categories and number of samples described."""
    import redbiom
    import redbiom.requests

    config = redbiom.get_config()
    get = redbiom.requests.make_get(config)

    # on metadata load
    # store into db0
    # increment counts as neededd


@search.command(name="observations")
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.argument('observations', nargs=-1)
def search_observations(from_, exact, observations):
    """Find samples containing observations."""
    import requests.util
    it = requests.util.from_or_nargs(from_, observations)

    # determine the samples which contain the observations of interest
    samples = requests.util.samples_from_observations(it, exact)

    for sample in samples:
        click.echo(sample)


