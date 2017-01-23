import click

from . import cli


@cli.group()
def search():
    """Observation and sample search support."""
    pass


@search.command(name="observations")
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.option('--context', required=True, type=str)
@click.argument('observations', nargs=-1)
def search_observations(from_, exact, context, observations):
    """Find samples containing observations."""
    import redbiom.requests
    import redbiom.util

    redbiom.requests.valid(context)

    it = redbiom.util.from_or_nargs(from_, observations)

    # determine the samples which contain the observations of interest
    samples = redbiom.util.samples_from_observations(it, exact, context)

    for sample in samples:
        click.echo(sample)
