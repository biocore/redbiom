import click

from . import cli


@cli.group()
def fetch():
    """Sample data and metadata retrieval."""
    pass


@fetch.command(name='sample-metadata')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--all-columns', is_flag=True, default=False,
              help=("If set, all metadata columns for all samples are "
                    "obtained. The empty string is used if the column is not "
                    "present for a given sample."))
@click.argument('samples', nargs=-1)
def fetch_sample_metadata(from_, samples, all_columns, output):
    """Retreive sample metadata."""
    import redbiom.util
    iterator = redbiom.util.from_or_nargs(from_, samples)

    import redbiom.fetch
    md = redbiom.fetch.sample_metadata(iterator, common=not all_columns)

    md.to_csv(output, sep='\t', header=True, index=False)


@fetch.command(name='observations')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides observations to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('observations', nargs=-1)
def fetch_samples_from_obserations(observations, exact, from_, output,
                                   context):
    """Fetch sample data containing observations."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, observations)

    import redbiom.fetch
    table = redbiom.fetch.data_from_observations(context, iterable, exact)

    import h5py
    with h5py.File(output, 'w') as fp:
        table.to_hdf5(fp, 'redbiom')


@fetch.command(name='samples')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('samples', nargs=-1)
def fetch_samples_from_samples(samples, from_, output, context):
    """Fetch sample data."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, samples)

    import redbiom.fetch
    table = redbiom.fetch.data_from_samples(context, iterable)

    import h5py
    with h5py.File(output, 'w') as fp:
        table.to_hdf5(fp, 'redbiom')
