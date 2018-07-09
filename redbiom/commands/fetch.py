import click

from . import cli


@cli.group()
def fetch():
    """Sample data and metadata retrieval."""
    pass


@fetch.command(name='samples-contained')
@click.option('--context', required=False, type=str, default=None,
              help="The context to fetch from.")
@click.option('--unambiguous', required=False, is_flag=True, default=False,
              help="Return ambiguous or unambiguous identifiers")
def fetch_samples_contained(context, unambiguous):
    """Get samples within a context.

    Return all of the sample identifiers which are represented in a context.
    """
    import redbiom.fetch
    for id_ in redbiom.fetch.samples_in_context(context, unambiguous):
        click.echo(id_)


@fetch.command(name='features-contained')
@click.option('--context', required=False, type=str, default=None,
              help="The context to fetch from.")
def fetch_features_contained(context):
    """Get features within a context.

    Return all of the features which are represented in a context.
    """
    import redbiom.fetch
    for id_ in redbiom.fetch.features_in_context(context):
        click.echo(id_)


@fetch.command(name='sample-metadata')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--context', required=False, type=str, default=None,
              help="The context to search within.")
@click.option('--all-columns', is_flag=True, default=False,
              help=("If set, all metadata columns for all samples are "
                    "obtained. The empty string is used if the column is not "
                    "present for a given sample."))
@click.option('--tagged', is_flag=True, default=False,
              help=("Obtain the tag specific metadata (e.g., preparation "
                    "information)."))
@click.argument('samples', nargs=-1)
def fetch_sample_metadata(from_, samples, all_columns, context, output,
                          tagged):
    """Retreive sample metadata."""
    import redbiom.util
    iterator = redbiom.util.from_or_nargs(from_, samples)

    import redbiom.fetch
    md, map_ = redbiom.fetch.sample_metadata(iterator, context=context,
                                             common=not all_columns,
                                             tagged=tagged)

    md.to_csv(output, sep='\t', header=True, index=False, encoding='utf-8')

    _write_ambig(map_, output)


@fetch.command(name='features')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides features to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified features")
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('features', nargs=-1)
def fetch_samples_from_obserations(features, exact, from_, output,
                                   context):
    """Fetch sample data containing features."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, features)

    import redbiom.fetch
    tab, map_ = redbiom.fetch.data_from_features(context, iterable, exact)

    import h5py
    with h5py.File(output, 'w') as fp:
        tab.to_hdf5(fp, 'redbiom')

    _write_ambig(map_, output)


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
    table, ambig = redbiom.fetch.data_from_samples(context, iterable)

    import h5py
    with h5py.File(output, 'w') as fp:
        table.to_hdf5(fp, 'redbiom')

    _write_ambig(ambig, output)


def _write_ambig(map_, output):
    has_ambig = {len(v) for v in map_.values()}

    if has_ambig and has_ambig != set([1]):
        import json
        ambig = {k: v for k, v in map_.items() if len(v) > 1}
        click.echo("%d sample ambiguities observed. Writing ambiguity "
                   "mappings to: %s" % (len(ambig), output + '.ambiguities'),
                   err=True)
        with open(output + '.ambiguities', 'w') as fp:
            fp.write(json.dumps(ambig))
