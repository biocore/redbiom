import click

from . import cli

@cli.group()
def fetch():
    """Sample data and metadata retrieval."""
    pass


@fetch.command(name='sample-metadata')
@click.option('--table', required=False, type=click.Path(exists=True))
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False))
@click.argument('samples', nargs=-1)
def fetch_sample_metadata(table, from_, samples, output):
    """Retreive sample metadata."""
    if table is not None:
        if from_ is not None or samples:
            click.echo("Cannot specify --table with --from or cmdline samples",
                       err=True)
            import sys
            sys.exit(1)

        import h5py
        it = iter(h5py.File(table)['sample/ids'][:])
    else:
        import redbiom.util
        it = redbiom.util.from_or_nargs(from_, samples)

    import json
    from collections import defaultdict
    import pandas as pd
    import redbiom
    import redbiom.requests

    config = redbiom.get_config()
    get = redbiom.requests.make_get(config)

    # TODO: express metadata-categories using redis sets
    # and then this can be done with SINTER
    all_columns = []
    all_samples = []
    getter = redbiom.requests.buffered(it, 'categories', 'MGET',
                                       'metadata', get=get, buffer_size=100)
    for samples, columns_by_sample in getter:
        all_samples.extend(samples)
        for column_set in columns_by_sample:
            if column_set is not None:
                column_set = json.loads(column_set)
                all_columns.append(set(column_set))

    common_columns = set(all_columns[0])
    for columns in all_columns[1:]:
        common_columns = common_columns.intersection(columns)

    metadata = defaultdict(dict)
    for sample in all_samples:
        metadata[sample]['#SampleID'] = sample

    for category in common_columns:
        key = 'category:%s' % category
        getter = redbiom.requests.buffered(iter(all_samples), None, 'HMGET',
                                           'metadata', get=get, buffer_size=100,
                                           multikey=key)

        for samples, category_values in getter:
            for sample, value in zip(samples, category_values):
                metadata[sample][category] = value

    md = pd.DataFrame(metadata).T
    md.to_csv(output, sep='\t', header=True, index=False)


@fetch.command(name='observations')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False))
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.option('--context', required=True, type=str)
@click.argument('observations', nargs=-1)
def fetch_samples_from_obserations(observations, exact, from_, output, context):
    """Fetch sample data containing observations."""
    import redbiom
    import redbiom.requests
    import redbiom.util

    it = redbiom.util.from_or_nargs(from_, observations)

    config = redbiom.get_config()
    get = redbiom.requests.make_get(config)

    # determine the samples which contain the observations of interest
    samples = redbiom.util.samples_from_observations(it, exact, context,
                                                     get=get)

    _biom_from_samples(iter(samples), context, output, get=get)


@fetch.command(name='samples')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False))
@click.option('--context', required=True, type=str)
@click.argument('samples', nargs=-1)
def fetch_samples_from_samples(samples, from_, output, context):
    """Fetch sample data."""
    import redbiom.util
    it = redbiom.util.from_or_nargs(from_, samples)
    _biom_from_samples(it, context, output)


def _biom_from_samples(samples, context, output, get=None):
    """Create a BIOM table from an iterable of samples"""
    import json
    from operator import itemgetter
    import scipy.sparse as ss
    import biom
    import h5py
    import redbiom.requests

    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

    # pull out the observation index so the IDs can be remapped
    obs_index = json.loads(get(context, 'GET', '__observation_index'))

    # redis contains {observation ID -> internal ID}, and we need
    # {internal ID -> observation ID}
    inverted_obs_index = {v: k for k, v in obs_index.items()}

    # pull out the per-sample data
    table_data = []
    unique_indices = set()
    getter = redbiom.requests.buffered(samples, 'data', 'MGET', context,
                                       get=get, buffer_size=100)
    for (sample_set, sample_set_data) in getter:
        for sample, data in zip(sample_set, sample_set_data):
            data = data.split('\t')
            table_data.append((sample, data))

            # update our perspective of total unique observations
            unique_indices.update({int(i) for i in data[::2]})

    # construct a mapping of
    # {observation ID : index position in the BIOM table}
    unique_indices_map = {observed: index
                          for index, observed in enumerate(unique_indices)}

    # pull out the observation and sample IDs in the desired ordering
    obs_ids = [inverted_obs_index[k]
               for k, _ in sorted(unique_indices_map.items(),
                                  key=itemgetter(1))]
    sample_ids = [d[0] for d in table_data]

    # fill in the matrix
    mat = ss.lil_matrix((len(unique_indices), len(table_data)))
    for col, (sample, col_data) in enumerate(table_data):
        # since this isn't dense, hopefully roworder doesn't hose us
        for index, value in zip(col_data[::2], col_data[1::2]):
            mat[unique_indices_map[int(index)], col] = value

    # write it out
    table = biom.Table(mat, obs_ids, sample_ids)
    with h5py.File(output, 'w') as fp:
        table.to_hdf5(fp, 'redbiom')
