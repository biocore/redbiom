import click

from . import cli

@cli.group()
def admin():
    """Update database, etc."""
    pass


@admin.command(name='load-observations')
@click.option('--table', required=True, type=click.Path(exists=True))
def load_observations(table):
    """Load observation to sample mappings.

    For each observation, all samples in the table associated with the
    observation are added to a Redis set keyed by "samples:<observation_id>".
    """
    import biom
    import redbiom
    import redbiom.requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom.requests.make_post(config)
    get = redbiom.requests.make_get(config)

    tab = biom.load_table(table)
    samples = tab.ids()[:]

    if redbiom.util.exists(samples, get=get):
        raise ValueError("%s contains sample IDs already stored" % table)

    for values, id_, _ in tab.iter(axis='observation', dense=False):
        observed = samples[values.indices]

        payload = "samples:%s/%s" % (id_, "/".join(observed))
        post('SADD', payload)


@admin.command(name='load-sample-data')
@click.option('--table', required=True, type=click.Path(exists=True))
def load_sample_data(table):
    """Load nonzero entries per sample.

    WARNING: this method does not support non count data.

    The observation IDs are remapped into an integer space to reduce memory
    consumption as sOTUs are large. The index is maintained in Redis.

    The indexing scheme requires a central authority for obtaining a unique
    and stable index value. As such, this method obtains a lock for performing
    an update. The index is updated with any new observations seen on each
    load and is stored under the "__observation_index" key. It is stored as
    a JSON object mapping the original observation ID to a stable and unique
    integer; stability and uniqueness is not assured across distinct redis
    databases.

    The data are stored per sample with keys of the form "data:<sample_id>".
    The string stored is tab delimited, where the even indices (i.e .0, 2, 4,
    etc) correspond to the unique index value for an observation ID, and the
    odd indices correspond to the counts associated with the sample/observation
    combination.
    """
    import biom
    import time
    import json
    import redbiom
    import redbiom.requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom.requests.make_post(config)
    get = redbiom.requests.make_get(config)

    tab = biom.load_table(table)
    obs = tab.ids(axis='observation')
    samples = tab.ids()

    if redbiom.util.exists(samples, get=get):
        raise ValueError("%s contains sample IDs already stored" % table)

    acquired = False
    while not acquired:
        # not using redlock as time interval isn't that critical
        acquired = get('SETNX', '__load_table_lock/1') == 1
        if not acquired:
            click.echo("%s is blocked" % table)
            time.sleep(1)

    try:
        # load the observation index
        obs_index = get('GET', '__observation_index')
        if obs_index is None:
            obs_index = {}
        else:
            obs_index = json.loads(obs_index)

        # update the observation index if and where necessary
        new_ids = {i for i in obs if i not in obs_index}
        id_start = max(obs_index.values() or [-1]) + 1
        for idx, id_ in zip(range(id_start, id_start + len(new_ids)), new_ids):
            obs_index[id_] = idx

        # load up the table per-sample
        for values, id_, _ in tab.iter(dense=False):
            int_values = values.astype(int)
            remapped = [obs_index[i] for i in obs[values.indices]]

            packed = '\t'.join(["%s\t%d" % (i, v)
                                for i, v in zip(remapped,
                                                int_values.data)])
            post('SET', 'data:%s/%s' % (id_, packed))

        # store the index following the load of the table
        post('SET', '__observation_index/%s' % json.dumps(obs_index))
    except:
        raise
    finally:
        # release the lock no matter what
        get('DEL', '__load_table_lock')


@admin.command(name='load-sample-metadata')
@click.option('--metadata', required=True, type=click.Path(exists=True))
def load_sample_metadata(metadata):
    """Load sample metadata."""
    import pandas as pd
    import json
    import redbiom
    import redbiom.requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom.requests.make_post(config)
    put = redbiom.requests.make_put(config)
    get = redbiom.requests.make_get(config)

    md = pd.read_csv(metadata, sep='\t', dtype=str).set_index('#SampleID')
    samples = md.index
    if redbiom.util.exists(samples, get=get):
        raise ValueError("%s contains sample IDs already stored" % metadata)

    null_values = {'Not applicable', 'Unknown', 'Unspecified',
                   'Missing: Not collected',
                   'Missing: Not provided',
                   'Missing: Restricted access',
                   'null', 'NULL', 'no_data', 'None', 'nan'}

    indexed_columns = md.columns
    for idx, row in md.iterrows():
        # denote what columns contain information
        columns = [c for c, i in zip(md.columns, row.values)
                   if _indexable(i, null_values)]
        key = "metadata-categories:%s" % idx

        # TODO: express metadata-categories using redis sets
        # TODO: dumps is expensive relative to just, say, '\t'.join
        put('SET', key, json.dumps(columns))

    for col in indexed_columns:
        bulk_set = ["%s/%s" % (idx, v) for idx, v in zip(md.index, md[col])
                    if _indexable(v, null_values)]

        payload = "category:%s/%s" % (col, '/'.join(bulk_set))
        post('HMSET', payload)


def _indexable(value, nullables):
    """Returns true if the value appears to be something that storable

    IMPORTANT: we cannot store values which contain a "/" as that character
    has a special meaning for a path.
    """
    if value in nullables:
        return False

    if isinstance(value, (float, int, bool)):
        return True
    else:
        return '/' not in value
