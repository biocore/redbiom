import click


def _get_config():
    import os
    import requests.auth
    user = os.environ.get('SEQUENCE_SEARCH_USER')
    password = os.environ.get('SEQUENCE_SEARCH_PASSWORD')
    hostname = os.environ.get('SEQUENCE_SEARCH_HOST', 'http://127.0.0.1:7379')

    if user is None:
        auth = None
    else:
        auth = requests.auth(config['user'], config['password'])
    return {'auth': auth, 'hostname': hostname}

### each factory should create and maintain its own session

def _make_post(config):
    import requests
    def f(payload):
        return requests.post(config['hostname'], data=payload,
                             auth=config['auth'])
    return f


def _make_put(config):
    import requests
    def f(url, data):
        url = '/'.join([config['hostname'], url])
        return requests.put(url, data=data, auth=config['auth'])
    return f


def _make_get(config):
    import requests
    def f(url):
        url = '/'.join([config['hostname'], url])
        return requests.get(url, auth=config['auth'])
    return f


def _indexable(value, nullables):
    if value in nullables:
        return False

    if isinstance(value, (float, int, bool)):
        return True
    else:
        return '/' not in value


@click.group()
def cli():
    pass


@cli.command()
@click.option('--table', required=True, type=click.Path(exists=True))
def update_observations(table):
    """Update DB observation to and sample mappings

    For each observation, all samples in the table associated with the
    observation are added to a Redis set keyed by "samples:<observation_id>".
    """
    import biom

    config = _get_config()
    post = _make_post(config)

    tab = biom.load_table(table)
    samples = tab.ids()[:]

    for values, id_, _ in tab.iter(axis='observation', dense=False):
        observed = samples[values.indices]
        payload = "SADD/samples:%s/%s" % (id_, "/".join(observed))

        req = post(payload)
        if req.status_code != 200:
            raise requests.HTTPError('Failed to update; ID: %s; payload size: '
                                     '%d' % (id_, len(payload)))


@cli.command()
@click.option('--table', required=True, type=click.Path(exists=True))
def load_table(table):
    """Load the nonzero COUNTS of the table into Redis

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

    config = _get_config()
    post = _make_post(config)
    get = _make_get(config)

    tab = biom.load_table(table)
    samples = tab.ids()[:]
    obs = tab.ids(axis='observation')

    acquired = False
    while not acquired:
        # not using redlock as time interval isn't that critical
        req = get('SETNX/__load_table_lock/1')
        if req.status_code != 200:
            raise requests.HTTPError('Failed to test/set lock')
        acquired = req.json()['SETNX'] == 1
        if not acquired:
            time.sleep(1)

    try:
        # load the observation index
        req = get('GET/__observation_index')
        if req.status_code != 200:
            raise requests.HTTPError('Failed to test/set lock')
        obs_index = req.json()['GET']
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
            post('SET/data:%s/%s' % (id_, packed))
            if req.status_code != 200:
                raise HTTPError("Failed to load %s" % id_)

        # store the index following the load of the table
        post('SET/__observation_index/%s' % json.dumps(obs_index))
        if req.status_code != 200:
            raise HTTPError("Failed to insert index")

    finally:
        # release the lock no matter what
        req = get('DEL/__load_table_lock')
        if req.status_code != 200:
            raise HTTPError("Cannot release lock")


@cli.command()
@click.option('--metadata', required=True, type=click.Path(exists=True))
def update_metadata(metadata):
    """Update DB sample metadata"""
    import pandas as pd
    import json

    config = _get_config()
    post = _make_post(config)
    put = _make_put(config)

    md = pd.read_csv(metadata, sep='\t', dtype=object).set_index('#SampleID')

    null_values = {'Not applicable',
                   'Missing: Not collected',
                   'Missing: Not provided',
                   'Missing: Restricted access',
                   'null', 'NULL', 'no_data', 'None'}

    indexed_columns = md.columns
    for idx, row in md.iterrows():
        # denote what columns contain information
        columns = [i for i in row.index if i not in null_values]
        url = "HSET/metadata:%s/__COLUMNS" % idx
        req = put(url, json.dumps(columns))
        if req.status_code != 200:
            raise requests.HTTPError('Failed to update; ID: %s; payload size: '
                                     '%d' % (idx, len(payload)))

    for col in indexed_columns:
        bulk_set = ["%s/%s" % (idx, v) for idx, v in zip(md.index, md[col])
                    if _indexable(v, null_values)]

        payload = "HMSET/category:%s/%s" % (col, '/'.join(bulk_set))
        req = post(payload)
        if req.status_code != 200:
            raise requests.HTTPError('Failed to update; ID: %s' % col)


@cli.command()
@click.option('--output', required=True, type=click.Path(exists=False))
@click.argument('observations', nargs=-1)
def fetch(observations, output):
    if len(observations) < 1:
        import sys
        click.echo('Need at least 1 observation', err=True)
        sys.exit(1)  # should be doable from click but need ctx

    import json
    from operator import itemgetter

    config = _get_config()
    get = _make_get(config)

    table = []
    samples = set()
    for observation in observations:
        req = get('SMEMBERS/samples:%s' % observation)
        if req.status_code != 200:
            raise requests.HTTPError('Failed to get: %s' % observation)
        samples.update(set(req.json()['SMEMBERS']))

    req = get('GET/__observation_index')
    if req.status_code != 200:
        raise requests.HTTPError('Failed to test/set lock')
    obs_index = req.json()['GET']
    obs_index = json.loads(obs_index)
    inverted_obs_index = {v: k for k, v in obs_index.items()}

    table_data = []
    unique_indices = set()
    for sample in samples:
        req = get('GET/data:%s' % sample)
        if req.status_code != 200:
            raise requests.HTTPError('Failed to get: %s' % observation)
        data = req.json()['GET'].split('\t')
        table_data.append((sample, data))
        unique_indices.update({int(i) for i in data[::2]})  # every other position is an ID index
    print(len(table_data))

    import scipy.sparse as ss
    unique_indices_map = {observed: index for index, observed in enumerate(unique_indices)}
    obs_ids = [inverted_obs_index[k] for k, _ in sorted(unique_indices_map.items(), key=itemgetter(1))]
    sample_ids = [d[0] for d in table_data]

    mat = ss.lil_matrix((len(unique_indices), len(table_data)))
    for col, (sample, col_data) in enumerate(table_data):
        # since this isn't dense, hopefully roworder doesn't hose us
        for index, value in zip(col_data[::2], col_data[1::2]):
            mat[unique_indices_map[int(index)], col] = value
    import biom
    table = biom.Table(mat, obs_ids, sample_ids)
    import h5py
    with h5py.File(output, 'w') as fp:
        table.to_hdf5(fp, 'seqsearch')


if __name__ == '__main__':
    cli()
