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


def _make_post(config):
    import requests
    s = requests.Session()
    s.auth = config['auth']
    def f(payload):
        return s.post(config['hostname'], data=payload)
    return f


def _make_put(config):
    import requests
    s = requests.Session()
    s.auth = config['auth']
    def f(url, data):
        url = '/'.join([config['hostname'], url])
        return s.put(url, data=data)
    return f


def _make_get(config):
    import requests
    s = requests.Session()
    s.auth = config['auth']
    def f(url):
        url = '/'.join([config['hostname'], url])
        return s.get(url)
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
            print("%s is blocked" % table)
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
        columns = [i.upper() for i in row.index if i not in null_values]
        url = "SET/metadata-categories:%s" % idx
        req = put(url, json.dumps(columns))
        if req.status_code != 200:
            raise requests.HTTPError('Failed to update; ID: %s; payload size: '
                                     '%d' % (idx, len(payload)))

    for col in indexed_columns:
        bulk_set = ["%s/%s" % (idx, v) for idx, v in zip(md.index, md[col])
                    if _indexable(v, null_values)]

        # NOTE: columns are normalized to upper case
        payload = "HMSET/category:%s/%s" % (col.upper(), '/'.join(bulk_set))
        req = post(payload)
        if req.status_code != 200:
            raise requests.HTTPError('Failed to update; ID: %s' % col)


@cli.command()
@click.option('--table', required=True, type=click.Path(exists=True))
@click.option('--output', required=True, type=click.Path(exists=False))
def fetch_metadata(table, output):
    """Fetch the common metadata categories for the samples in the table"""
    import h5py
    import json

    config = _get_config()
    get = _make_get(config)

    samples = h5py.File(table)['sample/ids'][:]

    all_columns = []
    for start in range(0, len(samples), 100):
        bulk = '/'.join(['metadata-categories:%s' % s for s in samples[start:start+100]])
        req = get('MGET/%s' % bulk)
        columns_per_sample = req.json()['MGET']

        for column_set in columns_per_sample:
            if column_set is not None:
                column_set = json.loads(column_set)
                all_columns.append(set(column_set))

    common_columns = set(all_columns[0])
    for columns in all_columns[1:]:
        common_columns = common_columns.intersection(columns)

    from collections import defaultdict
    metadata = defaultdict(dict)
    for sample in samples:
        metadata[sample]['#SampleID'] = sample

    for start in range(0, len(samples), 100):
        sample_subset = samples[start:start+100]
        bulk_samples = '/'.join(sample_subset)
        for category in common_columns:
            req = get('HMGET/category:%s/%s' % (category, bulk_samples))
            if req.status_code != 200:
                raise ValueError('Failed to get: %s' % category)
            values = req.json()['HMGET']
            for sample, value in zip(sample_subset, values):
                metadata[sample][category] = value

    import pandas
    md = pandas.DataFrame(metadata).T
    md.to_csv(output, sep='\t', header=True, index=False)

@cli.command()
@click.option('--category', required=False, type=str)
@click.argument('observations', nargs=-1)
def find_samples(category, observations):
    """Find all samples in which the observations appear

    Print category stats if desired
    """
    if len(observations) < 1:
        import sys
        click.echo('Need at least 1 observation', err=True)
        sys.exit(1)  # should be doable from click but need ctx

    config = _get_config()
    get = _make_get(config)

    # determine the samples which contain the observations of interest
    samples = set()
    for start in range(0, len(observations), 10):
        bulk = '/'.join(['samples:%s' % i for i in observations[start:start+10]])
        req = get('SUNION/%s' % bulk)
        if req.status_code != 200:
            raise requests.HTTPError(':(')
        samples.update(set(req.json()['SUNION']))

    if category is None:
        click.echo('\n'.join(samples))
    else:
        cat_results = []
        samples = list(samples)
        # larger chunk size as sample names usually << sOTUs
        for start in range(0, len(samples), 100):
            sample_slize = samples[start:start+100]
            req = get('HMGET/category:%s/%s' % (category, '/'.join(sample_slize)))
            if req.status_code != 200:
                raise requests.HTTPError(':(')
            cat_results.extend(req.json()['HMGET'])
        import collections
        from operator import itemgetter
        cat_stats = collections.Counter(cat_results)
        for val, count in sorted(cat_stats.items(), key=itemgetter(1), reverse=True):
            click.echo("%s\t%s" % (val, count))
        click.echo("\n%s\t%s" % ("Total samples", len(samples)))

@cli.command()
@click.option('--output', required=True, type=click.Path(exists=False))
@click.argument('observations', nargs=-1)
def fetch_samples(observations, output):
    """Obtain all samples in which the observations appear"""
    if len(observations) < 1:
        import sys
        click.echo('Need at least 1 observation', err=True)
        sys.exit(1)  # should be doable from click but need ctx

    import json
    from operator import itemgetter
    import scipy.sparse as ss
    import biom
    import h5py

    config = _get_config()
    get = _make_get(config)

    # determine the samples which contain the observations of interest
    samples = set()
    for observation in observations:
        req = get('SMEMBERS/samples:%s' % observation)
        if req.status_code != 200:
            raise requests.HTTPError('Failed to get: %s' % observation)
        samples.update(set(req.json()['SMEMBERS']))

    # pull out the observation index so the IDs can be remapped
    req = get('GET/__observation_index')
    if req.status_code != 200:
        raise requests.HTTPError('Failed to get observation index')
    obs_index = req.json()['GET']
    obs_index = json.loads(obs_index)

    # redis contains {observation ID -> internal ID}, and we need
    # {internal ID -> observation ID}
    inverted_obs_index = {v: k for k, v in obs_index.items()}

    # pull out the per-sample data
    table_data = []
    unique_indices = set()
    samples = list(samples)
    for start in range(0, len(samples), 100):
        sample_set = samples[start:start+100]
        bulk = '/'.join(['data:%s' % s for s in sample_set])
        req = get('MGET/%s' % bulk)
        if req.status_code != 200:
            raise requests.HTTPError('Failed')
        sample_set_data = req.json()['MGET']
        for sample, data in zip(sample_set, sample_set_data):
            data = data.split('\t')
            table_data.append((sample, data))

            # update our perspective of total unique observations
            unique_indices.update({int(i) for i in data[::2]})  # every other position is an ID index

    # construct a mapping of {observation ID : index position in the BIOM table}
    unique_indices_map = {observed: index for index, observed in enumerate(unique_indices)}

    # pull out the observation and sample IDs in the desired ordering
    obs_ids = [inverted_obs_index[k] for k, _ in sorted(unique_indices_map.items(), key=itemgetter(1))]
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
        table.to_hdf5(fp, 'seqsearch')


@cli.command()
@click.option('--category', type=str, required=True)
@click.option('--operator', required=True,
              type=click.Choice(['eq', 'ne', 'in', 'lt', 'gt']))
@click.option('--value', type=str, required=True)
def search(category, operator, value):
    """Search for samples based off arbitrary criteria"""
    config = _get_config()
    get = _make_get(config)

    op = {'eq': lambda a, b: a == b,
          'ne': lambda a, b: a != b,
          'in': lambda a, b: a in b,
          'lt': lambda a, b: a < b,
          'gt': lambda a, b: a > b}[operator]

    try:
        value = float(value)
    except:
        pass

    if operator == 'in':
        value = value.split(',')
        try:
            value = [float(v) for v in value]
        except:
            pass

    import time
    start = time.time()
    req = get('HGETALL/category:%s' % category)
    if req.status_code != 200:
        raise requests.HTTPError('Failed to get: %s' % category)
    sample_values = req.json()['HGETALL']
    print(time.time() - start)

    import pandas as pd
    for sample, obs_value in sample_values.items():
        try:
            obs_value = float(obs_value)
        except:
            pass
        try:

            if op(obs_value, value):
                #print(sample, obs_value)
                pass
        except:
            pass


if __name__ == '__main__':
    cli()
