# TODO: with decoupled logic, it may make sense to have returns from some of
# these methods more than just from load_sample_metadata.


def create_context(name, description):
    """Create a context within the cache

    Parameters
    ----------
    name : str
        The name of the context, e.g., deblur@150nt
    description : str
        A brief description about the context, e.g., "Default quality
        filtering, followed by application of Deblur with a trim length of
        150nt."

    Redis commmand summary
    ----------------------
    HSET state:context <name> <description>
    """
    import redbiom
    import redbiom.requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom.requests.make_post(config)

    post('state', 'HSET', "contexts/%s/%s" % (name, description))


def load_observations(table, context):
    """Load observation to sample mappings.

    Parameters
    ----------
    table : str, filepath
        The filepath to a BIOM table to load.
    context : str
        The context to load into.

    Raises
    ------
    ValueError
        If the context to load into does not exist.
        If any sample in the table already has been loaded.
        If a samples metadata has not already been loaded.

    Redis command summary
    ---------------------
    SMEMBERS <context>:samples-represented-observations
    SADD <context>:samples:<observation_id> <sample_id> ... <sample_id>
    SADD <context>:samples-represented-observations <sample_id> ... <sample_id>
    """
    import biom
    import redbiom
    import redbiom.requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom.requests.make_post(config)
    get = redbiom.requests.make_get(config)

    redbiom.requests.valid(context, get)

    tab = biom.load_table(table)
    samples = tab.ids()[:]

    represented = get(context, 'SMEMBERS', 'samples-represented-observations')
    if set(samples).intersection(set(represented)):
        raise ValueError("At least one sample to load already exists")

    if not redbiom.util.has_sample_metadata(samples):
        raise ValueError("Sample metadata must be loaded first.")

    for values, id_, _ in tab.iter(axis='observation', dense=False):
        observed = samples[values.indices]

        payload = "samples:%s/%s" % (id_, "/".join(observed))
        post(context, 'SADD', payload)

    payload = "samples-represented-observations/%s" % '/'.join(samples)
    post(context, 'SADD', payload)


def load_sample_data(table, context):
    """Load nonzero sample data.

    Parameters
    ----------
    table : str, filepath
        The filepath to a BIOM table to load.
    context : str
        The context to load into.

    Raises
    ------
    ValueError
        If the context to load into does not exist.
        If any sample in the table already has been loaded.
        If a samples metadata has not already been loaded.

    Notes
    -----
    This method does not support non count data.

    This load is formed to be performed in serial within a given context. It
    is safe to issue load commands in parallel as a lock within a context is
    obtained prior to performing the load.

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

    Redis command summary
    ---------------------
    SMEMBERS <context>:samples-represented-observations
    SETNX <context>:__load_table_lock 1
    GET <context>:__observation_index
    SET <context>:data:<sample_id> <packed-nz-representation>
    SET <context>:__observation_index <revised-observation-mappings>
    DEL <context>:__load_table_lock
    SADD <context>:samples-represented-data <sample_id> ... <sample_id>
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

    redbiom.requests.valid(context, get)

    tab = biom.load_table(table)
    obs = tab.ids(axis='observation')
    samples = tab.ids()

    if not redbiom.util.has_sample_metadata(samples):
        raise ValueError("Sample metadata must be loaded first.")

    represented = get(context, 'SMEMBERS', 'samples-represented-data')
    if set(samples).intersection(set(represented)):
        raise ValueError("At least one sample to load already exists")

    acquired = False
    while not acquired:
        # not using redlock as time interval isn't that critical
        acquired = get(context, 'SETNX', '__load_table_lock/1') == 1
        if not acquired:
            # TODO: support verbose
            time.sleep(1)

    try:
        # load the observation index
        obs_index = get(context, 'GET', '__observation_index')
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
            post(context, 'SET', 'data:%s/%s' % (id_, packed))

        # store the index following the load of the table
        post(context, 'SET', '__observation_index/%s' % json.dumps(obs_index))
    except:
        raise
    finally:
        # release the lock no matter what
        get(context, 'DEL', '__load_table_lock')

    payload = "samples-represented-data/%s" % '/'.join(samples)
    post(context, 'SADD', payload)


def load_sample_metadata(metadata):
    """Load sample metadata.

    Parameters
    ----------
    metadata : str, filepath
        A filepath to QIIME compatible sample metadata.

    Notes
    -----
    Values considered to be non-informative are omitted from load.

    TODO: expose a stable list of the nullables

    A lock is obtained for loading the sample metadata forcing this to be a
    serial operation. However, this limitation is slated to be removed upon
    support for "tags", or in qiita parlance, the prep ID.

    Raises
    ------
    ValueError
        If no new samples are found to load.

    Returns
    -------
    int
        The number of samples loaded.

    Redis command summary
    ---------------------
    SETNX metadata:__load_md_lock 1
    SMEMBERS metadata:samples-represented
    SET metadata:categories:<sample_id> <JSON-of-informative-columns>
    HMSET metadata:category:<column> <sample_id> <val> ... <sample_id> <val>
    SADD metadata:samples-represented <sample_id> ... <sample_id> ...
    SADD metadata:categories-represented <column> ... <column>
    DEL metadata:__load_md_lock

    """
    import pandas as pd
    import json
    import redbiom
    import redbiom.requests
    import redbiom.util
    import time

    config = redbiom.get_config()
    post = redbiom.requests.make_post(config)
    put = redbiom.requests.make_put(config)
    get = redbiom.requests.make_get(config)

    null_values = {'Not applicable', 'Unknown', 'Unspecified',
                   'Missing: Not collected',
                   'Missing: Not provided',
                   'Missing: Restricted access',
                   'null', 'NULL', 'no_data', 'None', 'nan'}

    md = pd.read_csv(metadata, sep='\t', dtype=str).set_index('#SampleID')

    acquired = False
    while not acquired:
        # not using redlock as time interval isn't that critical
        acquired = get('metadata', 'SETNX', '__load_md_lock/1') == 1
        if not acquired:
            # TODO: support verbose
            time.sleep(1)

    try:
        # subset to only the novel IDs
        represented = get('metadata', 'SMEMBERS', 'samples-represented')
        md = md.loc[set(md.index) - set(represented)]
        if len(md) == 0:
            raise ValueError("No new sample IDs found in: %s" % metadata)

        samples = md.index
        indexed_columns = md.columns
        for idx, row in md.iterrows():
            # denote what columns contain information
            columns = [c for c, i in zip(md.columns, row.values)
                       if _indexable(i, null_values)]
            key = "categories:%s" % idx

            # TODO: express metadata-categories using redis sets
            # TODO: dumps is expensive relative to just, say, '\t'.join
            put('metadata', 'SET', key, json.dumps(columns))

        for col in indexed_columns:
            bulk_set = ["%s/%s" % (idx, v) for idx, v in zip(md.index, md[col])
                        if _indexable(v, null_values)]

            payload = "category:%s/%s" % (col, '/'.join(bulk_set))
            post('metadata', 'HMSET', payload)

        payload = "samples-represented/%s" % '/'.join(md.index)
        post('metadata', 'SADD', payload)

        payload = "categories-represented/%s" % '/'.join(md.columns)
        post('metadata', 'SADD', payload)
    except:
        raise
    finally:
        # release the lock no matter what
        get('metadata', 'DEL', '__load_md_lock')

    return len(samples)


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
