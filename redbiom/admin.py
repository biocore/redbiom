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
    import redbiom._requests

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)

    post('state', 'HSET', "contexts/%s/%s" % (name, description))


def load_observations(table, context, tag=None):
    """Load observation to sample mappings.

    Parameters
    ----------
    table : biom.Table
        The BIOM table to load.
    context : str
        The context to load into.
    tag : str
        A tag to associated the samples with (e.g., a preparation ID).

    Returns
    -------
    int
        The number of samples in which observations where loaded from.

    Raises
    ------
    ValueError
        If the context to load into does not exist.
        If a samples metadata has not already been loaded.

    Redis command summary
    ---------------------
    SMEMBERS <context>:samples-represented-observations
    SADD <context>:samples:<observation_id> <sample_id> ... <sample_id>
    SADD <context>:samples-represented-observations <sample_id> ... <sample_id>
    """
    import redbiom
    import redbiom._requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)
    get = redbiom._requests.make_get(config)

    redbiom._requests.valid(context, get)

    table = _stage_for_load(table, context, get, 'observations', tag)

    samples = table.ids()[:]

    for values, id_, _ in table.iter(axis='observation', dense=False):
        observed = samples[values.indices]

        payload = "samples:%s/%s" % (id_, "/".join(observed))
        post(context, 'SADD', payload)

    payload = "samples-represented-observations/%s" % '/'.join(samples)
    post(context, 'SADD', payload)

    return len(samples)


def load_sample_data(table, context, tag=None):
    """Load nonzero sample data.

    Parameters
    ----------
    table : biom.Table
        The BIOM table to load.
    context : str
        The context to load into.
    tag : str
        A tag to associated the samples with (e.g., a preparation ID).

    Raises
    ------
    ValueError
        If the context to load into does not exist.
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

    Returns
    -------
    int
        The number of samples loaded.
    """
    import time
    import json
    import redbiom
    import redbiom._requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)
    get = redbiom._requests.make_get(config)

    redbiom._requests.valid(context, get)

    acquired = get(context, 'SETNX', '__load_table_lock/1') == 1
    while not acquired:
        # not using redlock as time interval isn't that critical
        time.sleep(1)
        acquired = get(context, 'SETNX', '__load_table_lock/1') == 1

    try:
        table = _stage_for_load(table, context, get, 'data', tag)
        samples = table.ids()[:]
        obs = table.ids(axis='observation')

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
        for values, id_, _ in table.iter(dense=False):
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

    return len(samples)


def load_sample_metadata(md, tag=None):
    """Load sample metadata.

    Parameters
    ----------
    md : pd.DataFrame
        QIIME or Qiita compatible metadata.
    tag : str, optional
        A tag associated with the information being loaded such as a
        preparation ID.

    Notes
    -----
    Values considered to be non-informative are omitted from load.

    TODO: expose a stable list of the nullables, see #19

    Returns
    -------
    int
        The number of samples loaded.

    Redis command summary
    ---------------------
    SMEMBERS metadata:samples-represented
    SET metadata:categories:<sample_id> <JSON-of-informative-columns>
    HMSET metadata:category:<column> <sample_id> <val> ... <sample_id> <val>
    SADD metadata:samples-represented <sample_id> ... <sample_id> ...
    SADD metadata:categories-represented <column> ... <column>
    """
    import json
    import redbiom
    import redbiom._requests
    import redbiom.util

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)
    put = redbiom._requests.make_put(config)
    get = redbiom._requests.make_get(config)

    null_values = redbiom.util.NULL_VALUES

    md = md.copy()
    if md.columns[0] not in ['#SampleID', 'sample_name']:
        md = md.reset_index()

    if tag is not None:
        original_ids = md[md.columns[0]][:]

        # if the metadata are tagged, they must have sample metadata already
        # loaded
        if not redbiom.util.has_sample_metadata(original_ids):
            raise ValueError("Sample metadata must be loaded first.")

        # tag the sample IDs
        md[md.columns[0]] = ['%s_%s' % (tag, i) for i in md[md.columns[0]]]

    md.set_index(md.columns[0], inplace=True)

    # subset to only the novel IDs
    represented = get('metadata', 'SMEMBERS', 'samples-represented')
    md = md.loc[set(md.index) - set(represented)]
    if len(md) == 0:
        return 0

    samples = md.index
    indexed_columns = md.columns
    for idx, row in md.iterrows():
        # denote what columns contain information
        columns = [c for c, i in zip(md.columns, row.values)
                   if _indexable(i, null_values)]
        key = "categories:%s" % idx

        # TODO: express metadata-categories using redis sets, see #18
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

    return len(samples)


def load_sample_metadata_full_search(md, tag=None):
    """Load stem -> sample associations

    Parameters
    ----------
    md : pd.DataFrame
        QIIME or Qiita compatible metadata.
    tag : str, optional
        A tag associated with the information being loaded such as a
        preparation ID.

    Notes
    -----
    Values considered to be non-informative are omitted from load.

    Returns
    -------
    int
        The number of stems based on metadata values found.
    int
        The number of stems based on the categories found.

    Redis command summary
    ---------------------
    SADD metadata:text-search:<stem> <sample-id> ... <sample-id>
    SADD metadata:category-search:<stem> <category> ... <category>
    """
    import json
    import redbiom
    import redbiom._requests
    import redbiom.util
    import pandas as pd

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)
    put = redbiom._requests.make_put(config)
    get = redbiom._requests.make_get(config)

    null_values = redbiom.util.NULL_VALUES

    md = md.copy()
    if md.columns[0] not in ['#SampleID', 'sample_name']:
        md = md.reset_index()

    if tag is not None:
        original_ids = md[md.columns[0]][:]

        # tag the sample IDs
        md[md.columns[0]] = ['%s_%s' % (tag, i) for i in md[md.columns[0]]]

    md.set_index(md.columns[0], inplace=True)

    if not redbiom.util.has_sample_metadata(set(md.index)):
        raise ValueError("Sample metadata must be loaded first.")

    # metadata value stems -> samples
    stems = redbiom.util.df_to_stems(md)
    for stem, samples in stems.items():
        payload = "text-search:%s/%s" % (stem, '/'.join(samples))
        post('metadata', 'SADD', payload)
    value_stems = len(stems)

    # category stems -> categories
    categories = [c.replace("_", " ") for c in md.columns]
    stems = redbiom.util.df_to_stems(pd.DataFrame(categories,
                                                  index=md.columns))
    for stem, cats in stems.items():
        payload = "category-search:%s/%s" % (stem, '/'.join(cats))
        post('metadata', 'SADD', payload)
    cat_stems = len(stems)

    return (value_stems, cat_stems)


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


def _stage_for_load(table, context, get, memberkey, tag=None):
    """Tag samples, reduce to only those relevant to load

    Parameters
    ----------
    table : biom.Table
        The table to operate on
    context : str
        The context to load into
    get : make_get instance
        A getter
    memberkey : str
        The redis set to check for membership in
    tag : str, optional
        The tag to apply to the samples

    Raises
    ------
    ValueError
        If a samples metadata has not already been loaded.

    Returns
    -------
    biom.Table
        A copy of the input table, filtered to only those samples which are
        novel to the context. Sample IDs reflect tag.
    """
    import redbiom.util

    if tag is None:
        tag = 'UNTAGGED'

    table = table.update_ids({i: "%s_%s" % (tag, i) for i in table.ids()},
                             inplace=False)
    samples = set(table.ids())

    represented = get(context, 'SMEMBERS',
                      'samples-represented-%s' % memberkey)
    represented = set(represented)
    to_load = samples - represented

    if not redbiom.util.has_sample_metadata(to_load):
        raise ValueError("Sample metadata must be loaded first.")

    table.filter(to_load)
    return table.filter(lambda v, i, md: v.sum() > 0, axis='observation')
