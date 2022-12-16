from urllib.parse import quote_plus as _quote_plus
from math import ceil
import numpy as np


def quote_plus(s):
    return _quote_plus(s).replace('.', '%2E')


class ScriptManager:
    """Static singleton for managing Lua scripts in the Redis backend"""
    # derived from http://stackoverflow.com/a/43900922/19741
    _scripts = {'get-index': """
                    local indices = {}
                    local kid = nil

                    -- for each index and identifier (like python's enumerate)
                    for position, name in ipairs(ARGV) do
                        kid = redis.call('HGET', KEYS[1], name)

                        -- if an identifier was not observed, add it
                        if not kid then
                          kid = redis.call('HINCRBY',
                                           KEYS[1],
                                           'current_id', 1) - 1
                          redis.call('HSET', KEYS[1], name, kid)
                          redis.call('HSET', KEYS[1] .. '-inverted', kid, name)
                        end

                        -- store store the mapping for return
                        indices[position] = tonumber(kid)
                    end
                    return cjson.encode(indices)""",
                'load-data': """
                    -- Redis has a compile time stack limit for Lua calls
                    -- so rather than recompiling with an arbitrary limit,
                    -- we're going to instead chunk calls where there are a
                    -- large number of arguments. The default is 8000 for the
                    -- stack size, so we'll use 7900 to be close without
                    -- going over
                    -- https://stackoverflow.com/a/39959618/19741
                    local call_in_chunks = function (command, key, args)
                        local step = 7900
                        for i = 1, #args, step do
                            redis.call(command,
                                       key,
                                       unpack(args,
                                              i,
                                              math.min(i + step - 1, #args)))
                        end
                    end

                    -- Lua does not have a natural split, for various reasons
                    -- outlined in the URL below, so we need to do this
                    -- manually. We'll split on "|" which should be safe
                    -- as the values sent are only ever expected to be integers
                    -- http://lua-users.org/wiki/SplitJoin
                    for idx, arg in ipairs(ARGV) do
                        local items = {}
                        for item in string.gmatch(arg, "([^|]+)") do
                            table.insert(items, item)
                        end
                        call_in_chunks('LPUSH', KEYS[idx], items)
                    end
                    return redis.status_reply("OK")""",
                'fetch-feature': """
                    local context = ARGV[1]
                    local key = ARGV[2]
                    local result = {}
                    local formedkey = context .. ':' .. 'feature' .. ':' .. key

                    local items = redis.call('LRANGE',
                                             formedkey,
                                             '0', '-1')

                    -- adapted from https://gist.github.com/klovadis/5170446
                    local resultkey
                    local ii = context .. ':' .. 'sample' .. '-index-inverted'
                    for idx, v in ipairs(items) do
                        if idx % 2 == 1 then
                            -- it is likely possible to issue a HMGET
                            resultkey = redis.call('HGET', ii, v)
                        else
                            result[resultkey] = tonumber(v)
                        end
                    end

                    return cjson.encode(result)""",
                'fetch-sample': """
                    local context = ARGV[1]
                    local key = ARGV[2]
                    local result = {}
                    local formedkey = context .. ':' .. 'sample' .. ':' .. key

                    local items = redis.call('LRANGE',
                                             formedkey,
                                             '0', '-1')

                    -- adapted from https://gist.github.com/klovadis/5170446
                    local resultkey
                    local ii = context .. ':' .. 'feature' .. '-index-inverted'
                    for idx, v in ipairs(items) do
                        if idx % 2 == 1 then
                            -- it is likely possible to issue a HMGET
                            resultkey = redis.call('HGET', ii, v)
                        else
                            result[resultkey] = tonumber(v)
                        end
                    end

                    return cjson.encode(result)"""}
    _admin_scripts = ('get-index', 'load-data')
    _cache = {}

    @staticmethod
    def load_scripts(read_only=True):
        """Load scripts into Redis

        Parameters
        ----------
        read_only : bool, optional
            If True, only load read-only scripts. If False, load writable
            scripts
        """
        import redbiom
        import redbiom._requests
        import hashlib

        config = redbiom.get_config()
        s = redbiom._requests.get_session()
        post = redbiom._requests.make_post(config)
        get = redbiom._requests.make_get(config)

        for name, script in ScriptManager._scripts.items():
            if read_only and name in ScriptManager._admin_scripts:
                continue

            sha1 = hashlib.sha1(script.encode('ascii')).hexdigest()
            keypair = 'scripts/%s/%s' % (name, sha1)

            # load the script
            s.put(config['hostname'] + '/SCRIPT/LOAD', data=script)

            # create a mapping
            post('state', 'HSET', keypair)

            # verify we've correctly computed the hash
            obs = get('state', 'HGET', 'scripts/%s' % name)
            assert obs == sha1

    @staticmethod
    def get(name):
        """Retreive the SHA1 of a script

        Parameters
        ----------
        name : str
            The name of the script to fetch

        Raises
        ------
        ValueError
            If the script name is not recognized
        """
        if name in ScriptManager._cache:
            return ScriptManager._cache[name]

        import redbiom
        import redbiom._requests
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

        sha = get('state', 'HGET', 'scripts/%s' % name)
        if sha is None:
            raise ValueError('Unknown script')

        ScriptManager._cache[name] = sha

        return sha

    @staticmethod
    def drop_scripts():
        """Flush the loaded scripts in the redis database"""
        import redbiom
        import redbiom._requests
        config = redbiom.get_config()
        s = redbiom._requests.get_session()
        s.get(config['hostname'] + '/SCRIPT/FLUSH')
        s.get(config['hostname'] + '/DEL/state:scripts')
        ScriptManager._cache = {}


def create_timestamp():
    """Create a new timestamp in the database

    Notes
    -----
    Time is represented as "%d.%b.%Y" (e.g., 25.Jul.2019).

    Timestamps are pushed into an array such that index 0 is the latest
    timestamp. A reasonable interpretation of this field, and the use of
    this method, is to obtain the timestamps of when the database was
    last updated.

    Redis command summary
    ---------------------
    LPUSH state:timestamps <current_time>
    """
    import redbiom
    import redbiom._requests
    import datetime
    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)
    fmt = datetime.datetime.now().strftime("%d.%b.%Y")
    post('state', 'LPUSH', 'timestamps/%s' % fmt)


def get_timestamps():
    """Obtain the stored timestamps

    Redis command summary
    ---------------------
    LRANGE state:timestamps 0 -1
    """
    import redbiom
    import redbiom._requests
    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)
    return get('state', 'LRANGE', 'timestamps/0/-1')


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
    HSET <context>:state db-version <current-db-version>
    """
    import redbiom
    import redbiom._requests

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)
    post('state', 'HSET', "contexts/%s/%s" % (name, description))
    post(name, 'HSET', "state/db-version/%s" % redbiom.__db_version__)
    ScriptManager.load_scripts()


def _load_axis_data(table, ids, opposite_ids, opposite_id_index, axis_label,
                    context, batchsize):
    """Manage the loading of data for a particular axis

    Parameters
    ----------
    table : biom.Table
        The table to obtain data from
    ids : iterable of str
        The IDs to obtain data for
    opposite_ids : iterable of str
        The IDs of the opposite axis in the table
    opposite_id_index : dict
        The index which maps an opposite ID to the index value within
        the Redis database for the identifier
    axis_label : str
        The biom.Table axis label of ids
    context : str
        The context to load the data into
    batchsize : int
        The number of identifiers to group into a single request

    Notes
    -----
    This method only supports count data.

    Data are loaded through the "load-data" Lua script managed in the
    ScriptsManager. This method in effect packs the data into a structure
    compatible with Webdis, and the EVALSHA command structure of Redis. The
    "load-data" script then iterates over the "KEYS" and "ARGV"s, parsing
    the respective entries into values that can be directly loaded.

    Redis command summary
    ---------------------
    EVALSHA <load-data-sha1> N <context>:<axis_label>:<id> ... <packeddata> ...

    Note that "N" refers to the number of "KEYS". The "load-data" Lua script
    assumes that there are "N" "KEYS" as well as "N" "ARGV"s. For the call,
    "KEYS" are the prefixed identifiers (e.g., "<context>:<axis_label>:<id>")
    and "ARGV" are the "packeddata". "KEYS" and "ARGV" are expected to be in
    index order with each other.
    """
    import redbiom
    import redbiom._requests
    if axis_label == 'feature':
        axis = 'observation'
    elif axis_label == 'sample':
        axis = 'sample'
    else:
        raise ValueError("%s is unrecognized as an axis" % axis)

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)
    loader_sha = ScriptManager.get('load-data')

    # partition our IDs into smaller batches
    splits = max(1, ceil(len(ids) / batchsize))
    for batch in np.array_split(ids, splits):
        keys = []
        argv = []

        # pack the id specific data into a format the Lua logic expects
        for id_ in batch:
            values = table.data(id_, axis=axis, dense=False)
            if not np.allclose(values.data - np.round(values.data, 1), 0.0):
                raise ValueError("Data do not appear to be counts")

            int_values = values.astype(int)
            remapped = [opposite_id_index[i]
                        for i in opposite_ids[values.indices]]

            packed = '|'.join(["%d|%d" % (v, i)
                               for i, v in zip(remapped, int_values.data)])

            keys.append(f"{context}:{axis_label}:{id_}")
            argv.append(packed)

        nkeys = str(len(keys))

        # load the count data
        payload = [loader_sha, nkeys] + keys + argv
        post(None, 'EVALSHA', '/'.join(payload))

        # note which identifiers are represented
        payload = f"{axis_label}s-represented/%s" % '/'.join(batch)
        post(context, 'SADD', payload, verbose=False)


def load_sample_data(table, context, tag=None, redis_protocol=False,
                     batchsize=1000):
    """Load nonzero sample data.

    Parameters
    ----------
    table : biom.Table
        The BIOM table to load.
    context : str
        The context to load into.
    tag : str
        A tag to associated the samples with (e.g., a preparation ID).
    redis_protocol : bool, optional
        Generate commands for bulk load instead of HTTP requests.
    batchsize : int, optional
        The number of samples or features to load at once

    Raises
    ------
    ValueError
        If the context to load into does not exist.
        If a samples metadata has not already been loaded.
        If a table is empty.

    Notes
    -----
    This method does not support non count data.

    The feature IDs are remapped into an integer space to reduce memory
    consumption as sOTUs are large. The index is maintained in Redis under
    <context>:feature-index and <context>:feature-index-inverted.

    The data are stored per sample with keys of the form "data:<sample_id>".
    The string stored is tab delimited, where the even indices (i.e .0, 2, 4,
    etc) correspond to the unique index value for an feature ID, and the
    odd indices correspond to the counts associated with the sample/feature
    combination.

    Redis command summary
    ---------------------
    EVALSHA <get-index-sha1> 1 <context>:feature-index <feature_id>
    EVALSHA <get-index-sha1> 1 <context>:sample-index <redbiom_id>
    LPUSH <context>:samples:<redbiom_id> <count> <feature_id> ...
    LPUSH <context>:features:<redbiom_id> <count> <redbiom_id> ...
    SADD <context>:samples-represented <redbiom_id> ... <redbiom_id>
    SADD <context>:features-represented <feature_id> ... <feature_id>

    Returns
    -------
    int
        The number of samples loaded.
    """
    import redbiom
    import redbiom._requests
    import redbiom.util

    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)
    post = redbiom._requests.make_post(config)

    redbiom._requests.valid(context, get)

    table = _stage_for_load(table, context, get, tag)
    samples = table.ids()[:]
    obs = table.ids(axis='observation')[:]

    obs_index = {i: j for i, j in zip(obs, get_index(context, obs, 'feature'))}
    samp_index = {i: j for i, j in
                  zip(samples, get_index(context, samples, 'sample'))}

    _load_axis_data(table, samples, obs, obs_index, 'sample', context,
                    batchsize=10)
    _load_axis_data(table, obs, samples, samp_index, 'feature', context,
                    batchsize=500)

    # load up taxonomy
    taxonomy = _metadata_to_taxonomy_tree(table.ids(axis='observation'),
                                          table.metadata(axis='observation'))
    if taxonomy is not None:
        post(context, 'HSET', "state/has-taxonomy/1")
        hmgetter = redbiom._requests.buffered

        tip_names = {n.name: n for n in taxonomy.tips()}
        ids_ = hmgetter(tip_names, None, 'HMGET', context,
                        get=get, buffer_size=100,
                        multikey='feature-index')

        for blk in ids_:
            for entity, idx in zip(*blk):
                tip_names[entity].name = idx

        for node in taxonomy.postorder(include_self=False):
            if not node.is_tip():
                # define node -> children relationships
                pack = []
                terminal_pack = []
                for c in node.children:
                    if c.is_tip():
                        pack.append('has-terminal')
                        terminal_pack.append(c.name)
                    else:
                        pack.append(c.name)

                packed = '/'.join(pack)
                post(context, 'SADD', 'taxonomy-children:%s/%s' % (node.name,
                                                                   packed))

                # define children -> parent relationships
                pack = ['%s/%s' % (c.name, node.name)
                        for c in node.children]
                post(context, 'HMSET', 'taxonomy-parents/%s' % '/'.join(pack))

                if terminal_pack:
                    id_pack = '/'.join(terminal_pack)
                    post(context, 'SADD', 'terminal-of:%s/%s' % (node.name,
                                                                 id_pack))

    return len(samples)


def _metadata_to_taxonomy_tree(ids, metadata):
    """Cast the taxonomy into a tree

    Parameters
    ----------
    ids : list of str
        The feature IDs
    metadata : list of dict
        Feature metadata in index order with the ids.

    Notes
    -----
    Children of unclassified nodes (e.g., s__) are migrated to the parent
    so that no unclassified nodes exist in the tree.

    Returns
    -------
    skbio.TreeNode
        A hierarchy of the taxonomy.
    """
    if metadata is None:
        return None

    import skbio
    t = skbio.TreeNode.from_taxonomy([(i, m['taxonomy'])
                                      for i, m in zip(ids, metadata)])

    for node in list(t.postorder()):
        if node.is_tip():
            continue
        if node.is_root():
            continue

        if len(node.name) == 3 and node.name.endswith('__'):
            parent = node.parent
            parent.extend(node.children)
            parent.remove(node)

    return t


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
    md = md.loc[list(set(md.index) - set(represented))]
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
        bulk_set = ["%s/%s" % (idx, quote_plus(str(v)))
                    for idx, v in zip(md.index, md[col])
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
    import redbiom
    import redbiom._requests
    import redbiom.util
    import pandas as pd

    config = redbiom.get_config()
    post = redbiom._requests.make_post(config)

    md = md.copy()
    if md.columns[0] not in ['#SampleID', 'sample_name']:
        md = md.reset_index()

    if tag is not None:
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
    """Returns true if the value appears to be something that storable"""
    return value not in nullables


class AlreadyLoaded(ValueError):
    pass


def _stage_for_load(table, context, get, tag=None):
    """Tag samples, reduce to only those relevant to load

    Parameters
    ----------
    table : biom.Table
        The table to operate on
    context : str
        The context to load into
    get : make_get instance
        A getter
    tag : str, optional
        The tag to apply to the samples

    Raises
    ------
    ValueError
        If a samples metadata has not already been loaded.
    ValueError
        If the table is empty.
    AlreadyLoaded
        If the table appears to already be loaded.

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

    if not samples:
        raise ValueError("The table is empty.")

    represented = get(context, 'SMEMBERS', 'samples-represented')
    represented = set(represented)
    to_load = samples - represented

    if not to_load and samples:
        raise AlreadyLoaded("The table appears to already be loaded.")

    if not redbiom.util.has_sample_metadata(to_load):
        raise ValueError("Sample metadata must be loaded first.")

    table.filter(to_load)
    return table.filter(lambda v, i, md: v.sum() > 0, axis='observation')


def get_index(context, keys, axis, batchsize=100):
    """Get a unique integer value for a key within a context

    Parameters
    ----------
    context : str
        The context to operate in
    keys : list or tuple of str
        The keys to get a unique index for
    axis : str
        Either feature or sample
    batchsize : int, optional
        The number of IDs to query at once

    Notes
    -----
    This method is an atomic equivalent of:

        def get_or_set(d, item):
            if item not in d:
                d[item] = len(d)
            return d[item]

    Returns
    -------
    tuple of int
        The unique integer indices within the context for the keys. This is
        returned in index order with keys.
    """
    import redbiom
    import json
    import redbiom._requests

    config = redbiom.get_config()

    post = redbiom._requests.make_post(config)
    indexer_sha = ScriptManager.get('get-index')
    context_axis = "%s:%s-index" % (context, axis)

    indices = []
    splits = max(1, ceil(len(keys) / batchsize))
    for batch in np.array_split(keys, splits):
        # the redis EVALSHA command structure requires specifying how many keys
        # there are, which is always 1 in this case.
        nkeys = '1'
        payload = [indexer_sha, nkeys, context_axis] + list(batch)
        data = json.loads(post(None, 'EVALSHA', '/'.join(payload)))
        indices.extend(data)

    return indices
