class ScriptManager:
    """Static singleton for managing Lua scripts in the Redis backend"""
    # derived from http://stackoverflow.com/a/43900922/19741
    _scripts = {'get-index': """
                    local kid = redis.call('HGET', KEYS[1], ARGV[1])
                    if not kid then
                      kid = redis.call('HINCRBY', KEYS[1], 'current_id', 1) - 1
                      redis.call('HSET', KEYS[1], ARGV[1], kid)
                      redis.call('HSET', KEYS[1] .. '-inverted', kid, ARGV[1])
                    end
                    return kid""",
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
    _admin_scripts = ('get-index', )
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


def load_sample_data(table, context, tag=None, redis_protocol=False):
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
    post = redbiom._requests.make_post(config, redis_protocol=redis_protocol)
    get = redbiom._requests.make_get(config)

    redbiom._requests.valid(context, get)

    table = _stage_for_load(table, context, get, tag)
    samples = table.ids()[:]
    obs = table.ids(axis='observation')

    if len(table.ids()) == 0:
        raise ValueError("The table is empty.")

    obs_index = {}
    for id_ in obs:
        obs_index[id_] = get_index(context, id_, 'feature')

    samp_index = {}
    for id_ in samples:
        samp_index[id_] = get_index(context, id_, 'sample')

    # load up per-sample
    for values, id_, _ in table.iter(dense=False):
        int_values = values.astype(int)
        remapped = [obs_index[i] for i in obs[values.indices]]

        packed = '/'.join(["%d/%s" % (v, i)
                           for i, v in zip(remapped,
                                           int_values.data)])
        post(context, 'LPUSH', 'sample:%s/%s' % (id_, packed))

    payload = "samples-represented/%s" % '/'.join(samples)
    post(context, 'SADD', payload)

    # load up per-observation
    for values, id_, md in table.iter(axis='observation', dense=False):
        int_values = values.astype(int)
        remapped = [samp_index[i] for i in samples[values.indices]]

        packed = '/'.join(["%d/%s" % (v, i)
                           for i, v in zip(remapped,
                                           int_values.data)])
        post(context, 'LPUSH', 'feature:%s/%s' % (id_, packed))

    payload = "features-represented/%s" % '/'.join(obs)
    post(context, 'SADD', payload)

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

    represented = get(context, 'SMEMBERS', 'samples-represented')
    represented = set(represented)
    to_load = samples - represented

    if not redbiom.util.has_sample_metadata(to_load):
        raise ValueError("Sample metadata must be loaded first.")

    table.filter(to_load)
    return table.filter(lambda v, i, md: v.sum() > 0, axis='observation')


def get_index(context, key, axis):
    """Get a unique integer value for a key within a context

    Parameters
    ----------
    context : str
        The context to operate in
    key : str
        The key to get a unique index for
    axis : str
        Either feature or sample

    Notes
    -----
    This method is an atomic equivalent of:

        def get_or_set(d, item):
            if item not in d:
                d[item] = len(d)
            return d[item]

    Returns
    -------
    int
        A unique integer index within the context for the key
    """
    import redbiom
    import redbiom._requests

    config = redbiom.get_config()

    # we need to issue the request directly as the command structure is
    # rather different than other commands
    s = redbiom._requests.get_session()
    sha = ScriptManager.get('get-index')
    url = '/'.join([config['hostname'], 'EVALSHA', sha,
                    '1', "%s:%s-index" % (context, axis), key])
    req = s.get(url)

    if req.status_code != 200:
        raise ValueError("Unable to obtain index; %d; %s" % (req.status_code,
                                                             req.content))

    return int(req.json()['EVALSHA'])
