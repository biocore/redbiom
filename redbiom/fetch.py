def samples_in_context(context, unambiguous, get=None):
    """Fetch samples in a context

    Parameters
    ----------
    context : str
        The context to obtain samples from.
    unambiguous : bool
        If True, return unambiguous identifiers, if false return ambiguated
        identifiers.
    get : a make_get instance, optional
        A constructed get method.

    Returns
    -------
    set
        The set of sample identifers within a context.

    Raises
    ------
    ValueError
        If the requested context is not known.

    Redis Command Summary
    ---------------------
    SMEMBERS <context>:samples-represented
    """
    import redbiom
    import redbiom._requests
    import redbiom.util

    if get is None:
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    redbiom._requests.valid(context, get)

    obs = get(context, 'SMEMBERS', 'samples-represented')

    if not unambiguous:
        _, _, _, tagged_clean = redbiom.util.partition_samples_by_tags(obs)
        return set(tagged_clean)
    else:
        return set(obs)


def features_in_context(context, get=None):
    """Features in a context

    Parameters
    ----------
    context : str
        The context to obtain samples from.
    get : a make_get instance, optional
        A constructed get method.

    Returns
    -------
    set
        The set of features within a context.

    Raises
    ------
    ValueError
        If the requested context is not known.

    Redis Command Summary
    ---------------------
    SMEMBERS <context>:features-represented
    """
    import redbiom
    import redbiom._requests
    import redbiom.util

    if get is None:
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    redbiom._requests.valid(context, get)

    obs = get(context, 'SMEMBERS', 'features-represented')

    return set(obs)


def sample_metadata(samples, common=True, context=None, restrict_to=None,
                    tagged=False):
    """Fetch metadata for the corresponding samples

    Parameters
    ----------
    samples : iterable of str
        The samples to obtain the metadata for.
    common : bool, optional
        If True (default), only the columns of the metadata common across all
        samples is returned. If False, all columns for all samples are
        returned. If value is missing for a given column and sample, None is
        stored in its place in the resulting DataFrame.
    context : str, optional
        If provided, resolve possible ambiguities in the sample identifiers
        relative to a context.
    restrict_to : Iterable of str, optional
        Restrict the retrieval of metadata to a subset of columns. If this
        parameter is specified, it will override the use of `common`.
    tagged : bool, optional
        Retrieve tagged metadata (e.g., preparation information).

    Returns
    -------
    pandas.DataFrame
        A DataFrame indexed by the sample IDs, with the sample metadata
    dict
        ambiguous associations {sample_id: [tagged_sample_ids]}

    Raises
    ------
    KeyError
        If a key in `restrict_to` is not found.

    Redis command summary
    ---------------------
    MGET metadata:categories:<sample_id> ... metadata:categories:<sample_id>
    HMGET metadata:category:<column> <sample_id> ... <sample_id>
    """
    import json
    from collections import defaultdict
    import pandas as pd
    import redbiom
    import redbiom._requests

    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)

    untagged, _, _, tagged_clean = \
        redbiom.util.partition_samples_by_tags(samples)
    samples = untagged + tagged_clean

    # resolve ambiguities
    if context is not None:
        _, _, ambig_assoc, rbid_map = \
            redbiom.util.resolve_ambiguities(context, samples, get)

        if tagged:
            ambig_assoc = {rbid: [rbid] for rbid in rbid_map}
    else:
        ambig_assoc = {k: [k] for k in samples}

    if not ambig_assoc:
        raise ValueError("None of the samples were found in the context")

    # TODO: express metadata-categories using redis sets
    # and then this can be done with SINTER
    all_columns = []
    all_samples = []

    getter = redbiom._requests.buffered(list(ambig_assoc), 'categories',
                                        'MGET', 'metadata', get=get,
                                        buffer_size=100)
    for samples, columns_by_sample in getter:
        all_samples.extend(samples)
        for column_set in columns_by_sample:
            if column_set is not None:
                column_set = json.loads(column_set)
                all_columns.append(set(column_set))

    columns_to_get = set(all_columns[0])

    for columns in all_columns[1:]:
        if (restrict_to is not None) or (not common):
            columns_to_get = columns_to_get.union(columns)
        else:
            columns_to_get = columns_to_get.intersection(columns)

    if restrict_to is not None:
        if not set(restrict_to).issubset(columns_to_get):
            raise KeyError("The following columns were not observed: "
                           "%s" % (set(restrict_to) - set(columns_to_get)))
        columns_to_get = restrict_to

    metadata = defaultdict(dict)
    for sample in all_samples:
        for sample_ambiguity in ambig_assoc[sample]:
            metadata[sample_ambiguity]['#SampleID'] = sample_ambiguity

    for category in columns_to_get:
        key = 'category:%s' % category
        getter = redbiom._requests.buffered(iter(all_samples), None, 'HMGET',
                                            'metadata', get=get,
                                            buffer_size=100,
                                            multikey=key)

        for samples, category_values in getter:
            for sample, value in zip(samples, category_values):
                for sample_ambiguity in ambig_assoc[sample]:
                    metadata[sample_ambiguity][category] = value

    md = pd.DataFrame(metadata).T

    if context is not None:
        new_ids = []
        for i in md['#SampleID']:
            tag, id_ = i.split('_', 1)
            new_ids.append("%s.%s" % (id_, tag))
        md['#SampleID'] = new_ids

    return md, ambig_assoc


def data_from_features(context, features, exact):
    """Fetch sample data from an iterable of features.

    Parameters
    ----------
    context : str
        The name of the context to retrieve sample data from.
    features : Iterable of str
        The features of interest.
    exact : bool
        If True, only samples in which all features exist are obtained.
        Otherwise, all samples with at least one feature are obtained.

    Returns
    -------
    biom.Table
        A Table populated with the found samples.
    dict
        A map of {sample_id_in_table: original_id}. This map can be used to
        identify what samples are ambiguous based off their original IDs.
    """
    import redbiom
    import redbiom.util
    import redbiom._requests

    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)

    redbiom._requests.valid(context, get)

    # determine the samples which contain the features of interest
    samples = redbiom.util.ids_from(features, exact, 'feature', [context])

    return _biom_from_samples(context, iter(samples), get=get)


def data_from_samples(context, samples):
    """Fetch sample data from an iterable of samples.

    Paramters
    ---------
    context : str
        The name of the context to retrieve sample data from.
    samples : Iterable of str
        The samples of interest.

    Returns
    -------
    biom.Table
        A Table populated with the found samples.
    dict
        A map of {sample_id_in_table: original_id}. This map can be used to
        identify what samples are ambiguous based off their original IDs.
    """
    return _biom_from_samples(context, samples)


def _biom_from_samples(context, samples, get=None, normalize_taxonomy=None):
    """Create a BIOM table from an iterable of samples

    Parameters
    ----------
    context : str
        The context to obtain sample data from.
    samples : iterable of str
        The samples to fetch.
    get : a make_get instance, optional
        A constructed get method.
    normalize_taxonomy : list, optional
        The ranks to normalize a lineage too (e.g., [k, p, c, o, f, g, s])

    Returns
    -------
    biom.Table
        A Table populated with the found samples.
    dict
        A map of {sample_id_in_table: original_id}. This map can be used to
        identify what samples are ambiguous based off their original IDs.

    Redis command summary
    ---------------------
    HMGET <context>:feature-index-inverted
    EVALSHA <fetch-sample-sha1> 0 context <redbiom-id>
    """
    from operator import itemgetter
    import scipy.sparse as ss
    import biom
    import redbiom.admin
    import redbiom._requests
    import redbiom.util
    import redbiom
    config = redbiom.get_config()

    if get is None:
        get = redbiom._requests.make_get(config)

    se = redbiom._requests.make_script_exec(config)

    redbiom._requests.valid(context, get)

    samples = list(samples)  # unroll iterator if necessary

    # resolve ambiguities
    stable_ids, unobserved, ambig_assoc, rimap = \
        redbiom.util.resolve_ambiguities(context, samples, get)

    table_data = []
    unique_indices = set()
    fetch_sample = redbiom.admin.ScriptManager.get('fetch-sample')
    for id_ in rimap:
        # 0 -> we're passing 0 keys, and instead using ARGV
        data = se(fetch_sample, 0, context, id_)
        table_data.append((id_, data))
        unique_indices.update(data)

    # construct a mapping of
    # {feature ID : index position in the BIOM table}
    unique_indices_map = {observed: index
                          for index, observed in enumerate(unique_indices)}

    # pull out the feature and sample IDs in the desired ordering
    obs_ids = [id_ for id_, _ in sorted(unique_indices_map.items(),
                                        key=itemgetter(1))]
    sample_ids = [id_ for id_, _ in table_data]

    # fill in the matrix
    mat = ss.lil_matrix((len(unique_indices), len(table_data)))
    for col, (sample, col_data) in enumerate(table_data):
        # since this isn't dense, hopefully roworder doesn't hose us
        for obs_id, value in col_data.items():
            mat[unique_indices_map[obs_id], col] = value

    lineages = taxon_ancestors(context, obs_ids, get,
                               normalize=normalize_taxonomy)

    if lineages is not None:
        obs_md = [{'taxonomy': lineage} for lineage in lineages]
    else:
        obs_md = None

    table = biom.Table(mat, obs_ids, sample_ids, obs_md)
    table.update_ids(rimap)

    return table, ambig_assoc


def taxon_ancestors(context, ids, get=None, normalize=None):
    """Fetch the taxonomy information for a set of IDs

    Parameters
    ----------
    context : str
        The context to operate in
    ids : list or tuple of str
        The IDs to retreive
    get : function, optional
        A get method
    normalize : list, optional
        The ranks to normalize a lineage too (e.g., [k, p, c, o, f, g, s])

    Returns
    -------
    list of list
        The lineage information for each ID in order with ids

    Redis Command Summary
    ---------------------
    HMGET <context>:taxonomy-parents <child> ... <child>
    """
    from future.moves.itertools import zip_longest
    import redbiom._requests

    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    hmgetter = redbiom._requests.buffered
    remapped_bulk = hmgetter(iter(ids), None, 'HMGET', context,
                             get=get, buffer_size=100,
                             multikey='feature-index')

    # map the feature identifier to an internal ID
    # if an internal ID does not exist, keep the provided ID
    # the provided ID is kept in the event a taxon name such as
    # p__Firmicutes is provided
    remapped = {name: id_ if id_ is not None else name
                for names, idx in remapped_bulk
                for name, id_ in zip(names, idx)}

    # bulk gather the taxonomy information for all the tips and their parents
    to_get = list(remapped.values())
    child_parent = {}

    while to_get:
        key = 'taxonomy-parents'
        getter = hmgetter(iter(to_get), None, 'HMGET',
                          context, get=get,
                          buffer_size=100, multikey=key)

        new_to_get = set()
        for block in getter:
            for child, parent in zip(*block):
                if parent is None:
                    continue

                child_parent[child] = parent
                new_to_get.add(parent)
        to_get = new_to_get

    if not child_parent:
        return None

    # form lineages from the child -> parent relationships
    lineages = []
    for id_ in ids:
        lineage = []
        current = remapped[id_]
        while current is not None:
            current = child_parent.get(current)
            if current is not None:
                lineage.append(current)
        lineage = lineage[::-1]

        # normalize if necessary to greengenes like strings
        if normalize is not None:
            lineage = [l if l else "%s__" % r
                       for l, r in zip_longest(lineage, normalize,
                                               fillvalue=False)]
        lineages.append(lineage)

    return lineages


def taxon_descendents(context, taxon, get=None):
    """Get tips associated with a taxon

    Parameters
    ----------
    context : str
        The context to operate in
    taxon : str
        The taxon to search for
    get : function, optional
        A get method

    Returns
    -------
    set
        The set of feature IDs found

    Redis Command Summary
    ---------------------
    SMEMBERS <context>:taxonomy-children:<taxon>
    """
    import redbiom._requests

    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)
    hmgetter = redbiom._requests.buffered

    to_get = [(None, taxon), ]
    to_keep = set()
    while to_get:
        new_to_get = []
        for parent, taxon in to_get:
            if taxon == 'has-terminal':
                tips = get(context, 'SMEMBERS', 'terminal-of:%s' % parent)
                to_keep.update(set(tips))
            else:
                gotten = get(context, 'SMEMBERS',
                             'taxonomy-children:%s' % taxon)
                new_to_get.extend([(taxon, child) for child in gotten])
        to_get = new_to_get

    remapped_bulk = hmgetter(to_keep, None, 'HMGET', context,
                             get=get, buffer_size=100,
                             multikey='feature-index-inverted')

    remapped = {name
                for idx, names in remapped_bulk
                for id_, name in zip(idx, names)}

    if None in remapped:
        # this should not happen and is a consistency check
        raise ValueError("An unassociated index has been found")

    return remapped


def category_sample_values(category, samples=None):
    """Obtain the samples and their corresponding category values

    Parameters
    ----------
    category : str
        A metadata column of interest.
    samples : Iterable of str, optional
        If provided, only the specified samples and their values are obtained.

    Returns
    -------
    pandas.Series
        A Series indexed by the Sample ID and valued by the metadata value for
        that sample for the specified category.

    Redis command summary
    ---------------------
    HGETALL metadata:category:<category>
    HMGET metadata:category:<category> <sample_id> ... <sample_id>
    """
    import redbiom
    import redbiom._requests
    import pandas as pd

    get = redbiom._requests.make_get(redbiom.get_config())

    key = 'category:%s' % category
    if samples is None:
        keys_vals = list(get('metadata', 'HGETALL', key).items())
    else:
        untagged, _, _, tagged_clean = \
            redbiom.util.partition_samples_by_tags(samples)
        samples = untagged + tagged_clean
        getter = redbiom._requests.buffered(iter(samples), None, 'HMGET',
                                            'metadata', get=get,
                                            buffer_size=100, multikey=key)

        # there is probably some niftier method than this.
        keys_vals = [(sample, obs_val) for idx, vals in getter
                     for sample, obs_val in zip(idx, vals)]

    index = (v[0] for v in keys_vals)
    data = (v[1] for v in keys_vals)
    return pd.Series(data=data, index=index)


def sample_counts_per_category(categories=None):
    """Get the number of samples with usable metadata per category

    Paramaters
    ----------
    categories : list of str, optional
        The specific categories to summarize. If None, then summarize
        all of the categories in the database.

    Returns
    -------
    pandas.Series
        A series keyed by the category and valued by the number of samples
        which have metadata for that category.

    Redis command summary
    ---------------------
    SMEMBERS metadata:categories-represented
    HLEN metadata:category:<category>
    """
    import redbiom
    import redbiom._requests
    import pandas as pd

    get = redbiom._requests.make_get(redbiom.get_config())

    if categories is None:
        categories = list(get('metadata', 'SMEMBERS',
                          'categories-represented'))
    results = []
    for category in categories:
        key = 'category:%s' % category
        results.append(int(get('metadata', 'HLEN', key)))

    return pd.Series(results, index=categories)


def metadata(where=None, tag=None, restrict_to=None):
    """Find samples from metadata

    Parameters
    ----------
    where : str, optional
        SQLite WHERE clause specifying criteria IDs must meet to be
        included in the results. All IDs are included by default.
    tag : str, optional
        A tag specific search. Defaults to sample metadata.
    restrict_to : list of str
        Restrict the retrieval of metadata to a subset of columns.

    Raises
    ------
    KeyError
        If a `restrict_to` column does not appear to be valid
    ValueError
        `restrict_to` must be specified

    Returns
    -------
    list
        A list of sample IDs

    Redis command summary
    ---------------------
    MGET metadata:categories:<sample_id> ... metadata:categories:<sample_id>
    HMGET metadata:category:<column> <sample_id> ... <sample_id>
    """
    import json
    from collections import defaultdict
    import pandas as pd
    import redbiom
    import redbiom._requests

    if restrict_to is None:
        raise ValueError("restrict_to must be set")

    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)

    categories = set(get('metadata', 'SMEMBERS', 'categories-represented'))
    if restrict_to is not None:
        if not set(restrict_to).issubset(categories):
            diff = set(restrict_to) - categories
            raise KeyError("The following requested categories are not "
                           "not found: %s" % ','.join(diff))
        else:
            categories = set(restrict_to)

    samples = set(get('metadata', 'SMEMBERS', 'samples-represented'))
    if tag is None:
        samples = {s for s in samples if '_' not in s}
    else:
        samples = {s for s in samples if s.startswith('%s_' % tag)}

    getter = redbiom._requests.buffered(samples, 'categories',
                                        'MGET', 'metadata', get=get,
                                        buffer_size=100)

    samples_to_get = []
    for chunk in getter:
        for sample, column_set in zip(*chunk):
            if sample in samples:
                # only keep the sample if it has a category of interest
                if column_set is not None:
                    column_set = set(json.loads(column_set))
                    if column_set.intersection(categories):
                        samples_to_get.append(sample)

    metadata = defaultdict(dict)
    for sample in samples_to_get:
        metadata[sample]['#SampleID'] = sample

    for category in categories:
        key = 'category:%s' % category
        getter = redbiom._requests.buffered(iter(samples_to_get), None,
                                            'HMGET',
                                            'metadata', get=get,
                                            buffer_size=100,
                                            multikey=key)

        for chunk in getter:
            for sample, value in zip(*chunk):
                metadata[sample][category] = value

    md = pd.DataFrame(metadata).T

    if len(md.columns) == 0:
        return set()
    else:
        md = redbiom.metadata.Metadata(md.set_index('#SampleID'))
        return md.ids(where=where)
