def sample_metadata(samples, common=True, context=None):
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

    Returns
    -------
    pandas.DataFrame
        A DataFrame indexed by the sample IDs, with the sample metadata

    Redis command summary
    ---------------------
    MGET metadata:categories:<sample_id> ... metadata:categories:<sample_id>
    HMGET metadata:category:<column> <sample_id> ... <sample_id>
    """
    import json
    from collections import defaultdict
    import pandas as pd
    import redbiom
    import redbiom.requests

    config = redbiom.get_config()
    get = redbiom.requests.make_get(config)

    untagged, _, _, tagged_clean = \
        redbiom.util.partition_samples_by_tags(samples)
    samples = untagged + tagged_clean

    # resolve ambiguities
    if context is not None:
        unambig, stable_ids, ambig_assoc, rimap = \
            redbiom.util.resolve_ambiguities(context, samples, get)
    else:
        unambig = list(samples)
        stable_ids = {k: k for k in samples}
        ambig_assoc = {k: [k] for k in samples}
        rimap = stable_ids

    # TODO: express metadata-categories using redis sets
    # and then this can be done with SINTER
    all_columns = []
    all_samples = []

    getter = redbiom.requests.buffered(list(ambig_assoc), 'categories', 'MGET',
                                       'metadata', get=get, buffer_size=100)
    for samples, columns_by_sample in getter:
        all_samples.extend(samples)
        for column_set in columns_by_sample:
            if column_set is not None:
                column_set = json.loads(column_set)
                all_columns.append(set(column_set))

    columns_to_get = set(all_columns[0])
    for columns in all_columns[1:]:
        if common:
            columns_to_get = columns_to_get.intersection(columns)
        else:
            columns_to_get = columns_to_get.union(columns)

    metadata = defaultdict(dict)
    for sample in all_samples:
        for sample_ambiguity in ambig_assoc[sample]:
            metadata[sample_ambiguity]['#SampleID'] = sample_ambiguity

    for category in columns_to_get:
        key = 'category:%s' % category
        getter = redbiom.requests.buffered(iter(all_samples), None, 'HMGET',
                                           'metadata', get=get,
                                           buffer_size=100,
                                           multikey=key)

        for samples, category_values in getter:
            for sample, value in zip(samples, category_values):
                for sample_ambiguity in ambig_assoc[sample]:
                    metadata[sample_ambiguity][category] = value

    md = pd.DataFrame(metadata).T

    return md, ambig_assoc


def data_from_observations(context, observations, exact):
    """Fetch sample data from an iterable of observations.

    Parameters
    ----------
    context : str
        The name of the context to retrieve sample data from.
    observations : Iterable of str
        The observations of interest.
    exact : bool
        If True, only samples in which all observations exist are obtained.
        Otherwise, all samples with at least one observation are obtained.

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
    import redbiom.requests

    config = redbiom.get_config()
    get = redbiom.requests.make_get(config)

    redbiom.requests.valid(context, get)

    # determine the samples which contain the observations of interest
    samples = redbiom.util.samples_from_observations(observations, exact,
                                                     context, get=get)

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


def _biom_from_samples(context, samples, get=None):
    """Create a BIOM table from an iterable of samples

    Parameters
    ----------
    context : str
        The context to obtain sample data from.
    samples : iterable of str
        The samples to fetch.
    get : a make_get instance, optional
        A constructed get method.

    Returns
    -------
    biom.Table
        A Table populated with the found samples.
    dict
        A map of {sample_id_in_table: original_id}. This map can be used to
        identify what samples are ambiguous based off their original IDs.

    Redis command summary
    ---------------------
    GET <context>:__observation_index
    MGET <context>:data:<sample_id> ... <context>:data:<sample_id>
    """
    import json
    from operator import itemgetter
    import scipy.sparse as ss
    import biom
    import redbiom.requests
    import redbiom.util

    # TODO: centralize this as it's boilerplate
    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

    redbiom.requests.valid(context, get)

    samples = list(samples)  # unroll iterator if necessary

    # resolve ambiguities
    stable_ids, unobserved, ambig_assoc, rimap = \
            redbiom.util.resolve_ambiguities(context, samples, get)

    # pull out the observation index so the IDs can be remapped
    obs_index = json.loads(get(context, 'GET', '__observation_index'))

    # redis contains {observation ID -> internal ID}, and we need
    # {internal ID -> observation ID}
    inverted_obs_index = {v: k for k, v in obs_index.items()}

    # pull out the per-sample data
    table_data = []
    unique_indices = set()
    getter = redbiom.requests.buffered(list(rimap), 'data', 'MGET', context,
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

    table = biom.Table(mat, obs_ids, sample_ids)
    table.update_ids(rimap)

    return table, ambig_assoc


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
    import redbiom.requests
    import pandas as pd

    get = redbiom.requests.make_get(redbiom.get_config())

    key = 'category:%s' % category
    if samples is None:
        keys_vals = list(get('metadata', 'HGETALL', key).items())
    else:
        untagged, _, _, tagged_clean = \
            redbiom.util.partition_samples_by_tags(samples)
        samples = untagged + tagged_clean
        getter = redbiom.requests.buffered(iter(samples), None, 'HMGET',
                                           'metadata', get=get,
                                           buffer_size=100, multikey=key)

        # there is probably some niftier method than this.
        keys_vals = [(sample, obs_val) for idx, vals in getter
                     for sample, obs_val in zip(idx, vals)]

    index = (v[0] for v in keys_vals)
    data = (v[1] for v in keys_vals)
    return pd.Series(data=data, index=index)


def sample_counts_per_category():
    """Get the number of samples with usable metadata per category

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
    import redbiom.requests
    import pandas as pd

    get = redbiom.requests.make_get(redbiom.get_config())

    categories = list(get('metadata', 'SMEMBERS', 'categories-represented'))
    results = []
    for category in categories:
        key = 'category:%s' % category
        results.append(int(get('metadata', 'HLEN', key)))

    return pd.Series(results, index=categories)
