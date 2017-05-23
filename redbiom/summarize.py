def contexts(detail=True):
    """Obtain the name and description of known contexts

    Parameters
    ----------
    detail : bool, optional
        If True, obtain additional context detail.

    Returns
    -------
    DataFrame
        Containing context information.

    Redis command summary
    ---------------------
    HGETALL state:contexts
    SCARD <context>:samples-represented
    SCARD <context>:features-represented
    """
    import pandas as pd
    import redbiom
    import redbiom._requests
    get = redbiom._requests.make_get(redbiom.get_config())

    if not detail:
        contexts = get('state', 'HKEYS', 'contexts')
        return pd.DataFrame(contexts, columns=['ContextName'])
    else:
        contexts = get('state', 'HGETALL', 'contexts')

        result = []
        for name, desc in contexts.items():
            ctx_n_samp = get(name, 'SCARD', 'samples-represented')
            ctx_n_feat = get(name, 'SCARD', 'features-represented')

            result.append((name, int(ctx_n_samp), int(ctx_n_feat), desc))

        return pd.DataFrame(result, columns=['ContextName', 'SamplesWithData',
                                             'FeaturesWithData',
                                             'Description'])


def category_from_features(context, category, features, exact):
    """Summarize a metadata category from samples from a set of features

    Parameters
    ----------
    context : str
        A context to search in.
    category : str
        The category to summarize.
    features : Iterable of str
        The features to search for samples with.
    exact : bool
        If true, all samples must contain all specified features. If false,
        all samples contain at least one of the features.

    Returns
    -------
    pandas.Series
        A series indexed by the sample ID and valued by its category value. A
        None is used if the sample does not have that piece of metadata.
    """
    import redbiom._requests
    redbiom._requests.valid(context)

    import redbiom.util
    samples = redbiom.util.ids_from(features, exact, 'feature', [context])

    import redbiom.fetch
    return redbiom.fetch.category_sample_values(category, samples)


def category_from_samples(category, samples):
    """Summarize a metadata category from samples

    Parameters
    ----------
    category : str
        The category to summarize.
    samples : Iterable of str
        The samples to search for samples with.

    Returns
    -------
    pandas.Series
        A series indexed by the sample ID and valued by its category value. A
        None is used if the sample does not have that piece of metadata.
    """
    import redbiom.fetch
    return redbiom.fetch.category_sample_values(category, samples)
