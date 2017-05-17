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
    SCARD <context>:samples-represented-data
    SCARD <context>:samples-represented-observations
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
            ctx_n_data = get(name, 'SCARD', 'samples-represented-data')
            ctx_n_obs = get(name, 'SCARD', 'samples-represented-observations')

            result.append((name, int(ctx_n_data), int(ctx_n_obs), desc))

        return pd.DataFrame(result, columns=['ContextName', 'SamplesWithData',
                                             'SamplesWithObservations',
                                             'Description'])


def category_from_observations(context, category, observations, exact):
    """Summarize a metadata category from samples from a set of observations

    Parameters
    ----------
    context : str
        A context to search in.
    category : str
        The category to summarize.
    observations : Iterable of str
        The observations to search for samples with.
    exact : bool
        If true, all samples must contain all specified observations. If false,
        all samples contain at least one of the observations.

    Returns
    -------
    pandas.Series
        A series indexed by the sample ID and valued by its category value. A
        None is used if the sample does not have that piece of metadata.
    """
    import redbiom._requests
    redbiom._requests.valid(context)

    import redbiom.util
    # TODO: should samples_from_observations be in redbiom.fetch?
    samples = redbiom.util.samples_from_observations(observations, exact,
                                                     [context])

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
