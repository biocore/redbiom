def contexts():
    """Obtain the name and description of known contexts

    Returns
    -------
    list of (str, str)
        A list of (name, desciption) for each known context.

    Redis command summary
    ---------------------
    HGETALL state:contexts
    """
    import redbiom
    import redbiom.requests
    get = redbiom.requests.make_get(redbiom.get_config())

    return get('state', 'HGETALL', 'contexts')


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
    import redbiom.requests
    redbiom.requests.valid(context)

    import redbiom.util
    # TODO: should samples_from_observations be in redbiom.fetch?
    samples = redbiom.util.samples_from_observations(observations, exact,
                                                     context)

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
