import click


NULL_VALUES = {'Not applicable', 'Unknown', 'Unspecified',
               'Missing: Not collected', None,
               'Missing: Not provided',
               'Missing: Not Provided', 'missing', '',
               'Missing: Restricted access',
               'Missing:Not reported',
               'Missing: Not applicable',
               'NA', 'null', 'NULL', 'no_data', 'None', 'nan',
               'NaN'}


def from_or_nargs(from_, nargs_variable):
    """In support of buffered: determine whether to use from_ or nargs"""
    import sys
    if (from_ is None or from_ == '-') and not nargs_variable:
        # let's assume the user wants to use stdin
        from_ = sys.stdin

    if from_ is not None and nargs_variable:
        click.echo("Unable to handle --from as well as cmdline items",
                   err=True)
        sys.exit(1)

    if from_ is not None:
        nargs_variable = from_

    return iter((s.strip() for s in nargs_variable))


def ids_from(it, exact, axis, contexts):
    """Grab samples from an iterable of IDs

    Parameters
    ----------
    it : iteraable of str
        The IDs to search for
    exact : boolean
        If True, compute the intersection of results per context. If False,
        compute the union of results per context.
    axis : {'feature', 'sample'}
        The axis to operate over.
    contexts : list of str
        The contexts to search in

    Notes
    -----
    Contexts are evaluated independently, and the results of each context are
    unioned.

    Returns
    -------
    set
        The sample IDs associated with the search IDs.

    """
    import redbiom
    import redbiom._requests
    import redbiom.admin
    config = redbiom.get_config()
    se = redbiom._requests.make_script_exec(config)

    retrieved = set()

    if axis not in {'feature', 'sample'}:
        raise ValueError("Unknown axis: %s" % axis)

    if not isinstance(contexts, (list, set, tuple)):
        contexts = [contexts]

    it = list(it)
    fetcher = redbiom.admin.ScriptManager.get('fetch-%s' % axis)
    for context in contexts:
        context_ids = None
        for id_ in it:
            block = se(fetcher, 0, context, id_)
            if not exact:
                if context_ids is None:
                    context_ids = set()
                context_ids.update(block)
            else:
                if context_ids is None:
                    context_ids = set(block)
                else:
                    context_ids = context_ids.intersection(block)

        if context_ids:
            retrieved = retrieved.union(context_ids)

    return retrieved


def category_exists(category, get=None):
    """Test if a category exists

    Parameters
    ----------
    category : str
        The category to test for
    get : function
        A get method

    Returns
    -------
    bool
        True if the category exists, False otherwise

    Redis Command Summary
    ---------------------
    SISMEMBER <category> metadata:catetories-represented
    """
    if get is None:
        import redbiom
        import redbiom._requests
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    # this use highlights how get is being abused at the moment. this is a
    # command which takes two arguments, they key and the member to test.
    return get('metadata', 'SISMEMBER', 'categories-represented/%s' % category)


def float_or_nan(t):
    import numpy as np
    try:
        return float(t)
    except Exception:
        return np.nan


def has_sample_metadata(samples, get=None):
    """Test if all samples have sample metadata"""
    import redbiom._requests
    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom._requests.make_get(config)

    untagged, tagged, _, tagged_clean = partition_samples_by_tags(samples)

    # make sure all samples have metadata
    represented = set(get('metadata', 'SMEMBERS', 'samples-represented'))
    if not set(untagged).issubset(represented):
        return False
    if not set(tagged_clean).issubset(represented):
        return False

    return True


def partition_samples_by_tags(samples):
    """Partition samples by the presence of a sample tag"""
    # by example ['foo_123.3', 'xyz', '23_ss']
    tagged = []  # ['foo_123.3', '23_s']
    tagged_clean = []  # ['123.3', 'ss']
    tags = []  # ['foo', '23']
    untagged = []  # ['xyz']
    for sample in samples:
        parts = sample.split('_', 1)
        if len(parts) == 2:
            tag, sample_split = parts
            tagged.append(sample)
            tags.append(tag)
            tagged_clean.append(sample_split)
        else:
            untagged.append(sample)

    return untagged, tagged, tags, tagged_clean


def resolve_ambiguities(context, samples, get):
    """Determine mappings for requested samples

    This method accepts samples in the form of "sampleid" or "rid_sampleid". It
    then attempts to resolve any sample ambiguities which may exist in the
    context. For a "sampleid" there may be multiple "rid_sampleid" values which
    exists, for instance, the same sample may have multiple preps within the
    same study and datatype (e.g., biological replicates).

    Parameters
    ----------
    context : str
        The context to search within
    samples : Iterable of str
        The samples to resolve. The samples must be in the form of "sampleid"
        or "rid_sampleid". The former is an ambiguous association as it does
        not have a tag affixed (e.g., a sample preparation ID). The latter is
        fully specified and assured to be unique within the context.
    get : redbiom._requests.make_get instance
        A getter

    Returns
    -------
    stable
        A dict of stable QIIME compatible sample IDs, keyed by the QIIME
        compatible ID and valued by the redbiom ID.
    unobserved
        A list of any requested ID which was not observed in the context.
    ambituities
        A dict of untagged sample IDs (e.g., "sampleid") to a list of the
        observed "rid_sampleid" values within the context. In other words,
        this dict associated an unspecific ID to a unique redbiom ID.
    redbiomids
        A dict keyed by "rid_sampleid" and valued by a QIIME compatible sample
        ID.
    """
    from collections import defaultdict

    # split the requested samples into what is and is not tagged
    untagged, tagged, _, tagged_clean = partition_samples_by_tags(samples)

    # get all known tagged samples in the context
    ctx = get(context, 'SMEMBERS', 'samples-represented')
    _, ctx_tagged, _, ctx_tagged_clean = partition_samples_by_tags(ctx)

    # create a map of known ambiguous ID -> known stable IDs
    ctx_with_ambig = defaultdict(list)
    for with_tag, without_tag in zip(ctx_tagged, ctx_tagged_clean):
        ctx_with_ambig[without_tag].append(with_tag)
    ctx_known_stable = set(ctx_tagged)

    # what is ambiguous and exists
    unobserved = []
    known_ambiguous = {}
    for i in untagged:
        if i in ctx_with_ambig:
            known_ambiguous[i] = ctx_with_ambig[i]
        else:
            unobserved.append(i)

    stable, ri = _stable_ids_from_ambig(known_ambiguous)

    # what is unambiguous and exists
    unambiguous = []
    for t, tc in zip(tagged, tagged_clean):
        if t in ctx_known_stable:
            unambiguous.append(t)
            if tc not in known_ambiguous:
                known_ambiguous[tc] = []
            known_ambiguous[tc].append(t)
        else:
            unobserved.append(t)

    stable_unamb, ri_unamb = _stable_ids_from_unambig(unambiguous)
    stable.update(stable_unamb)
    ri.update(ri_unamb)

    return stable, unobserved, known_ambiguous, ri


def _stable_ids_from_ambig(ambig_map):
    """Create stable IDs from an ambiguity map"""
    # {qiimeid: stableid}
    ambig_assoc = {}

    # {rid: sampleid}
    ri = {}

    for k, v in ambig_map.items():
        for unambig in v:
            tag, untagged = unambig.split('_', 1)
            stab = "%s.%s" % (untagged, tag)
            ambig_assoc[stab] = k
            ri[unambig] = stab

    return ambig_assoc, ri


def _stable_ids_from_unambig(unambig):
    """Create stable IDs from an unambiguous IDs"""
    # {qiimeid: stableid}
    assoc = {}

    # {rid: sampleid}
    ri = {}

    for k in unambig:
        tag, untagged = k.split('_', 1)
        stab = "%s.%s" % (untagged, tag)
        assoc[stab] = k
        ri[k] = stab
    return assoc, ri


def df_to_stems(df):
    """Convert a DataFrame to stem -> index associations

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame to index

    Returns
    -------
    dict
        {stem: {set of indices}}
    """
    from collections import defaultdict
    import functools
    import nltk

    # not using nltk default as we want this to be portable so that, for
    # instance, a javascript library can query
    stemmer = nltk.PorterStemmer(nltk.PorterStemmer.MARTIN_EXTENSIONS)

    stops = frozenset(nltk.corpus.stopwords.words('english'))
    stem_f = functools.partial(stems, stops, stemmer)

    d = defaultdict(set)

    for sample, row in df.iterrows():
        for value in row.values:
            for stem in stem_f(value):
                d[stem].add(sample)

    return dict(d)


def stems(stops, stemmer, string):
    """Gather stems from string"""
    import re
    import nltk
    to_skip = set('()!@#$%^&*-+=|{}[]<>./?;:')
    to_skip.update(NULL_VALUES)

    # match numbers (doesn't catch sci notation...)
    numeric_regex = re.compile(r'(^-?\d+\.\d+$)|(^-?\d+$)')

    # time like. we don't actually care if this doesn't match time
    # as things like 1234:23123 are probably not useful for *general* search
    time_regex = re.compile(r"^\d+:\d+(am|AM|pm|PM)?$")

    if string in to_skip:
        raise StopIteration

    # for each word
    for word in nltk.tokenize.word_tokenize(string):
        if word in to_skip or len(word) == 1:
            continue

        if word in stops or '/' in word:
            # / is reserved as it's part of a URL
            continue

        if numeric_regex.match(word) is not None:
            continue

        if time_regex.match(word) is not None:
            continue

        try:
            yield stemmer.stem(word).lower()
        except Exception:
            continue
