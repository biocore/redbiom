import click


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

    return iter(nargs_variable)


def samples_from_observations(it, exact, context, get=None):
    """Grab samples from an iterable of observations"""
    import redbiom.requests

    cmd = 'SINTER' if exact else 'SUNION'
    samples = None
    for _, block in redbiom.requests.buffered(it, 'samples', cmd, context,
                                              get=get):
        block = set(block)
        if not exact:
            if samples is None:
                samples = set()
            samples.update(block)
        else:
            if samples is None:
                samples = block
            else:
                samples = samples.intersection(block)
    return samples


def float_or_nan(t):
    import math
    try:
        return float(t)
    except:
        return math.nan


def has_sample_metadata(samples, get=None):
    """Test if all samples have sample metadata"""
    import redbiom.requests
    if get is None:
        import redbiom
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

    untagged, tagged, _, tagged_clean = partition_samples_by_tags(samples)

    # make sure all samples have metadata
    represented = set(get('metadata', 'SMEMBERS', 'samples-represented'))
    if not set(untagged).issubset(represented):
        return False
    if not set(tagged_clean).issubset(represented):
        return False
    if not set(tagged).issubset(represented):
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
