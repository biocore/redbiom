import click

from . import cli


@cli.group()
def summarize():
    """Summarize things."""
    pass


@summarize.command(name='contexts')
def summarize_caches():
    """List names of available caches"""
    import redbiom.summarize
    contexts = redbiom.summarize.contexts()

    if len(contexts):
        import sys
        import io
        if sys.version_info[0] < 3:
            out = io.BytesIO()
        else:
            out = io.StringIO()
        contexts.to_csv(out, sep='\t', header=True, index=False)
        out.seek(0)
        click.echo(out.read())
    else:
        click.echo("No available contexts")


@summarize.command(name='metadata-category')
@click.option('--category', required=True,
              help="The metadata category (i.e., column) to summarize")
@click.option('--counter', required=False, is_flag=True, default=False,
              help="If true, obtain value counts")
@click.option('--descending', is_flag=True, required=False, default=False,
              help="If true, sort in descending order")
@click.option('--dump', required=False, is_flag=True, default=False,
              help="If true, print the sample information.")
@click.option('--sort-index', is_flag=True, required=False, default=False,
              help=("If true, sort on the index instead of the values. This "
                    "option is only relevant when --counter is specified."))
def summarize_metadata_category(category, counter, descending, dump,
                                sort_index):
    """Summarize the values within a metadata category"""
    if not counter and not dump:
        click.echo("Please specify either --counter or --dump",
                   err=True)
        import sys
        sys.exit(1)

    import redbiom.fetch
    md = redbiom.fetch.category_sample_values(category)

    if counter:
        click.echo("Category value\tcount")
        counts = md.value_counts(ascending=not descending)
        if sort_index:
            counts = counts.sort_index(ascending=not descending)

        for idx, val in zip(counts.index, counts):
            click.echo("%s\t%s" % (idx, val))
    else:
        click.echo("#SampleID\t%s" % category)
        for idx, val in zip(md.index, md):
            click.echo("%s\t%s" % (idx, val))


@summarize.command(name='metadata')
@click.option('--descending', is_flag=True, required=False, default=False,
              help="If true, sort in descending order")
@click.argument('categories', nargs=-1)
def summarize_metadata(descending, categories):
    """Get the known metadata categories and associated sample counts"""
    import redbiom.fetch

    if not categories:
        categories = None

    md = redbiom.fetch.sample_counts_per_category(categories)
    md = md.sort_values(ascending=not descending)

    for idx, val in zip(md.index, md):
        click.echo("%s\t%s" % (idx, val))


def _summarize_id(context, category, id):
    """Summarize the ID over the category"""
    import redbiom.summarize
    res = redbiom.summarize.category_from_features(context, category,
                                                   [id], False)
    res = res.value_counts()
    counts = {i: c for i, c in zip(res.index, res)}
    counts['feature'] = id
    return counts


@summarize.command(name='table')
@click.option('--category', type=str, required=True)
@click.option('--context', required=True, type=str)
@click.option('--output', required=False, type=click.Path(exists=False),
              default=None)
@click.option('--threads', type=int, default=1)
@click.option('--verbosity', type=int, default=0)
@click.option('--table', type=click.Path(exists=True), required=True)
def summarize_table(category, context, output, threads, verbosity, table):
    """Summarize all features in a BIOM table.

    This command will assess, per feature, the number of samples that
    feature is found in relative to the metadata category specified.
    """

    import redbiom.util
    if not redbiom.util.category_exists(category):
        import sys
        click.echo("%s is not found" % category, err=True)
        sys.exit(1)

    import biom
    table = biom.load_table(table)

    import joblib
    with joblib.parallel.Parallel(n_jobs=threads, verbose=verbosity) as par:
        mappings = par(joblib.delayed(_summarize_id)(context, category, id)
                       for id in table.ids(axis='observation'))

    import pandas as pd
    df = pd.DataFrame(mappings)
    df.set_index('feature', inplace=True)
    df[df.isnull()] = 0

    tsv = df.to_csv(None, sep='\t', header=True, index=True)
    if output is None:
        click.echo(tsv)
    else:
        with open(output, 'w') as fp:
            fp.write(tsv)
    # TODO: should this output BIOM? It is a feature table.


@summarize.command(name='features')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--category', type=str, required=True)
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified features")
@click.option('--context', required=True, type=str)
@click.argument('features', nargs=-1)
def summarize_features(from_, category, exact, context,
                       features):
    """Summarize features over a metadata category."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, features)

    import redbiom.summarize
    md = redbiom.summarize.category_from_features(context, category,
                                                  iterable, exact)

    cat_stats = md.value_counts()
    for val, count in zip(cat_stats.index, cat_stats.values):
        click.echo("%s\t%s" % (val, count))
    click.echo("\n%s\t%s" % ("Total samples", sum(cat_stats.values)))


@summarize.command(name='samples')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--category', type=str, required=True)
@click.argument('samples', nargs=-1)
def summarize_samples(from_, category, samples):
    """Summarize samples over a metadata category."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, samples)

    import redbiom.summarize
    md = redbiom.summarize.category_from_samples(category, iterable)

    cat_stats = md.value_counts()
    for val, count in zip(cat_stats.index, cat_stats.values):
        click.echo("%s\t%s" % (val, count))
    click.echo("\n%s\t%s" % ("Total samples", sum(cat_stats.values)))


@summarize.command(name='taxonomy')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.option('--normalize-ranks', required=False, default='kpcofgs',
              type=str,
              help="Coerce normalized rank information for unclassifieds")
@click.argument('features', nargs=-1)
def taxonomy(from_, context, normalize_ranks, features):
    """Summarize taxonomy at all levels.

    This yields each taxonomic group, the number of features represented by
    the taxon, and the fraction of the total features passed in.
    """
    import redbiom.util
    ids = list(redbiom.util.from_or_nargs(from_, features))

    import redbiom.fetch
    lineages = redbiom.fetch.taxon_ancestors(context, ids,
                                             normalize=normalize_ranks)

    import skbio
    tree = skbio.TreeNode.from_taxonomy([(i, l)
                                         for i, l in zip(ids, lineages)])

    n_tips = float(len(list(tree.tips())))
    click.echo("Taxon\tCount\tFractionOfInput")
    for n in tree.postorder(include_self=False):
        if n.is_tip():
            n.count = 1
        else:
            n.count = sum(c.count for c in n.children)
            if not n.name.endswith('__'):
                click.echo("%s\t%d\t%0.4f" % (n.name, n.count,
                                              n.count / n_tips))
