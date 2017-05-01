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
def summarize_metadata(descending):
    """Get the known metadata categories and associated sample counts"""
    import redbiom.fetch
    md = redbiom.fetch.sample_counts_per_category()
    md = md.sort_values(ascending=not descending)

    for idx, val in zip(md.index, md):
        click.echo("%s\t%s" % (idx, val))


def _summarize_id(context, category, id):
    """Summarize the ID over the category"""
    import redbiom.summarize
    res = redbiom.summarize.category_from_observations(context, category,
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
@click.option('--verbosity', type=int, default=0)
@click.option('--table', type=click.Path(exists=True), required=True)
def summarize_table(category, context, output, verbosity, table):
    """Summarize all observations in a BIOM table.

    This command will assess, per observation, the number of samples that
    observation is found in relative to the metadata category specified.
    """
    import redbiom.util
    if not redbiom.util.category_exists(category):
        import sys
        click.echo("%s is not found" % category, err=True)
        sys.exit(1)

    import biom
    table = biom.load_table(table)

    mappings = [_summarize_id(context, category, id)
                for id in table.ids(axis='observation')]

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


@summarize.command(name='observations')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              default=None)
@click.option('--category', type=str, required=True)
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified observations")
@click.option('--context', required=True, type=str)
@click.argument('observations', nargs=-1)
def summarize_observations(from_, category, exact, context,
                           observations):
    """Summarize observations over a metadata category."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, observations)

    import redbiom.summarize
    md = redbiom.summarize.category_from_observations(context, category,
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
