import click

from . import cli


def _axis_search(from_, exact, context, ids, axis):
    import redbiom._requests
    import redbiom.util

    redbiom._requests.valid(context)

    it = redbiom.util.from_or_nargs(from_, ids)

    # determine the opposite axis ids associated with query ids
    observed = redbiom.util.ids_from(it, exact, axis, context)

    for id_ in observed:
        click.echo(id_)


@cli.group()
def search():
    """Feature and sample search support."""
    pass


@search.command(name="features")
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides features to search for',
              default=None)
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified features")
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('features', nargs=-1)
def search_features(from_, exact, context, features):
    """Get samples containing features."""
    _axis_search(from_, exact, context, features, 'feature')


@search.command(name="samples")
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--exact', is_flag=True, default=False,
              help=("All found features must be present in all specified "
                    "samples"))
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('samples', nargs=-1)
def search_samples(from_, exact, context, samples):
    """Get features present in samples."""
    import redbiom
    import redbiom._requests
    import redbiom.util

    config = redbiom.get_config()
    get = redbiom._requests.make_get(config)
    _, _, _, rb_ids = redbiom.util.resolve_ambiguities(context, samples, get)
    rb_ids = list(rb_ids)
    _axis_search(from_, exact, context, iter(rb_ids), 'sample')


@search.command(name='metadata')
@click.option('--categories', is_flag=True, required=False, default=False,
              help="Search for metadata categories instead of metadata values")
@click.argument('query', nargs=1)
def search_metadata(query, categories):
    """Find samples or categories.

    The metadata search engine uses natural language processing to search for
    word stems within a samples metadata. A word stem disregards modifiers and
    plurals, so for instance, a search for "antibiotics" will actually perform
    a search for "antibiot". Similarly, a search for "crying" will actually
    search for "cry". The words specified can be combined with set-based
    operations, so for instance, a search for "antibiotics & crying" will
    obtain the set of samples in which each sample has "antibiot" in its
    metadata as well as "cry". N.B., the specific category in which a stem is
    found is not assured to be the same, "antibiot" could be in one category
    and "cry" in another. A set intersection can be performed with "&", a
    union with "|" and a difference with "-".

    The stem based search can also be applied to metadata categories when
    "--categories" is specified.

    In addition to the stem-based search, value based searches can also be a
    applied. These use a Python-like grammar and allow for a rich set of
    comparisons to be performed based on a metadata category of interest. For
    example, "where qiita_study_id == 10317" will find all samples which have
    the qiita_study_id metadata category, and in which the value for that
    sample is "10317."

    These two types of queries can be combined. A few examples are below.
    These queries make assumptions about the metadata available, and are
    only intended to be illustrative.

    Find all samples in which the word antibiotics exists in its metadata.

    $ redbiom search metadata antibiotics

    Find all samples in which the word infant exists, as well as antibiotics,
    where the infants are under a certain number of days old:

    $ redbiom search metadata "infant & antibiotics where age_days < 30"

    We can also use this engine to find metadata categories. In the next
    example, we're searching for all metadata categories which contain the
    "ph", and we'll go ahead and remove any category which contains the stem
    "water".

    $ redbiom search metadata --categories "ph - water"
    """
    import redbiom.search
    for i in redbiom.search.metadata_full(query, categories):
        click.echo(i)


@search.command(name='taxon')
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.argument('query', nargs=1)
def search_taxon(context, query):
    """Find features associated with a taxon"""
    import redbiom.fetch
    for i in redbiom.fetch.taxon_descendents(context, query):
        click.echo(i)
