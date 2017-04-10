import click

from . import cli


@cli.group()
def select():
    """Select items based on metadata"""
    pass


@select.command(name='samples-from-metadata')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--context', required=False, type=str, default=None,
              help="The context to search within.")
@click.option('--restrict-to', required=False, type=str, default=None,
              help="A comma separated list of categories to restrict the "
                   "retrieval of metadata too. This is strictly done in order "
                   "to limit the expense of obtaining all of the metadata in "
                   "common for the samples under investigation. If this "
                   "option is used, it is assumed that the WHERE clause is "
                   "applicable to the columns in the restricted set.")
@click.option('--where', required=True, type=str,
              help="The WHERE clause to apply")
@click.argument('samples', nargs=-1)
def select_samples_from_metadata(from_, restrict_to, context, where, samples):
    """Given samples, select based on metadata"""
    import redbiom.util
    iterator = redbiom.util.from_or_nargs(from_, samples)

    import redbiom.fetch

    if restrict_to is not None:
        restrict_to = restrict_to.split(',')

    md, map_ = redbiom.fetch.sample_metadata(iterator, context=context,
                                             restrict_to=restrict_to,
                                             common=False)

    import redbiom.metadata
    md = redbiom.metadata.Metadata(md.set_index('#SampleID'))

    ids = md.ids(where=where)
    for i in ids:
        click.echo(i)
