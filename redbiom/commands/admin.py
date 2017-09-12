import click

from . import cli


@cli.group()
def admin():
    """Update database, etc."""
    pass


@admin.command(name='create-context')
@click.option('--name', required=True, type=str,
              help="The name of the context, e.g., deblur@150nt")
@click.option('--description', required=True, type=str,
              help=("Default quality filtering, followed by application of "
                    "Deblur with a trim length of 150nt."))
def create_context(name, description):
    """Create context for sample data."""
    import redbiom.admin
    redbiom.admin.create_context(name, description)


@admin.command(name='coherency')
@click.option('--context', required=False, type=str, default=None,
              help='The context to examine, do all if not specified.')
def coherency(context):
    """Assert coherency within contexts.

    Coherency is defined as:

    - each sample in each context has sample metadata
    - each sample in each context has feature associations
    - each sample in each context has sample data
    """
    # useful as this is not explicitly enforced. explicit enforcement would
    # pose a massive challenge. should only be necessary to run _after_ a
    # cache load. since the cache is read-only following low, follow up
    # coherency checking is not critical.
    raise ValueError("see inline comment")


@admin.command(name='load-sample-data')
@click.option('--table', required=True, type=click.Path(exists=True),
              help="The filepath to the table to load.")
@click.option('--context', required=True, type=str,
              help="The name of the context to load into.")
@click.option('--tag', required=False, type=str, default=None,
              help="The tag associated to the samples (e.g., preparation ID).")
@click.option('--mass-insertion', default=False, is_flag=True)
def load_sample_data(table, context, tag, mass_insertion):
    """Load nonzero entries per sample."""
    import redbiom.admin
    import biom
    table = biom.load_table(table)
    redbiom.admin.load_sample_data(table, context, tag=tag,
                                   redis_protocol=mass_insertion)


@admin.command(name='load-sample-metadata')
@click.option('--metadata', required=True, type=click.Path(exists=True),
              help="The filepath to the sample metadata to load.")
def load_sample_metadata(metadata):
    """Load sample metadata."""
    import redbiom.admin
    import pandas as pd
    metadata = pd.read_csv(metadata, sep='\t', dtype=str,
                           keep_default_na=False, na_values=[])
    n_loaded = redbiom.admin.load_sample_metadata(metadata)
    click.echo("Loaded %d samples" % n_loaded)


@admin.command(name='load-sample-metadata-search')
@click.option('--metadata', required=True, type=click.Path(exists=True),
              help="The filepath to the sample metadata to load.")
def load_sample_metadata_search(metadata):
    """Load sample metadata."""
    import redbiom.admin
    import pandas as pd
    metadata = pd.read_csv(metadata, sep='\t', dtype=str,
                           keep_default_na=False, na_values=[])
    n_values, n_cats = redbiom.admin.load_sample_metadata_full_search(metadata)
    click.echo("Found %d category stems and %d metadata value stems" %
               (n_cats, n_values))


@admin.command(name='scripts-read-only')
def read_only():
    """Set scripts to read-only"""
    import redbiom.admin
    redbiom.admin.ScriptManager.drop_scripts()
    redbiom.admin.ScriptManager.load_scripts(read_only=True)


@admin.command(name='scripts-writable')
def writable():
    """Set scripts to allow write"""
    import redbiom.admin
    redbiom.admin.ScriptManager.drop_scripts()
    redbiom.admin.ScriptManager.load_scripts(read_only=False)
