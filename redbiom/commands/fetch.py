import click

from . import cli


@cli.group()
def fetch():
    """Sample data and metadata retrieval."""
    pass


@fetch.command(name='tags-contained')
@click.option('--context', required=True, type=str, default=None,
              help="The context to fetch from.")
def fetch_tags_contained(context):
    """Get the observed tags within a context"""
    import redbiom.fetch
    for id_ in redbiom.fetch.tags_in_context(context):
        click.echo(id_)


@fetch.command(name='samples-contained')
@click.option('--context', required=False, type=str, default=None,
              help="The context to fetch from.")
@click.option('--unambiguous', required=False, is_flag=True, default=False,
              help="Return ambiguous or unambiguous identifiers")
def fetch_samples_contained(context, unambiguous):
    """Get samples within a context.

    Return all of the sample identifiers which are represented in a context.
    """
    import redbiom.fetch
    for id_ in redbiom.fetch.samples_in_context(context, unambiguous):
        click.echo(id_)


@fetch.command(name='features-contained')
@click.option('--context', required=False, type=str, default=None,
              help="The context to fetch from.")
def fetch_features_contained(context):
    """Get features within a context.

    Return all of the features which are represented in a context.
    """
    import redbiom.fetch
    for id_ in redbiom.fetch.features_in_context(context):
        click.echo(id_)


@fetch.command(name='sample-metadata')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--context', required=False, type=str, default=None,
              help="The context to search within.")
@click.option('--all-columns', is_flag=True, default=False,
              help=("If set, all metadata columns for all samples are "
                    "obtained. The empty string is used if the column is not "
                    "present for a given sample."))
@click.option('--tagged', is_flag=True, default=False,
              help=("Obtain the tag specific metadata (e.g., preparation "
                    "information)."))
@click.option('--resolve-ambiguities', is_flag=True, default=False,
              help=("Output unambiguous identifiers only. This option is "
                    "incompatible with --tagged."))
@click.option('--force-category', type=str, required=False, multiple=True,
              help=("Force the output to include specific metadata variables "
                    "if the metadata variable was observed in any of the "
                    "samples. This can be specified mulitple times for "
                    "multiple categories."))
@click.argument('samples', nargs=-1)
def fetch_sample_metadata(from_, samples, all_columns, context, output,
                          tagged, force_category, resolve_ambiguities):
    """Retreive sample metadata."""
    if resolve_ambiguities and tagged:
        click.echo("Cannot resolve ambiguities and fetch tagged metadata",
                   err=True)
        import sys
        sys.exit(1)

    import redbiom.util
    import redbiom.fetch
    import pandas as pd

    iterator = redbiom.util.from_or_nargs(from_, samples)

    if not force_category:
        force_category = None

    md, map_ = redbiom.fetch.sample_metadata(iterator, context=context,
                                             common=not all_columns,
                                             restrict_to=force_category,
                                             tagged=tagged)

    if resolve_ambiguities:
        md.set_index('#SampleID', inplace=True)

        # a temporary key to use when resolving ambiguities
        # that will be removed before writing the metadata
        key = "__@@AMBIGUITY@@__"

        # add ambiguity information into the frame
        ambigs = pd.Series(map_)
        ambigs = ambigs.loc[md.index]
        md[key] = ambigs

        # remove duplicated unambiguous identifiers
        md = md[~md[key].duplicated()]

        # remove our index, and replace the entries with the ambiguous names
        md.reset_index(inplace=True)
        md['#SampleID'] = md[key]

        # cleanup
        md.drop(columns=key, inplace=True)

    md.to_csv(output, sep='\t', header=True, index=False, encoding='utf-8')

    _write_ambig(map_, output)


@fetch.command(name='features')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides features to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--exact', is_flag=True, default=False,
              help="All found samples must contain all specified features")
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.option('--md5', required=False, type=bool,
              help="Calculate and use MD5 for the features. This will also "
              "save a tsv file with the original feature name and the md5",
              default=False)
@click.option('--resolve-ambiguities', required=False,
              type=click.Choice(['merge', 'most-reads']), default=None,
              help=("Resolve ambiguities that may be present in the samples "
                    "which can arise from, for example, technical "
                    "replicates."))
@click.argument('features', nargs=-1)
def fetch_samples_from_obserations(features, exact, from_, output,
                                   context, md5, resolve_ambiguities):
    """Fetch sample data containing features."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, features)

    import redbiom.fetch
    tab, map_ = redbiom.fetch.data_from_features(context, iterable, exact)

    if md5:
        tab, new_ids = redbiom.util.convert_biom_ids_to_md5(tab)
        with open(output + '.tsv', 'w') as f:
            f.write('\n'.join(['\t'.join(x) for x in new_ids.items()]))

    if resolve_ambiguities == 'merge':
        tab = redbiom.fetch._ambiguity_merge(tab, map_)
    elif resolve_ambiguities == 'most-reads':
        tab = redbiom.fetch._ambiguity_keep_most_reads(tab, map_)

    import h5py
    with h5py.File(output, 'w') as fp:
        tab.to_hdf5(fp, 'redbiom')

    _write_ambig(map_, output)


@fetch.command(name='samples')
@click.option('--from', 'from_', type=click.File('r'), required=False,
              help='A file or stdin which provides samples to search for',
              default=None)
@click.option('--output', required=True, type=click.Path(exists=False),
              help="A filepath to write to.")
@click.option('--context', required=True, type=str,
              help="The context to search within.")
@click.option('--md5', required=False, type=bool,
              help="Calculate and use MD5 for the features. This will also "
              "save a tsv file with the original feature name and the md5",
              default=False)
@click.option('--resolve-ambiguities', required=False,
              type=click.Choice(['merge', 'most-reads']), default=None,
              help=("Resolve ambiguities that may be present in the samples "
                    "which can arise from, for example, technical "
                    "replicates."))
@click.argument('samples', nargs=-1)
def fetch_samples_from_samples(samples, from_, output, context, md5,
                               resolve_ambiguities):
    """Fetch sample data."""
    import redbiom.util
    iterable = redbiom.util.from_or_nargs(from_, samples)

    import redbiom.fetch
    table, ambig = redbiom.fetch.data_from_samples(context, iterable)

    if md5:
        table, new_ids = redbiom.util.convert_biom_ids_to_md5(table)
        with open(output + '.tsv', 'w') as f:
            f.write('\n'.join(['\t'.join(x) for x in new_ids.items()]))

    if resolve_ambiguities == 'merge':
        table = redbiom.fetch._ambiguity_merge(table, ambig)
    elif resolve_ambiguities == 'most-reads':
        table = redbiom.fetch._ambiguity_keep_most_reads(table, ambig)

    import h5py
    with h5py.File(output, 'w') as fp:
        table.to_hdf5(fp, 'redbiom')
    _write_ambig(ambig, output)


def _write_ambig(map_, output):
    from collections import defaultdict
    ambig = defaultdict(list)
    for k, v in map_.items():
        ambig[v].append(k)
    ambig = {k: v for k, v in ambig.items() if len(v) > 1}

    if len(ambig) > 1:
        import json
        click.echo("%d sample ambiguities observed. Writing ambiguity "
                   "mappings to: %s" % (len(ambig), output + '.ambiguities'),
                   err=True)
        with open(output + '.ambiguities', 'w') as fp:
            fp.write(json.dumps(ambig, indent=2))
