# ![redbiom](logo.png)
# ![build-status](https://travis-ci.org/wasade/redbiom.svg?branch=master)

# What is this?

Load BIOM tables and sample metadata from a lot of studies into Redis to facilitate search using observation IDs or metadata.

It is safe to say the interfaces are not stable.

# Installation

Needs Redis (tested with 3.2.6) and Webdis (just clone the repo). Also depends on BIOM (tested on 2.1.5-dev), Pandas (tested on 0.19.0), Click (required >= 6.7) and scipy (whatever BIOM is happy with).

# Load some stuff

To make use of this cache, we need to load things. Loading can be done in parallel. First, we'll load up metadata. This will create keys in Redis which describe all of the columns associated with a sample (e.g., `metadata:categories:<sample_id>`, hash buckets for each category and sample combination (e.g., `metadata:category:<category_name>` as the hash and `<sample_id>` as the field), a set of all known categories (e.g., `metadata:categories-represented`), and a set of all known sample IDs (e.g., `metadata:samples-represented`):

	$ redbiom admin load-sample-metadata --metadata path/to/qiime/compat/mapping.txt

redbiom supports 1-many mappings between sample metadata and actual sample data. This is done as there may be multiple types of processing performed on the same data (e.g., different nucleotide trims). Or, a physical sample may have been run through multiple protocols (e.g., 16S, WGS, etc). So before we load any data, we need to create a context for the data to be placed. The following action will add an entry into the `state:contexts` hash bucket keyed by `name` and valued by `description`:

    $ redbiom admin create-context --name deblur-100nt --description "16S V4 Caporaso et al data deblurred at 100nt"

Next, we'll load up associations between every single observation in a BIOM table to all the samples its found in. This will create Redis sets and can be accessed using keys of the form `<context_name>:samples:<observation_id>`. Note that we specify the context we're loading into.

	$ redbiom admin load-observations --context deblur-100nt --table /path/to/biom/table.biom

For icing on the cake, lets also load up all of the BIOM table data because why not. We'll only store the non-zero values, and we'll encode the sample data into something simple so that it goes in as just a string to Redis. Important: we only support storing count data right now, not floating point. The keys created are of the form `<context_name>:data:<sample_id>`. To reduce space, we reindex the observation IDs as things like sOTUs tend to be very long in name. The mapping is stable over all tables loaded (ie the same observation has the same index), and the index is stored as a JSON object under the key `<context_name>:__observation_index`. Because we need to update the index, this operation cannot be done in parallel however the code is setup with a redis-based mutex so it's okay to queue up multiple loads.

	$ redbiom load-sample-data --context deblur-100nt --table /path/to/biom/table.biom

# Get you some stuff

Now that things are loaded, we can search for stuff. Let's say you have a few OTUs of interest, and you are curious about what other samples they exist in. You can find that out with:

	$ redbiom search observations --context deblur-100nt <space delimited list of observation IDs>

Or, perhaps you loaded the EMP dataset and are curious about where these OTUs reside. You can get summarized environment information from the search as well:

	$ redbiom search observations --context deblur-100nt --category empo_3 <space delimited list of observation IDs>

That was fun. So now let's go a little further. Perhaps you are interested not just in where those sequences are found in, but also in the samples themselves for a meta-analysis. To pull out all the samples associated with your IDs of interest, and construct a BIOM table, you can do the following:

	$ redbiom fetch observations --context deblur-100nt --output some/path.biom <space delimited list of observation IDs>

...but you probably also want the metadata! Once you have your table, you can obtain it by passing the table back in. This will attempt to grab the metadata (only the columns in common at the moment) for all samples present in your table. Note that we do not have to specify a context here as the sample metadata are context independent:

	$ redbiom fetch sample-metadata --output some/path.txt --table some/path.biom 
