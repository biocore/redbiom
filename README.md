# ![redbiom](logo.png)
# ![build-status](https://travis-ci.org/wasade/redbiom.svg?branch=master)

# What is this?

Load BIOM tables and sample metadata from a lot of studies into Redis to facilitate search using observation IDs or metadata. redbiom is intended as a caching layer. The command line application, for many of the commands, supports Unix pipes allowing for a fair bit of composability. As a hypothetical example, let's say we had loaded a bunch of microbiome data picked againts Greengenes 13_8 at 97% (and into an appropriately named context). To obtain a BIOM table representing all samples in which either OTUs 367523 or 3064251 exist, you can perform the following set of querys:

    $ redbiom search observations --context "greengenes-13_8-97" 367523 3064251 | redbiom fetch samples --from - --output result.biom

Additionally, assuming a comprehensive set of metadata are available, you can obtain summarized environment information:
    
    $ redbiom search observations --context "greengenes-13_8-97" 367523 3064251 | redbiom summarize samples --from - --category "env_biome"

# Installation

Needs [Redis](https://redis.io/) (tested with 3.2.6) and [Webdis](http://webd.is/) (just clone the repo). Also depends on [BIOM](http://biom-format.org/) (tested on 2.1.5), [Pandas](http://pandas.pydata.org/) (tested on 0.19.0), [Click](http://click.pocoo.org/5/) (required >= 6.7) and [scipy](https://www.scipy.org/) (whatever BIOM is happy with). It is not necessary to have super user access.

For Redis, the following has worked on OSX and multiple flavors of Linux without issue.

    $ http://download.redis.io/releases/redis-3.2.6.tar.gz
    $ tar xzf redis-3.2.6.tar.gz
    $ pushd redis-3.2.6
    $ make
    $ ./src/redis-server --daemonize
    $ popd

Webdis packages its dependencies with the exception of libevent. It is entirely likely that libevent is already available on your system. If so, the following should work. If libevent is not available, compilation will die quickly. However, libevent is in all the common repositories (e.g., yum, apt, brew, etc), and compiling by source is straight forward. 

    $ git clone https://github.com/nicolasff/webdis.git
    $ pushd webdis
    $ make
    $ ./webdis &
    $ popd

Last, redbiom itself can be installed as a normal Python package.

    $ git clone https://github.com/wasade/redbiom.git
    $ pip install -e .    

# Examples

The first example block surrounds loading data, because without anything in the cache, there is nothing fun to do. The second block highlights some example commands that can be run, or chained together, for querying the data loaded.

## Load some data

To make use of this cache, we need to load things. Loading can be done in parallel. First, we'll load up metadata. This will create keys in Redis which describe all of the columns associated with a sample (e.g., `metadata:categories:<sample_id>`, hash buckets for each category and sample combination (e.g., `metadata:category:<category_name>` as the hash and `<sample_id>` as the field), a set of all known categories (e.g., `metadata:categories-represented`), and a set of all known sample IDs (e.g., `metadata:samples-represented`):

	$ redbiom admin load-sample-metadata --metadata path/to/qiime/compat/mapping.txt

**IMPORTANT**: values containing `/` characters cannot be represented the forward slash is used to denote arguments with Webdis. At present, these values are omitted. This is more generally a problem for dates which have not been normalized into an ISO standard. See issue #9.

**IMPORTANT**: values which appear to be null are not stored. The set of values currently considered nulls are: 
    
    {'Not applicable', 'Unknown', 'Unspecified', 'Missing: Not collected',
     'Missing: Not provided', 'Missing: Restricted access',
     'null', 'NULL', 'no_data', 'None', 'nan'}

redbiom supports 1-many mappings between sample metadata and actual sample data. This is done as there may be multiple types of processing performed on the same data (e.g., different nucleotide trims). Or, a physical sample may have been run through multiple protocols (e.g., 16S, WGS, etc). So before we load any data, we need to create a context for the data to be placed. The following action will add an entry into the `state:contexts` hash bucket keyed by `name` and valued by `description`:

    $ redbiom admin create-context --name deblur-100nt --description "16S V4 Caporaso et al data deblurred at 100nt"

Next, we'll load up associations between every single observation in a BIOM table to all the samples its found in. This will create Redis sets and can be accessed using keys of the form `<context_name>:samples:<observation_id>`. Note that we specify the context we're loading into.

	$ redbiom admin load-observations --context deblur-100nt --table /path/to/biom/table.biom

Last, let's load up all of the BIOM table data. We'll only store the non-zero values, and we'll encode the sample data into something simple so that it goes in as just a string to Redis. Important: we only support storing count data right now, not floating point. The keys created are of the form `<context_name>:data:<sample_id>`. To reduce space, we reindex the observation IDs as things like sOTUs tend to be very long in name. The mapping is stable over all tables loaded (ie the same observation has the same index), and the index is stored as a JSON object under the key `<context_name>:__observation_index`. Because we need to update the index, this operation cannot be done in parallel however the code is setup with a redis-based mutex so it's okay to queue up multiple loads.

	$ redbiom load-sample-data --context deblur-100nt --table /path/to/biom/table.biom

## Query for content

Now that things are loaded, we can search for stuff. Let's say you have a few OTUs of interest, and you are curious about what other samples they exist in. You can find that out with:

	$ redbiom search observations --context deblur-100nt <space delimited list of observation IDs>

Or, perhaps you loaded the EMP dataset and are curious about where these OTUs reside. You can get summarized environment information from the search as well:

	$ redbiom search observations --context deblur-100nt --category empo_3 <space delimited list of observation IDs>

That was fun. So now let's go a little further. Perhaps you are interested not just in where those sequences are found in, but also in the samples themselves for a meta-analysis. To pull out all the samples associated with your IDs of interest, and construct a BIOM table, you can do the following:

	$ redbiom fetch observations --context deblur-100nt --output some/path.biom <space delimited list of observation IDs>

...but you probably also want the metadata! Once you have your table, you can obtain it by passing the table back in. This will attempt to grab the metadata (only the columns in common at the moment) for all samples present in your table. Note that we do not have to specify a context here as the sample metadata are context independent:

	$ redbiom fetch sample-metadata --output some/path.txt --table some/path.biom 

# Design decisions

There are a few design decisions in place which deviate from some other typical Python projects. First off, the majority of `import`s are deferred. The motivating force here is to minimize overhead on load as to provide a responsive user interface -- deferred imports are the most straight forward way to accomplish that goal. 

The test harness is broken into multiple components, and are driven by a `Makefile`. This was done initially to be pragmatic as it was easier to write integration tests than unit tests for the `click` commands. That, and I didn't have wifi at the time in order to refer to the docs. These tests can be found in `test.sh` which is composed of "positive" tests and `test_failures.sh` which is composed of "negative" tests. The difference being that the positive tests will fail if any command results in a nonzero exit status, whereas the negative tests expect a nonzero exit status (and really, the decision was to avoid unsetting "-e"). Additional tests which validate some of the Redis contents can be found in `redbiom/tests/test_rest.py`. These are neither unit tests nor integration tests, but simply exercise the behind-the-scenes REST interface. Last, there are some unit tests placed under `redbiom/tests/test_util.py`. 

Many of the command line methods are designed to operate with Unix pipes. This allows for greater composability. However, it also results in some boilerplate code for managing whether data are coming in over stdin, from a file, or from the command line. Note: Unix pipe support was partially resolved in Click for release 6.7, and in general it works, however it is not perfect and if a pipe is terminated (e.g., `foo | head`), it is possible for some noise to be produced over `stderr`. This issue is discussed in more detail [here](https://github.com/pallets/click/issues/712). 

