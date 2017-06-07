# ![redbiom](logo.png)
# ![build-status](https://travis-ci.org/biocore/redbiom.svg?branch=master)

# What is this?

Redbiom is a cache service for sample metadata and sample data. It allows for rapidly:

* finding samples by the features they contain
* finding samples by arbitrary metadata searches
* summarizing samples over metadata
* retieval of sample data into BIOM
* discovering metadata categories
* pulling out sample data from different processing types (e.g., search over 16S, retrieve WGS)

Redbiom is designed to handle biological and technical replicates. Specifically, it allows for a one to many relationship between a sample's metadata and its data, both within and between preparation types.

This repository defines the de facto redbiom data representation, and one possible interface into the resource. Other interfaces (e.g., Javascript) are possible to define. Please see the Design section below for details about how other interfaces can be written.

By default, redbiom will search against `qiita.ucsd.edu:7379`. This can be changed at runtime by setting the `REDBIOM_HOST` environmental variable, e.g., `export REDBIOM_HOST=http://qiita.ucsd.edu:7379`. The default host is **read-only** and administrative functions like loading data will not work against it.

If you intend to **load** your own data, you must setup a local instance (please see the server installation instructions below). In addition, you must explicitly set the `REDBIOM_HOST` environment variable.

# Installation

### General requirements

Redbiom depends on [BIOM](http://biom-format.org/) (tested on >= 2.1.5), [Pandas](http://pandas.pydata.org/) (tested on 0.19.0), [Click](http://click.pocoo.org/5/) (required >= 6.7), [nltk](http://www.nltk.org/) (tested on 3.2.2), [joblib](https://pythonhosted.org/joblib/) (tested on 0.9.3), and [scipy](https://www.scipy.org/) (whatever BIOM is happy with).

### Client

If you would like to use redbiom as only a client (which is the general case), then the following instructions apply.

    $ git clone https://github.com/biocore/redbiom.git
    $ cd redbiom
    $ pip install numpy
    $ pip install -e .

### Server

If you would like to run your own resource, and load data locally or private data, then the following instructions apply.

In addition to the general requirements, redbiom server needs [Redis](https://redis.io/) (tested with 2.8.17 and 3.2.6) and [Webdis](http://webd.is/) (just clone the repo). It is not necessary to have super user access to establish a redbiom server.

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

    $ git clone https://github.com/biocore/redbiom.git
    $ pip install numpy
    $ pip install -e .    

### Testing

The test framework is setup to by default only execute tests against `localhost`, specifically, `127.0.0.1:7379`. However, the repository, by default, is setup to communicate with a remote Webdis server. If you wish to execute the test suite, please `export REDBIOM_HOST=http://127.0.0.1:7379`.

# Terminology and notation

In redbiom, the word "context" refers to a way in which the sample data were processed. Data are loaded into contexts and searches for samples by feature happen within contexts.

To support the one to many relationship between a sample's metadata and its data, within a context, a sample's IDs are prefixed by a "tag" which can be specified at load. Internally, within a context, these IDs are of the form `<tag>_<sample-id>`. The use of the `_` character ensures that they are not valid QIIME sample IDs, and is necessary so we can appropriately differentiate these IDs. Methods which write data will coerce these invalid QIIME IDs into valid IDs of the form `<sample-id>.<tag>`. **IMPORTANT**: if you run your own resource, it is important to specify `--tag` on load of sample data to differentiate BIOM tables in which the sample IDs between the tables may not be mutually exclusive. 

Commands which write data will notify the user if there were ambiguities. An ambiguitiy means that there was a sample ID which mapped to multiple redbiom IDs within the output. The IDs written are unique because of the reasons noted above, 

# Command structure

Redbiom relies on `Click` to provide a tiered command line interface. An example of the first tier is below, and with the exception of `admin`, are composed of verbs:

    $ redbiom --help
    Usage: redbiom [OPTIONS] COMMAND [ARGS]...

    Options:
      --version  Show the version and exit.
      --help     Show this message and exit.

    Commands:
      admin      Update database, etc.
      fetch      Sample data and metadata retrieval.
      search     Feature and sample search support.
      select     Select items based on metadata
      summarize  Summarize things.

The actual commands to execute are contained within a submodule. For instance, below are the commands associated with "search":

    $ redbiom search --help
    Usage: redbiom search [OPTIONS] COMMAND [ARGS]...

      Feature and sample search support.

    Options:
      --help  Show this message and exit.

    Commands:
      metadata      Find samples or categories.
      features      Find samples containing features.
      taxon         Find features associated with a taxon

The intention is for commands to make sense in English. The general command form is "redbiom <verb> <noun>", however this form is not strictly enforced. 

In general, these commands are intended to be composable via Unix pipes. For example:

    redbiom search metadata antibiotics | redbiom fetch samples --context <foo> --output my_table.biom

# Examples

The first example block surrounds loading data, because without anything in the cache, there is nothing fun to do. The second block highlights some example commands that can be run, or chained together, for querying the data loaded.

### Load some data

To make use of this cache, we need to load things. Loading can be done in parallel. First, we'll load up metadata. This will create keys in Redis which describe all of the columns associated with a sample (e.g., `metadata:categories:<sample_id>`, hash buckets for each category and sample combination (e.g., `metadata:category:<category_name>` as the hash and `<sample_id>` as the field), a set of all known categories (e.g., `metadata:categories-represented`), and a set of all known sample IDs (e.g., `metadata:samples-represented`):

    $ redbiom admin load-sample-metadata --metadata path/to/qiime/compat/mapping.txt

redbiom supports one to many mappings between sample metadata and actual sample data. This is done as there may be multiple types of processing performed on the same data (e.g., different nucleotide trims). Or, a physical sample may have been run through multiple protocols (e.g., 16S, WGS, etc). So before we load any data, we need to create a context for the data to be placed. The following action will add an entry into the `state:contexts` hash bucket keyed by `name` and valued by `description`:

    $ redbiom admin create-context --name deblur-100nt --description "16S V4 Caporaso et al data deblurred at 100nt"

Next, we'll load up associations between every single feature in a BIOM table to all the samples its found in. This will create Redis sets and can be accessed using keys of the form `<context_name>:samples:<feature_id>`. Note that we specify the context we're loading into.

    $ redbiom admin load-features --context deblur-100nt --table /path/to/biom/table.biom

Last, let's load up all of the BIOM table data. We'll only store the non-zero values, and we'll encode the sample data into something simple so that it goes in as just a string to Redis. Important: we only support storing count data right now, not floating point. The keys created are of the form `<context_name>:sample:<redbiom_id>`. To reduce space, we reindex the feature IDs as things like sOTUs tend to be very long in name. The mapping is stable over all tables loaded (ie the same feature has the same index), and is stored under `<context_name>:feature-index`. Because we need to update the index, this operation cannot be done in parallel however the code is setup with a redis-based mutex so it's okay to queue up multiple loads.

    $ redbiom load-sample-data --context deblur-100nt --table /path/to/biom/table.biom

### Query for content

Now that things are loaded, we can search for stuff. Let's say you have a few OTUs of interest, and you are curious about what other samples they exist in. You can find that out with:

    $ redbiom search features --context deblur-100nt <space delimited list of feature IDs>

Or, perhaps you loaded the EMP dataset and are curious about where these OTUs reside. You can get summarized environment information from the search as well:

    $ redbiom search features --context deblur-100nt --category empo_3 <space delimited list of feature IDs>

That was fun. So now let's go a little further. Perhaps you are interested not just in where those sequences are found in, but also in the samples themselves for a meta-analysis. To pull out all the samples associated with your IDs of interest, and construct a BIOM table, you can do the following:

    $ redbiom fetch features --context deblur-100nt --output some/path.biom <space delimited list of feature IDs>

...but you probably also want the metadata! Once you have your table, you can obtain it by passing the table back in. This will attempt to grab the metadata (only the columns in common at the moment) for all samples present in your table. Note that we do not have to specify a context here as the sample metadata are context independent:

    $ redbiom fetch sample-metadata --output some/path.txt --table some/path.biom 

# Caveats

Redbiom is still in heavy active development. At this time, there are still some important caveats. 

* Metadata values containing `/` characters cannot be represented the forward slash is used to denote arguments with Webdis. At present, these values are omitted. This is more generally a problem for dates which have not been normalized into an ISO standard. See issue #9.
* Metadata values which appear to be null are not stored. The set of values currently considered nulls are: 
    
    {'Not applicable', 'Unknown', 'Unspecified', 'Missing: Not collected',
     'Missing: Not provided', 'Missing: Restricted access',
     'null', 'NULL', 'no_data', 'None', 'nan'}
     
* Sample IDs must be QIIME compatible.

# Design

### Python and testing
There are a few design decisions in place which deviate from some other typical Python projects. First off, the majority of `import`s are deferred. The motivating force here is to minimize overhead on load as to provide a responsive user interface -- deferred imports are the most straight forward way to accomplish that goal. 

The test harness is broken into multiple components, and are driven by a `Makefile`. This was done initially to be pragmatic as it was easier to write integration tests than unit tests for the `click` commands. These tests can be found in `test.sh` which is composed of "positive" tests and `test_failures.sh` which is composed of "negative" tests. The difference being that the positive tests will fail if any command results in a nonzero exit status, whereas the negative tests expect a nonzero exit status (and really, the decision was to avoid unsetting "-e"). Additional tests which validate some of the Redis contents can be found in `redbiom/tests/test_rest.py`. These are neither unit tests nor integration tests, but simply exercise the behind-the-scenes REST interface. Last, there are a suite of unit tests placed under `redbiom/tests/`. 

### Redis data organization

Because redbiom is currently in alpha, and its data model is subject to change, we are holding off an indepth description of it. That being said, the API methods in general outline the Redis commands issued within their docstrings and can be used to guide interaction. 

The key structures used are in the following forms:

* `state:*` redbiom state information such as context details
* `metadata:category:<category>` the samples and metadata values for the category
* `metadata:categories:<sample-id>` the metadata categories known to exist for a given sample
* `metadata:text-search:<stem>` the samples associated with a given metadata value stem
* `metadata:category-search:<stem>` the categories associated with a given stem
* `metadata:samples-represented` the samples that are represented by the metadata
* `<context>:sample:<redbiom-id>` the sample data within a context
* `<context>:feature:<feature-id>` the feature data within a context
* `<context>:samples-represented` the samples within the context which contain BIOM data
* `<context>:sample-index` a mapping between a sample ID and a context-unique stable integer
* `<context>:sample-index-inverted` a mapping between a context-unique stable integer and its associated sample ID 
* `<context>:features-represented` the reatures represented within the context 
* `<context>:feature-index` a mapping between a feature ID and a context-unique stable integer
* `<context>:feature-index-inverted` a mapping between a context-unique stable integer and its associated feature ID
* `<context>:taxonomy-children:<taxon>` the children of a taxon
* `<context>:taxonomy-parents` child to parent taxon mappings
