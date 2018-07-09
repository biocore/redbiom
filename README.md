# ![redbiom](https://raw.githubusercontent.com/biocore/redbiom/master/logo.png)
# ![build-status](https://travis-ci.org/biocore/redbiom.svg?branch=master)

# What is this?

Redbiom is a cache service for sample metadata and sample data. It allows for rapidly:

* finding samples by the features they contain
* finding samples by arbitrary metadata searches
* summarizing samples over metadata
* retieval of sample data into BIOM
* discovering metadata categories
* pulling out sample data from different processing types (e.g., search over 16S, retrieve WGS)

redbiom is designed to handle biological and technical replicates. Specifically, it allows for a one to many relationship between a sample's metadata and its data, both within and between preparation types.

This repository defines the de facto redbiom data representation, and one possible interface into the resource. Other interfaces (e.g., Javascript) are possible to define. Please see the Design section below for details about how other interfaces can be written.

By default, redbiom will search against `qiita.ucsd.edu:7329`. This can be changed at runtime by setting the `REDBIOM_HOST` environmental variable, e.g., `export REDBIOM_HOST=http://qiita.ucsd.edu:7329`. The default host is **read-only** and administrative functions like loading data will not work against it.

If you intend to **load** your own data, you must setup a local instance (please see the server installation instructions below). In addition, you must explicitly set the `REDBIOM_HOST` environment variable.

# Very brief examples

A few quick examples of what can be done. More complex and detailed examples can be found later in the document.

Get all the samples in which the word "beer" is found:

    $ redbiom search metadata beer | head
    10105.Ingredient.18
    1976.Beer.286
    1689.261
    10105.Ingredient.19
    1976.Beer.262
    10105.Ingredient.23
    1976.Beer.228
    10105.Ingredient.17
    10395.000041066
    10105.Ingredient.24

Get the closed reference OTU picking 16S V4 data for those samples (more on what `ctx` and `context` is in the longer examples below):

    $ export ctx=Pick_closed-reference_OTUs-illumina-16S-v4-66f541
    $ redbiom search metadata beer | head | redbiom fetch samples --context $ctx --output beer_example.biom
    $ redbiom search metadata beer | head | redbiom fetch sample-metadata --context $ctx --output beer_example.txt

Find the feature IDs (Greengenes OTU IDs in this case) associated with S. aureus (and for example purposes, an arbitrary 10):

    $ redbiom search taxon --context $ctx s__aureus | head
    943389
    1023241
    862312
    1102743
    870118
    969777
    1086805
    976010
    951488
    951136

...and then find samples which contain those 10 S. aureus features:

    $ redbiom search taxon --context $ctx s__aureus | head | redbiom search features --context $ctx | wc -l
       40164

# Installation

### General requirements

Redbiom depends on [BIOM](http://biom-format.org/) (tested on >= 2.1.5), [Pandas](http://pandas.pydata.org/) (tested on 0.19.0), [Click](http://click.pocoo.org/5/) (required >= 6.7), [nltk](http://www.nltk.org/) (tested on 3.2.2), [joblib](https://pythonhosted.org/joblib/) (tested on 0.9.3), and [scipy](https://www.scipy.org/) (whatever BIOM is happy with).

### Client

If you would like to use redbiom as only a client (which is the general case), then the following instructions apply. Note that we need to install numpy separately as one of the dependencies, BIOM-Format, imports numpy within its installation process.

    $ pip install numpy
    $ pip install redbiom

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

    $ pip install numpy
    $ pip install redbiom

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

### Search for samples by metadata

By default, redbiom is setup to query against [Qiita](https://qiita.ucsd.edu). First, let's search for some samples by metadata. Specifically, what we're going to do is identify what samples exist in Qiita in which any of their sample metadata contains the [stem](https://en.wikipedia.org/wiki/Stemming) of the word beer. This returns quite a few samples, so for the sake of the example, we're only going to show the first 10 using `head`: 

    $ redbiom search metadata beer | head
    10105.Ingredient.18
    1976.Beer.286
    1689.261
    10105.Ingredient.19
    1976.Beer.262
    10105.Ingredient.23
    1976.Beer.228
    10105.Ingredient.17
    10395.000041066
    10105.Ingredient.24

    $ redbiom search metadata beer | wc -l
      70

Now that we have some samples, let's pull out their sample data. Qiita contains a huge amount of data, which are logically partitioned by the sample preparations and processing parameters -- these partitions are denoted as **contexts** in redbiom. In order to pull out the data, we need to specify the context to operate in. There are a lot of contexts, so let's filter to only those which are 16S and V4 using `grep`. We're also going to `cut` the first three columns of data as the fourth one is a voluminous description of the processing parameters. And last, let's `sort` the results by the number of samples represented in the context. Unfortunately, the `grep` removes the column headers, so we'll run a second summarize command and just grab the header:

    $ redbiom summarize contexts | cut -f 1,2,3 | grep 16S-v4 | sort -k 2 -n
    Pick_closed-reference_OTUs-illumina-16S-v45-66f541  102 29598
    Pick_closed-reference_OTUs-flx-16S-v4-66f541    116 4699
    Pick_closed-reference_OTUs-ls454-16S-v4-66f541  145 8437
    Pick_closed-reference_OTUs-titanium-16S-v46-66f541  214 3568
    Pick_closed-reference_OTUs-titanium-16S-v4-66f541   800 14269
    deblur-workflow-illumina-16S-v4-150nt-ae489c    24613   1932042
    deblur-workflow-illumina-16S-v4-100nt-ae489c    60150   3738847
    deblur-workflow-illumina-16S-v4-90nt-ae489c 65143   3162632
    Pick_closed-reference_OTUs-illumina-16S-v4-66f541   89405   84828
    
    $ redbiom summarize contexts | head -n 1
    ContextName SamplesWithData FeaturesWithData    Description

To reduce typing later, let's just pick a context and store it as an environment variable:

    $ export ctx=Pick_closed-reference_OTUs-illumina-16S-v4-66f541

...and now we can grab some data:

    $ redbiom search metadata beer | redbiom fetch samples --context $ctx --output example.biom
    $ biom summarize-table -i example.biom | head
    Num samples: 37
    Num observations: 3653
    Total count: 2205617
    Table density (fraction of non-zero values): 0.091

    Counts/sample summary:
     Min: 1717.0
     Max: 208223.0
     Median: 59224.000
     Mean: 59611.270
    
We probably also want to get the sample metadata:

    $ redbiom search metadata beer | redbiom fetch sample-metadata --output example.txt --context $ctx

You might note that the total number of samples found by the metadata search is not the same as the number of samples found by the sample data fetch. The sample information is distinct from the sample preparation, and data processing: just because there is sample information does not mean a given sample has (for instance) 16S V4 sequence data associated with it.

The query structures for metadata are fairly permissive, and there are actually two types of queries that can be performed. The structure is as follows: `<set operations> where <value restrictions>`. The `<set operations>` work by finding all samples with that contain a given word, which can be combined together. For the set queries, `&` performs an intersection of the sample IDs, `|` a union, and `-` a difference:

    $ redbiom search metadata "soil & europe where ph < 7" | wc -l
    5521

**IMPORTANT**: just because a sample may have a word associated with it, does not mean that word is used as you may expect. In the example below, we're counting the number of samples by their described `sample_type` value. We are working to improve the search functionality, and it is important for users to scrutinize their results:

    $ redbiom search metadata "soil & europe where ph < 7" | redbiom summarize samples --category sample_type  | head
    soil    1978
    XXQIITAXX   1686
    Soil    612
    fresh water 519
    peat    192
    sebum   99
    bodily fluid    81
    belly   41
    biofilm 39
    ab_liq  38 

### Search by feature

We can also use redbiom to search for samples containing features of interest. Let's operate off our example table from the metadata search above. What we're going to do is find all samples in Qiita that contain any of the a handful of the feature IDs. In this particular example, let's just grab 10 arbitrary IDs:

    $ biom table-ids -i example.biom --observations | head
    4449525
    4420570
    471180
    815819
    4235445
    1108951
    519367
    12364
    4454153
    4227110

...and then let's pipe them back into redbiom to search for other samples in our context which contain those same features:

    $ biom table-ids -i example.biom --observations | head | redbiom search features --context $ctx | wc -l
       43133

    $ biom table-ids -i example.biom --observations | head | redbiom search features --context $ctx | head
    3759_10172.3338
    2923_10317.000017653
    2096_1716.McG.PAPrS17
    2015_1034.CHB1
    2150_755.SSFA.L1.D30.07.06.11.lane1.NoIndex.L001
    2150_755.LSSF.ALPHA.D20.14.07.11.lane1.NoIndex.L001
    26483_10317.000007237
    3788_10119.MT.741
    2112_1774.527.Skin.Puer
    2102_1734.BD.ERD510

### Search by taxon

One thing you might want to do is find features based on taxonomy. We can do this by searching for a taxon:

    $ redbiom search taxon g__Roseburia --context $ctx | wc -l
         108

What we get back are the feature IDs that are of that taxon. We can then take those feature IDs and feed them back into redbiom. So for instance, let's say we wanted to find all samples which contain a Roseburia feature:

    $ redbiom search taxon g__Roseburia --context $ctx | redbiom search features --context $ctx | wc -l
       37539

**IMPORTANT** not all contexts necessarily have taxonomy, and taxonomy may not make sense for a context (e.g., if it contains KEGG Orthologous group features).

### Summarizations

We found a lot of samples that contain Roseburia. That isn't too surprising since Qiita contains a lot of fecal samples. How many? In this next example, we're taking all of the feature IDs associated with Roseburia, then finding all of the samples which contain that taxon, followed by binning each sample by their `sample_type` category value, and finally we're taking just the top 10 entries. You can see that the metadata are a bit noisy.

	$ redbiom search taxon g__Roseburia --context $ctx | redbiom search features --context $ctx | redbiom summarize samples --category sample_type | head
	Stool	13251
	stool	11416
	XXQIITAXX	1029
	tanker milk	984
	biopsy	930
	Floor	622
	skin	615
	Stool_Stabilizer	566
	control blank	520
	Mouth	420

We can still work through the noise though. Let's take our samples we found that contain Roseburia, and only select the ones that appear to obviously be fecal. Instead of summarizing as we did in our last example, we're going to "select" the samples in which `sample_type` is either "Stool" or "stool". (as this command is getting long, we'll break it up with \\):

	$ redbiom search taxon g__Roseburia --context $ctx | \
		redbiom search features --context $ctx | \
		redbiom select samples-from-metadata --context $ctx "where sample_type in ('Stool', 'stool')" | \
		wc -l
	   24667

And last, we can grab the data for those samples. Fetching data for 24,667 samples can take a few minutes, so for the purpose of the example, let's just grab the ones associated with skin. Please note the "ambiguity" on the output, more in a second on that:

	$ redbiom search taxon g__Roseburia --context $ctx | \
        redbiom search features --context $ctx | \
        redbiom select samples-from-metadata --context $ctx "where sample_type=='skin'" | \
        redbiom fetch samples --context $ctx --output roseburia_example.biom
	16 sample ambiguities observed. Writing ambiguity mappings to: roseburia_example.biom.ambiguities

Ambiguities can arise if the same sample was processed multiple times as might happen with a technical replicate. It is the same physical sample, but it may have been processed multiple times. The `.ambiguities` file is in JSON and contains a mapping of what IDs map to the same sample.

### Load some data (i.e., if you are running your own server)

To make use of this cache, we need to load things. Loading can be done in parallel. First, we'll load up metadata. This will create keys in Redis which describe all of the columns associated with a sample (e.g., `metadata:categories:<sample_id>`, hash buckets for each category and sample combination (e.g., `metadata:category:<category_name>` as the hash and `<sample_id>` as the field), a set of all known categories (e.g., `metadata:categories-represented`), and a set of all known sample IDs (e.g., `metadata:samples-represented`):

    $ redbiom admin load-sample-metadata --metadata path/to/qiime/compat/mapping.txt

redbiom supports one to many mappings between sample metadata and actual sample data. This is done as there may be multiple types of processing performed on the same data (e.g., different nucleotide trims). Or, a physical sample may have been run through multiple protocols (e.g., 16S, WGS, etc). So before we load any data, we need to create a context for the data to be placed. The following action will add an entry into the `state:contexts` hash bucket keyed by `name` and valued by `description`:

    $ redbiom admin create-context --name deblur-100nt --description "16S V4 Caporaso et al data deblurred at 100nt"

Next, we'll load up associations between every single feature in a BIOM table to all the samples its found in. This will create Redis sets and can be accessed using keys of the form `<context_name>:samples:<feature_id>`. Note that we specify the context we're loading into.

    $ redbiom admin load-features --context deblur-100nt --table /path/to/biom/table.biom

Last, let's load up all of the BIOM table data. We'll only store the non-zero values, and we'll encode the sample data into something simple so that it goes in as just a string to Redis. Important: we only support storing count data right now, not floating point. The keys created are of the form `<context_name>:sample:<redbiom_id>`. To reduce space, we reindex the feature IDs as things like sOTUs tend to be very long in name. The mapping is stable over all tables loaded (ie the same feature has the same index), and is stored under `<context_name>:feature-index`. Because we need to update the index, this operation cannot be done in parallel however the code is setup with a redis-based mutex so it's okay to queue up multiple loads.

    $ redbiom load-sample-data --context deblur-100nt --table /path/to/biom/table.biom

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
