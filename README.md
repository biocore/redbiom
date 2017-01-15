# What is this?

Load BIOM tables and sample metadata from a lot of studies into Redis to facilitate search using observation IDs or metadata.

It is safe to say the interfaces are not stable.

# Installation

Needs Redis (tested with 3.2.6) and Webdis (just clone the repo). Also depends on BIOM (tested on 2.1.5-dev), Pandas (tested on 0.19.0), Click (tested on 6.6) and scipy (whatever BIOM is happy with).

# Load some stuff

To make use of this cache, we need to load things. Loading can be done in parallel. First, we'll load up metadata. This will create keys in Redis which describe all of the columns associated with a sample (e.g., `metadata-categories:<sample_id>` and hash buckets for each category and sample combination (e.g., `category:<category_name>` as the hash and `<sample_id>` as the field). 

	$ ./redbiom admin load-sample-metadata --metadata path/to/qiime/compat/mapping.txt

Next, we'll load up associations between every single observation in the BIOM table to all the samples its found in. This will create Redis sets and can be accessed using keys of the form `samples:<observation_id>`. 

	$ ./redbiom admin load-observations --table /path/to/biom/table.biom

For icing on the cake, lets also load up all of the BIOM table data because why not. We'll only store the non-zero values, and we'll encode the sample data into something simple so that it goes in as just a string to Redis. Important: we only support storing count data right now, not floating point. The keys created are of the form `data:<sample_id>`. To reduce space, we reindex the observation IDs as things like sOTUs tend to be very long in name. The mapping is stable over all tables loaded (ie the same observation has the same index), and the index is stored as a JSON object under the key `__observation_index`. Because we need to update the index, this operation cannot be done in parallel however the code is setup with a redis-based mutex so it's okay to queue up multiple loads.

	$ ./redbiom load-sample-data --table /path/to/biom/table.biom

# Get you some stuff

Now that things are loaded, we can search for stuff. Let's say you have a few OTUs of interest, and you are curious about what other samples they exist in. You can find that out with:

	$ ./redbiom search observations <space delimited list of observation IDs>

Or, perhaps you loaded the EMP dataset and are curious about where these OTUs reside. You can get summarized environment information from the search as well:


	$ ./redbiom search observations --category EMPO_3 <space delimited list of observation IDs>

That was fun. So now let's go a little further. Perhaps you are interested not just in where those sequences are found in, but also in the samples themselves for a meta-analysis. To pull out all the samples associated with your IDs of interest, and construct a BIOM table, you can do the following:

	$ ./redbiom fetch observations --output some/path.biom <space delimited list of observation IDs>

...but you probably also want the metadata! Once you have your table, you can obtain it by passing the table back in. This will attempt to grab the metadata (only the columns in common at the moment) for all samples present in your table:

	$ ./redbiom fetch sample-metadata --output some/path.txt --table some/path.biom 

Last, what about finding samples of interest based off the metadata? Simple! You can get lists of samples from a singular query at the moment (e.g., all samples with "ph < 5"). We haven't built support yet for pulling out the BIOM data from those samples just yet though, but that's close. And of course, what you really want to do are complex queries combining things like "(ph < 5) and (altitude > 1000)" but we aren't there just yet. The following does the "ph < 5" and prints the list of samples and their ph value.
