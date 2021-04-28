#!/bin/bash
set -e
set -x

if [[ "$(uname)" == "Darwin" ]]; then
    md5=md5
else
    md5=md5sum
fi

# https://stackoverflow.com/a/13864829/19741
if [ ! -z ${REDBIOM_HOST+x} ]; then
    if [[ ${REDBIOM_HOST} != *"http://127.0.0.1"* ]]; then
        if [ -z ${REDBIOM_OVERRIDE_HOST_AND_TEST+x} ]; then
            echo "An unexpected host is set for testing, and \$REDBIOM_OVERRIDE_HOST_AND_TEST is not set"
            exit 1
        fi
    fi
fi

alias md5=md5sum
function md5test ()
{
    # biom summarize-table sets locale, making the 
    # presence of a numeric separator difference 
    # across systems. so let's remove "," if we see it.
    _obs=`sort ${1} | tr -d "," | ${md5}`
    _exp=`sort ${2} | tr -d "," | ${md5}`
    if [[ "${_obs}" != "${_exp}" ]]; then
        echo "Failed"
        echo "*****OBS*****"
        cat ${1}
        echo
        echo "*****EXP*****"
        cat ${2}
        exit 1
    fi
}

query="TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG"

# verify we're getting back the expected samples for a simple query
exp="exp_test_query_results.txt"
obs="obs_test_query_results.txt"
echo "UNTAGGED_10317.000033804" > ${exp}
echo "UNTAGGED_10317.000047188" >> ${exp}
echo "UNTAGGED_10317.000046868" >> ${exp}

redbiom search features --context test ${query} | sort - > ${obs}
md5test ${obs} ${exp}

# verify we're getting the expected samples back for a simple query when going via a pipe
echo ${query} | redbiom search features --context test | sort - > ${obs}
md5test ${obs} ${exp}

# fetch sample identifiers
redbiom fetch samples-contained --context test | sort - > test_obs_samples_contained.txt
biom table-ids -i test.biom > test_exp_samples_contained_tmp.txt
biom table-ids -i test_with_alts.biom >> test_exp_samples_contained_tmp.txt
sort test_exp_samples_contained_tmp.txt | uniq > test_exp_samples_contained.txt
md5test test_obs_samples_contained.txt test_exp_samples_contained.txt

# fetch feature identifiers
redbiom fetch features-contained --context test | sort - > test_obs_features_contained.txt
biom table-ids -i test.biom --observations > test_exp_features_contained_tmp.txt
biom table-ids -i test_with_alts.biom --observations >> test_exp_features_contained_tmp.txt
sort test_exp_features_contained_tmp.txt | uniq > test_exp_features_contained.txt
md5test test_obs_features_contained.txt test_exp_features_contained.txt

# fetch samples based on features and sanity check
echo ${query} | redbiom fetch features --context test --output pipetest.biom --from -
python -c "import biom; t = biom.load_table('pipetest.biom'); assert len(t.ids() == 3)"
python -c "import biom; t = biom.load_table('pipetest.biom'); exp = biom.load_table('pipetestexp.biom').filter(t.ids()).filter(lambda v, i, md: (v > 0).sum() > 0, axis='observation').sort_order(t.ids()).sort_order(t.ids(axis='observation'), axis='observation'); exp._observation_metadata = None; t._observation_metadata = None; assert t == exp"

# fetch data via sample
redbiom fetch samples --context test --output cmdlinetest.biom 10317.000033804 10317.000047188 10317.000046868
python -c "import biom; t = biom.load_table('cmdlinetest.biom'); assert sorted(t.ids()) == ['10317.000033804.UNTAGGED', '10317.000046868.UNTAGGED', '10317.000047188.UNTAGGED']"

# we do _NOT_ expect the qiime compatible ID "10317.000046868.UNTAGGED" to work.
# this is because we cannot safely convert it into a redbiom ID as we cannot
# assume it is safe to rsplit('.', 1) on it as "." is a valid sample ID
# character
redbiom fetch samples --context test --output cmdlinetest.biom 10317.000033804 UNTAGGED_10317.000047188
python -c "import biom; t = biom.load_table('cmdlinetest.biom'); assert sorted(t.ids()) == ['10317.000033804.UNTAGGED', '10317.000047188.UNTAGGED']"

# fetch data via sample and via pipe
cat exp_test_query_results.txt | redbiom fetch samples --context test --output pipetest.biom --from -
python -c "import biom; t = biom.load_table('pipetest.biom'); assert sorted(t.ids()) == ['10317.000033804.UNTAGGED', '10317.000046868.UNTAGGED', '10317.000047188.UNTAGGED']"

# fetch sample metadata
redbiom fetch sample-metadata --output cmdlinetest.txt UNTAGGED_10317.000033804 10317.000047188 10317.000046868
obs=$(grep -c FECAL cmdlinetest.txt)
exp=3
if [[ "${obs}" != "${exp}" ]]; then
    echo "Failed"
    exit 1
fi

# fetch sample metadata via pipe
cat exp_test_query_results.txt | redbiom fetch sample-metadata --output cmdlinetest.txt --from -
obs=$(grep -c FECAL cmdlinetest.txt)
exp=3
if [[ "${obs}" != "${exp}" ]]; then
    echo "Failed"
    exit 1
fi

# summarize samples from features
echo "FECAL	4" > exp_summarize.txt
echo "SKIN	1" >> exp_summarize.txt
echo "" >> exp_summarize.txt
echo "Total samples	5" >> exp_summarize.txt

redbiom summarize features --context test --category SIMPLE_BODY_SITE TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

# summarize samples from features in which all samples contain all requested observations
echo "FECAL	2" > exp_summarize.txt
echo "" >> exp_summarize.txt
echo "Total samples	2" >> exp_summarize.txt

redbiom summarize features --exact --context test --category SIMPLE_BODY_SITE TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

# pull out a selection of the summarized samples
echo "UNTAGGED_10317.000047188"  > exp_summarize.txt
echo "UNTAGGED_10317.000033804" >> exp_summarize.txt

redbiom search features --exact --context test TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT | redbiom select samples-from-metadata --context test "where SIMPLE_BODY_SITE in ('FECAL', 'SKIN')" > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

# round trip the sample data
# Does not currently support passing in QIIME compatible versions of redbiom IDs
#python -c "import biom; t = biom.load_table('test.biom'); print('\n'.join(t.ids()))" | redbiom fetch samples --context test --from - --output observed.biom
#python -c "import biom; obs = biom.load_table('observed.biom'); exp = biom.load_table('test.biom').sort_order(obs.ids()).sort_order(obs.ids(axis='observation'), axis='observation'); assert obs == exp"

# pull out a metadata category
echo "Category value	count" > exp_metadata_categories.txt
echo "2	1" >> exp_metadata_categories.txt
echo "21.0	1" >> exp_metadata_categories.txt
echo "24.0	1" >> exp_metadata_categories.txt
echo "32.0	1" >> exp_metadata_categories.txt
echo "33	2" >> exp_metadata_categories.txt
echo "33.0	1" >> exp_metadata_categories.txt
echo "39.0	2" >> exp_metadata_categories.txt
echo "48.0	1" >> exp_metadata_categories.txt
echo "59	1" >> exp_metadata_categories.txt
redbiom summarize metadata-category --category AGE_YEARS --counter --sort-index > obs_metadata_categories.txt
md5test obs_metadata_categories.txt exp_metadata_categories.txt

# check counts for a few metadata categories
echo "AGE_YEARS	11" > exp_metadata_counts.txt
echo "BODY_SITE	12" >> exp_metadata_counts.txt
redbiom summarize metadata | grep AGE_YEARS > obs_metadata_counts.txt
redbiom summarize metadata | grep "^BODY_SITE" >> obs_metadata_counts.txt
md5test obs_metadata_counts.txt exp_metadata_counts.txt

# load table with some duplicate sample IDs
head -n 1 test.txt > test.with_dups.txt
tail -n 2 test.txt >> test.with_dups.txt
tail -n 1 test.txt | sed 's/^10317\.[0-9]*/anewID/' >> test.with_dups.txt
echo "Loaded 1 samples" > exp_load_count.txt
redbiom admin load-sample-metadata --metadata test.with_dups.txt > obs_load_count.txt
md5test obs_load_count.txt exp_load_count.txt
echo "SKIN	1" > exp_anewid.txt
redbiom summarize samples --category SIMPLE_BODY_SITE anewID | grep SKIN > obs_anewid.txt
md5test obs_anewid.txt exp_anewid.txt

redbiom search metadata "where AGE_YEARS > 40" | redbiom fetch samples --context test --output metadata_search_test.biom
echo "Num samples: 2" > exp_metadata_search.txt
echo "Num observations: 425" >> exp_metadata_search.txt
echo "Total count: 21,462" >> exp_metadata_search.txt
biom summarize-table -i metadata_search_test.biom | head -n 3 > obs_metadata_search.txt
md5test obs_metadata_search.txt exp_metadata_search.txt

redbiom search metadata "where AGE_YEARS > 40" | redbiom fetch sample-metadata --context test --output metadata_search_test.txt
echo "10317.000012975.UNTAGGED" > exp_metadata_search.txt
echo "10317.000047188.UNTAGGED" >> exp_metadata_search.txt
cut -f 1 metadata_search_test.txt | grep -v "^#" | sort - > obs_metadata_search.txt
md5test obs_metadata_search.txt exp_metadata_search.txt

echo "ContextName	SamplesWithData	FeaturesWithData	Description" > exp_contexts.txt
echo "test-alt	5	666	test context" >> exp_contexts.txt
echo "test	12	925	test context" >> exp_contexts.txt
echo "" >> exp_contexts.txt
redbiom summarize contexts > obs_contexts.txt
md5test obs_contexts.txt exp_contexts.txt

# exercise table summary
redbiom summarize table --table test.biom --context test --category COUNTRY --output obs_tablesummary_full.txt
echo "feature	Australia	USA	United Kingdom" | tr "	" "\n" | sort > exp_tablesummary.txt
head -n 1 obs_tablesummary_full.txt | tr "	" "\n" | sort > obs_tablesummary.txt
md5test obs_tablesummary.txt exp_tablesummary.txt
if [[ "$(wc -l obs_tablesummary_full.txt | awk '{ print $1 }')" != "924" ]];
then
    echo "fail"
    exit 1
fi

echo "10317.000033804" > exp_metadata_full.txt
echo "10317.000001378" >> exp_metadata_full.txt
redbiom search metadata "antibiotics where AGE_YEARS < 25" > obs_metadata_full.txt
md5test obs_metadata_full.txt exp_metadata_full.txt

obs=$(redbiom select features-from-samples --context test 10317.000047188 10317.000005080 | wc -l | awk '{ print $1 }')
exp=492
if [[ "$obs" != "$exp" ]]; then
    echo "fail"
    exit 1
fi

redbiom search metadata "where AGE_YEARS > 40" | redbiom select features-from-samples --context test | redbiom summarize taxonomy --context test | grep "^p__" | sort > obs_taxonomy.txt
echo "p__Actinobacteria	14	0.0329" > exp_taxonomy.txt
echo "p__Bacteroidetes	43	0.1012" >> exp_taxonomy.txt
echo "p__Euryarchaeota	1	0.0024" >> exp_taxonomy.txt
echo "p__Firmicutes	334	0.7859" >> exp_taxonomy.txt
echo "p__Lentisphaerae	4	0.0094" >> exp_taxonomy.txt
echo "p__Proteobacteria	10	0.0235" >> exp_taxonomy.txt
echo "p__Tenericutes	15	0.0353" >> exp_taxonomy.txt
echo "p__Verrucomicrobia	2	0.0047" >> exp_taxonomy.txt
md5test obs_taxonomy.txt exp_taxonomy.txt

# test getting features in samples
python -c "import biom; t = biom.load_table('test.biom'); print('\n'.join(sorted(t.ids(axis='observation')[t.data('10317.000003302') > 0])))" > exp_sample_search.txt
redbiom search samples --context test 10317.000003302 | sort - > obs_sample_search.txt
redbiom search samples --context test UNTAGGED_10317.000003302 | sort - > obs_sample_search_rbid.txt
md5test obs_sample_search.txt exp_sample_search.txt
md5test obs_sample_search_rbid.txt exp_sample_search.txt
