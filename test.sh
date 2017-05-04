#!/bin/bash
set -e
set -x

if [[ "$(uname)" == "Darwin" ]]; then
    md5=md5
else
    md5=md5sum
fi

alias md5=md5sum
function md5test ()
{
    _obs=`sort ${1} | ${md5}`
    _exp=`sort ${2} | ${md5}`
    if [[ "${_obs}" != "${_exp}" ]]; then
        echo "Failed"
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

redbiom search observations --context test ${query} | sort - > ${obs}
md5test ${obs} ${exp}

# verify we're getting the expected samples back for a simple query when going via a pipe
echo ${query} | redbiom search observations --context test | sort - > ${obs}
md5test ${obs} ${exp}

# fetch samples based on observations and sanity check
echo ${query} | redbiom fetch observations --context test --output pipetest.biom --from -
python -c "import biom; t = biom.load_table('pipetest.biom'); assert len(t.ids() == 3)"
python -c "import biom; t = biom.load_table('pipetest.biom'); exp = biom.load_table('pipetestexp.biom').filter(t.ids()).filter(lambda v, i, md: (v > 0).sum() > 0, axis='observation').sort_order(t.ids()).sort_order(t.ids(axis='observation'), axis='observation'); assert t == exp"

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

# summarize samples from observations
echo "FECAL	4" > exp_summarize.txt
echo "SKIN	1" >> exp_summarize.txt
echo "" >> exp_summarize.txt
echo "Total samples	5" >> exp_summarize.txt

redbiom summarize observations --context test --category SIMPLE_BODY_SITE TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

# summarize samples from observations in which all samples contain all requested observations
echo "FECAL	2" > exp_summarize.txt
echo "" >> exp_summarize.txt
echo "Total samples	2" >> exp_summarize.txt

redbiom summarize observations --exact --context test --category SIMPLE_BODY_SITE TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

# pull out a selection of the summarized samples
echo "UNTAGGED_10317.000047188"  > exp_summarize.txt
echo "UNTAGGED_10317.000033804" >> exp_summarize.txt

redbiom search observations --exact --context test TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT | redbiom select samples-from-metadata --context test "where SIMPLE_BODY_SITE in ('FECAL', 'SKIN')" > obs_summarize.txt
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
tail -n 1 test.txt | sed -s 's/^10317\.[0-9]*/anewID/' >> test.with_dups.txt
echo "Loaded 1 samples" > exp_load_count.txt
redbiom admin load-sample-metadata --metadata test.with_dups.txt > obs_load_count.txt
md5test obs_load_count.txt exp_load_count.txt
echo "SKIN	1" > exp_anewid.txt
redbiom summarize samples --category SIMPLE_BODY_SITE anewID | grep SKIN > obs_anewid.txt
md5test obs_anewid.txt exp_anewid.txt

redbiom search metadata "where AGE_YEARS > 40" | redbiom fetch samples --context test --output metadata_search_test.biom
echo "Num samples: 2" > exp_metadata_search.txt
echo "Num observations: 427" >> exp_metadata_search.txt
echo "Total count: 21780" >> exp_metadata_search.txt
biom summarize-table -i metadata_search_test.biom | head -n 3 > obs_metadata_search.txt
md5test obs_metadata_search.txt exp_metadata_search.txt

echo "#ContextName	SamplesWithData	SamplesWithObservations	Description" > exp_contexts.txt
echo "test	12	12	test context" >> exp_contexts.txt
echo "test_alt	5	5	test context" >> exp_contexts.txt
redbiom summarize contexts > obs_contexts.txt

# exercise table summary
redbiom summarize table --table test.biom --context test --category COUNTRY --output obs_tablesummary_full.txt
echo "feature	Australia	USA	United Kingdom" > exp_tablesummary.txt
head -n 1 obs_tablesummary_full.txt > obs_tablesummary.txt
md5test obs_tablesummary.txt exp_tablesummary.txt
if [[ "$(wc -l obs_tablesummary_full.txt | awk '{ print $1 }')" != "930" ]];
then
    echo "fail"
    exit 1
fi

echo "10317.000033804" > exp_metadata_full.txt
echo "10317.000001378" >> exp_metadata_full.txt
redbiom search metadata "antibiotics where AGE_YEARS < 25" > obs_metadata_full.txt
md5test obs_metadata_full.txt exp_metadata_full.txt
