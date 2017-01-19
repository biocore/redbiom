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
echo "10317.000033804" > ${exp}
echo "10317.000047188" >> ${exp} 
echo "10317.000046868" >> ${exp} 

redbiom search observations --context test ${query} | sort - > ${obs}
md5test ${obs} ${exp}

# verify we're getting the expected samples back for a simple query when going via a pipe
echo ${query} | redbiom search observations --context test --from - | sort - > ${obs}
md5test ${obs} ${exp}

# fetch samples based on observations and sanity check
echo ${query} | redbiom fetch observations --context test --output pipetest.biom --from -
python -c "import biom; t = biom.load_table('pipetest.biom'); assert len(t.ids() == 3)"
python -c "import biom; t = biom.load_table('pipetest.biom'); exp = biom.load_table('test.biom').filter(t.ids()).filter(lambda v, i, md: (v > 0).sum() > 0, axis='observation').sort_order(t.ids()).sort_order(t.ids(axis='observation'), axis='observation'); assert t == exp"

# fetch data via sample
redbiom fetch samples --context test --output cmdlinetest.biom 10317.000033804 10317.000047188 10317.000046868
python -c "import biom; t = biom.load_table('cmdlinetest.biom'); assert sorted(t.ids()) == ['10317.000033804', '10317.000046868', '10317.000047188']"

# fetch data via sample and via pipe
cat exp_test_query_results.txt | redbiom fetch samples --context test --output pipetest.biom --from -
python -c "import biom; t = biom.load_table('pipetest.biom'); assert sorted(t.ids()) == ['10317.000033804', '10317.000046868', '10317.000047188']"

# fetch sample metadata
redbiom fetch sample-metadata --output cmdlinetest.txt 10317.000033804 10317.000047188 10317.000046868
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
echo "10317.000047188"  > exp_summarize.txt
echo "10317.000033804" >> exp_summarize.txt

redbiom summarize observations --exact --context test --category SIMPLE_BODY_SITE --value "in FECAL,'SKIN'" TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

# round trip the sample data
python -c "import biom; t = biom.load_table('test.biom'); print('\n'.join(t.ids()))" | redbiom fetch samples --context test --from - --output observed.biom
python -c "import biom; obs = biom.load_table('observed.biom'); exp = biom.load_table('test.biom').sort_order(obs.ids()).sort_order(obs.ids(axis='observation'), axis='observation'); assert obs == exp"

