set -e
set -x

function md5test ()
{
    _obs=`sort ${1} | md5 -q`
    _exp=`sort ${2} | md5 -q`
    if [[ "${_obs}" != "${_exp}" ]]; then
        echo "Failed"
        exit 1
    fi
}

query="TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG"

### 
exp="exp_test_query_results.txt"
obs="obs_test_query_results.txt"
echo "10317.000033804" > ${exp}
echo "10317.000047188" >> ${exp} 
echo "10317.000046868" >> ${exp} 

./redbiom search observations ${query} | sort - > ${obs}
md5test ${obs} ${exp}

###
echo ${query} | ./redbiom search observations --from - | sort - > ${obs}
md5test ${obs} ${exp}

###
echo ${query} | ./redbiom fetch observations --output pipetest.biom --from -
python -c "import biom; t = biom.load_table('pipetest.biom'); assert len(t.ids() == 3)"

###
./redbiom fetch samples --output cmdlinetest.biom 10317.000033804 10317.000047188 10317.000046868
python -c "import biom; t = biom.load_table('cmdlinetest.biom'); assert sorted(t.ids()) == ['10317.000033804', '10317.000046868', '10317.000047188']"

###
cat exp_test_query_results.txt | ./redbiom fetch samples --output pipetest.biom --from -
python -c "import biom; t = biom.load_table('pipetest.biom'); assert sorted(t.ids()) == ['10317.000033804', '10317.000046868', '10317.000047188']"

###
./redbiom fetch sample-metadata --output cmdlinetest.txt 10317.000033804 10317.000047188 10317.000046868
obs=$(grep -c FECAL cmdlinetest.txt)
exp=3
if [[ "${obs}" != "${exp}" ]]; then
    echo "Failed"
    exit 1
fi

###
cat exp_test_query_results.txt | ./redbiom fetch sample-metadata --output cmdlinetest.txt --from -
obs=$(grep -c FECAL cmdlinetest.txt)
exp=3
if [[ "${obs}" != "${exp}" ]]; then
    echo "Failed"
    exit 1
fi

###
echo "FECAL\t4" > exp_summarize.txt
echo "SKIN\t1" >> exp_summarize.txt
echo "" >> exp_summarize.txt
echo "Total samples\t5" >> exp_summarize.txt

./redbiom summarize observations --category SIMPLE_BODY_SITE TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

###
echo "FECAL\t2" > exp_summarize.txt
echo "" >> exp_summarize.txt
echo "Total samples\t2" >> exp_summarize.txt

./redbiom summarize observations --exact --category SIMPLE_BODY_SITE TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt

###
echo "10317.000047188"  > exp_summarize.txt
echo "10317.000033804" >> exp_summarize.txt

./redbiom summarize observations --exact --category SIMPLE_BODY_SITE --value "in FECAL,'SKIN'" TACGTAGGTGGCAAGCGTTGTCCGGATTTACTGGGTGTAAAGGGCGTGCAGCCGGGCATGCAAGTCAGATGTGAAATCTCAGGGCTCAACCCTGAAACTG TACGTAGGTGGCAAGCGTTATCCGGAATTATTGGGCGTAAAGCGCGCGTAGGCGGTTTTTTAAGTCTGATGTGAAAGCCCACGGCTCAACCGTGGAGGGT > obs_summarize.txt
md5test obs_summarize.txt exp_summarize.txt



