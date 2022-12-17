#!/bin/bash

set -x 
set -e

unset REDBIOM_HOST

# WARNING: the context is not assured to be stable. It is possible
# that in the future, the context name will change and this test
# will fail. However, that is unlikely to happen anytime soon so
# let's do the simple thing of hard coding it.
redbiom fetch qiita-study \
    --study-id 2136 \
    --context Deblur_2021.09-Illumina-16S-V4-90nt-dd6875 \
    --output-basename 2136test \
    --resolve-ambiguities most-reads \
    --remove-blanks

in_table=$(biom table-ids -i 2136test.biom | wc -l)
in_md=$(grep "^2136" 2136test.tsv | wc -l)
if [[ ${in_table} != ${in_md} ]];
then
    exit 1
fi

if [[ ! ${in_table} -eq 450 ]];
then
    exit 1
fi
