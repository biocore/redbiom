#!/bin/bash
set -x

# can't do set -e because of the nature of the tests

# verify duplicate loads fail
redbiom admin load-sample-metadata --metadata test.txt 2> /dev/null
if [[ "$?" == 0 ]]; then
    echo "Failed"
    exit 1
fi

redbiom admin load-observations --table test.biom --context test 2> /dev/null
if [[ "$?" == 0 ]]; then
    echo "Failed"
    exit 1
fi

redbiom admin load-sample-data --table test.biom --context test 2> /dev/null
if [[ "$?" == 0 ]]; then
    echo "Failed"
    exit 1
fi
	
# blow away the resource, verify we can only load when metadata are present
curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
redbiom admin load-observations --table test.biom --context test 2> /dev/null
if [[ "$?" == 0 ]]; then
    echo "Failed"
    exit 1
fi

redbiom admin load-sample-data --table test.biom --context test 2> /dev/null
if [[ "$?" == 0 ]]; then
    echo "Failed"
    exit 1
fi
