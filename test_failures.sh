#!/bin/bash
set -x

# can't do set -e because of the nature of the tests

# blow away the resource, verify we can only load when metadata are present
curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
redbiom admin load-sample-data --table test.biom --context test 2> /dev/null
if [[ "$?" == 0 ]]; then
    echo "Failed"
    exit 1
fi
