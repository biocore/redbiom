REDISCLI=${HOME}/redis-3.2.6/src/redis-cli

test_db:
	date
	curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
	
	redbiom admin scripts-writable
	redbiom admin create-context --name "test" --description "test context"
	redbiom admin load-sample-metadata --metadata test.txt 
	redbiom admin load-sample-metadata-search --metadata test.txt 
	redbiom admin load-sample-data --table test.biom --context test
	
	# with test_has_alts, the input has some overlapping samples (i.e., a 
	# different prep) and some novel IDs. 
	
	# this metadata file contains some new entries and some overlap with the
	# test context above, so we expect that 2 samples will load
	redbiom admin load-sample-metadata --metadata test_with_alts.txt 
	redbiom admin load-sample-metadata-search --metadata test_with_alts.txt 
	
	# only the "novel" samples should load. 
	redbiom admin load-sample-data --table test_with_alts.biom --context test

	# now lets create a separate context to represent a totally different prep
	# and lets just cram it full of the alt data. No additional metadata
	# should get loaded as it is already represented.
	redbiom admin create-context --name "test-alt" --description "test context"

	# we can load metadata, but nothing will get loaded since it already got
	# loaded above. **IMPORTANT** prep specific information is not yet 
	# supported, so this means **ONLY** sample metadata is stored
	redbiom admin load-sample-metadata --metadata test_with_alts.txt 
	redbiom admin load-sample-data --table test_with_alts.biom --context test-alt
	
	redbiom admin scripts-read-only
	date

test: test_db 
	/bin/bash test.sh
	nosetests
	/bin/bash test_failures.sh  # this blows away the db

test_bulk: test_db_bulk
	/bin/bash test.sh
	/bin/bash test_failures.sh  # this blows away the db

test_db_bulk:
	date
	curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
	
	redbiom admin scripts-writable
	redbiom admin create-context --name "test" --description "test context"
	redbiom admin load-sample-metadata --metadata test.txt 
	redbiom admin load-sample-metadata-search --metadata test.txt 
	redbiom admin load-sample-data --table test.biom --context test --mass-insertion | ${REDISCLI} --pipe
	
	# with test_has_alts, the input has some overlapping samples (i.e., a 
	# different prep) and some novel IDs. 
	
	# this metadata file contains some new entries and some overlap with the
	# test context above, so we expect that 2 samples will load
	redbiom admin load-sample-metadata --metadata test_with_alts.txt 
	redbiom admin load-sample-metadata-search --metadata test_with_alts.txt 
	
	# only the "novel" samples should load. 
	redbiom admin load-sample-data --table test_with_alts.biom --context test --mass-insertion | ${REDISCLI} --pipe

	# now lets create a separate context to represent a totally different prep
	# and lets just cram it full of the alt data. No additional metadata
	# should get loaded as it is already represented.
	redbiom admin create-context --name "test-alt" --description "test context"

	# we can load metadata, but nothing will get loaded since it already got
	# loaded above. **IMPORTANT** prep specific information is not yet 
	# supported, so this means **ONLY** sample metadata is stored
	redbiom admin load-sample-metadata --metadata test_with_alts.txt 
	redbiom admin load-sample-data --table test_with_alts.biom --context test-alt --mass-insertion | ${REDISCLI} --pipe
	
	redbiom admin scripts-read-only
	date
