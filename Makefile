test_db:
	curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
	redbiom admin create-context --name "test" --description "test context"
	redbiom admin load-sample-metadata --metadata test.txt 
	redbiom admin load-observations --table test.biom --context test
	redbiom admin load-sample-data --table test.biom --context test

test: test_db 
	/bin/bash test.sh
	nosetests
	/bin/bash test_failures.sh  # this blows away the db
