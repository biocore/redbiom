test:
	curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
	redbiom admin create-context --name "test" --description "test context"
	redbiom admin load-sample-metadata --metadata test.txt 
	redbiom admin load-observations --table test.biom --context test
	redbiom admin load-sample-data --table test.biom --context test
	/bin/bash test.sh
	python test.py
