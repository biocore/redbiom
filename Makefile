test:
	curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
	./redbiom admin load-observations --table test.biom
	./redbiom admin load-sample-metadata --metadata test.txt
	./redbiom admin load-sample-data --table test.biom
	python test.py
	sh test.sh
