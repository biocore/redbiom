test:
	curl -s http://127.0.0.1:7379/FLUSHALL > /dev/null
	./redbiom update_observations --table test.biom
	./redbiom update_metadata --metadata test.txt
	./redbiom load_table --table test.biom
	python test.py
	sh test.sh
