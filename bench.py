import h5py
import sys
import time
import random
import requests
import json
import collections

some_id = random.randint(0, 10000)

with h5py.File(sys.argv[1]) as fp:
    ids = list(fp['observation/ids'][:])
random.shuffle(ids)
# this one spans a lot of samples
# ids.insert(0, 'TACGGAGGGAGCTAGCGTTGTTCGGAATTACTGGGCGTAAAGAGTACGTAGGCGGTGATTCAAGTCAGAGGTGAAAGCCTGGAGCTCAAC')
empo_3 = []
samples = set()
start = time.time()

with requests.Session() as session:
    #for i in ids[:int(sys.argv[2])]:
    #    req = session.get('http://127.0.0.1:7379/SMEMBERS/samples:%s' % i)
    #    if req.status_code != 200:
    #        print(i)
    #        print(req.status_code)
    #        print(req.content)
    #        raise ValueError()
    #    samples.update(set(req.json()['SMEMBERS']))
    for chunk in range(0, int(sys.argv[2]), 50):
        bulk = '/'.join(['samples:%s' % i for i in ids[chunk:chunk+50]])
        req = session.get('http://127.0.0.1:7379/SUNION/%s' % bulk)
        if req.status_code != 200:
            print(i)
            print(req.status_code)
            print(req.content)
            raise ValueError()
        samples.update(set(req.json()['SUNION']))

samples = list(samples)
with requests.Session() as session:
    for chunk in range(0, len(samples), 500):
        sample_slice = samples[chunk:chunk+500]
        req = session.get('http://127.0.0.1:7379/HMGET/category:empo_3/%s' % '/'.join(sample_slice))
        if req.status_code != 200:
            print(i)
            print(req.status_code)
            print(req.content)
            raise ValueError()
        empo_3.extend(req.json()['HMGET'])
#    for s in samples:
 #       req = requests.get('http://127.0.0.1:7379/HGET/metadata:%s/empo_3' % s)
  #      if req.status_code != 200:
   #         raise ValueError()
    #    empo_3.append(req.json()['HGET'])
print(some_id, time.time() - start)
print(collections.Counter(empo_3))
print(len(empo_3))


