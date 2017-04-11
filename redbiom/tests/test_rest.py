"""Some sanity checking against the REST interface directly"""
import redbiom
import redbiom.admin
import unittest
import biom
import requests
import json
import numpy as np
import numpy.testing as npt
import pandas as pd


table = biom.load_table('test.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str)
md = pd.read_csv('test.txt', sep='\t', dtype=str).set_index('#SampleID')


def get(cmd, remainder):
    req = requests.get('http://127.0.0.1:7379/%s/%s' % (cmd, remainder))
    if req.status_code != 200:
        raise requests.HTTPError()
    return req.json()[cmd]


# database integrity tests
class RESTTests(unittest.TestCase):
    def setUp(self):
        host = redbiom.get_config()['hostname']
        req = requests.get(host + '/FLUSHALL')
        assert req.status_code == 200

    def test_observation_sample_associations(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_observations(table, context, tag=None)

        sample_ids = np.array(['UNTAGGED_%s' % i for i in table.ids()])
        for values, id_, _ in table.iter(axis='observation'):
            exp = set(sample_ids[values > 0])
            obs = set(get('SMEMBERS', 'test:samples:%s' % id_))
            self.assertEqual(obs, exp)

    def test_sample_data(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, context, tag=None)

        observation_ids = table.ids(axis='observation')

        obs_index = json.loads(get('GET', 'test:__observation_index'))
        inv_index = {v: k for k, v in obs_index.items()}

        for values, id_, _ in table.iter():
            exp_data = values[values > 0]
            exp_ids = observation_ids[values > 0]

            obs = get('GET', 'test:data:UNTAGGED_%s' % id_).split('\t')

            obs_data = np.array([float(i) for i in obs[1::2]])
            obs_ids = np.array([inv_index[int(i)] for i in obs[::2]])

            npt.assert_equal(obs_data, exp_data)
            npt.assert_equal(obs_ids, exp_ids)

    def test_metadata_categories(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)

        null_values = {'Not applicable', 'Unknown', 'Unspecified',
                       'Missing: Not collected',
                       'Missing: Not provided',
                       'Missing: Restricted access',
                       'null', 'NULL', 'no_data', 'None', 'nan'}

        for idx, row in md.iterrows():
            exp = [c for c, v in zip(md.columns, row.values)
                   if v not in null_values and '/' not in str(v)]
            obs = json.loads(get('GET', 'metadata:categories:%s' % idx))

            self.assertEqual(obs, exp)

    def test_category_values(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)

        null_values = {'Not applicable', 'Unknown', 'Unspecified',
                       'Missing: Not collected',
                       'Missing: Not provided',
                       'Missing: Restricted access',
                       'null', 'NULL', 'no_data', 'None', 'nan'}

        for idx, row in md.iterrows():
            cats = [c for c, v in zip(md.columns, row.values)
                    if v not in null_values and '/' not in str(v)]
            for c in cats:
                exp = row[c]
                obs = get('HGET', 'metadata:category:%s/%s' % (c, idx))
                self.assertEqual(obs, str(exp))


if __name__ == '__main__':
    unittest.main()
