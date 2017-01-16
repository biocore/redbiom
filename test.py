import unittest
import biom
import requests
import json
import numpy as np
import numpy.testing as npt
import pandas as pd


table = biom.load_table('test.biom')
md = pd.read_csv('test.txt', sep='\t', dtype=str).set_index('#SampleID')


def get(cmd, remainder):
    req = requests.get('http://127.0.0.1:7379/%s/%s' % (cmd, remainder))
    if req.status_code != 200:
        raise requests.HTTPError()
    return req.json()[cmd]


# database integrity tests
class redbiomtests(unittest.TestCase):
    def test_observation_sample_associations(self):
        sample_ids = table.ids()
        for values, id_, _ in table.iter(axis='observation'):
            exp = set(sample_ids[values > 0])
            obs = set(get('SMEMBERS', 'samples:%s' % id_))
            self.assertEqual(obs, exp)

    def test_sample_data(self):
        observation_ids = table.ids(axis='observation')

        obs_index = json.loads(get('GET', '__observation_index'))
        inv_index = {v: k for k, v in obs_index.items()}

        for values, id_, _ in table.iter():
            exp_data = values[values > 0]
            exp_ids = observation_ids[values > 0]

            obs = get('GET', 'data:%s' % id_).split('\t')

            obs_data = np.array([float(i) for i in obs[1::2]])
            obs_ids = np.array([inv_index[int(i)] for i in obs[::2]])

            npt.assert_equal(obs_data, exp_data)
            npt.assert_equal(obs_ids, exp_ids)

    def test_metadata_categories(self):
        null_values = {'Not applicable', 'Unknown', 'Unspecified',
                       'Missing: Not collected',
                       'Missing: Not provided',
                       'Missing: Restricted access',
                       'null', 'NULL', 'no_data', 'None', 'nan'}

        for idx, row in md.iterrows():
            exp = [c for c, v in zip(md.columns, row.values)
                   if v not in null_values and '/' not in str(v)]
            obs = json.loads(get('GET', 'metadata-categories:%s' % idx))

            self.assertEqual(obs, exp)

    def test_category_values(self):
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
                obs = get('HGET', 'category:%s/%s' % (c, idx))
                self.assertEqual(obs, str(exp))


if __name__ == '__main__':
    unittest.main()
