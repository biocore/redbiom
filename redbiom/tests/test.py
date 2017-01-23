import unittest
import biom
import requests
import json
import numpy as np
import numpy.testing as npt
import pandas as pd
import random
from redbiom.util import (float_or_nan, from_or_nargs, exists,
                          samples_from_observations, has_sample_metadata)


table = biom.load_table('test.biom')
md = pd.read_csv('test.txt', sep='\t', dtype=str).set_index('#SampleID')


def get(cmd, remainder):
    req = requests.get('http://127.0.0.1:7379/%s/%s' % (cmd, remainder))
    if req.status_code != 200:
        raise requests.HTTPError()
    return req.json()[cmd]


# database integrity tests
class RESTTests(unittest.TestCase):
    def test_observation_sample_associations(self):
        sample_ids = table.ids()
        for values, id_, _ in table.iter(axis='observation'):
            exp = set(sample_ids[values > 0])
            obs = set(get('SMEMBERS', 'test:samples:%s' % id_))
            self.assertEqual(obs, exp)

    def test_sample_data(self):
        observation_ids = table.ids(axis='observation')

        obs_index = json.loads(get('GET', 'test:__observation_index'))
        inv_index = {v: k for k, v in obs_index.items()}

        for values, id_, _ in table.iter():
            exp_data = values[values > 0]
            exp_ids = observation_ids[values > 0]

            obs = get('GET', 'test:data:%s' % id_).split('\t')

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
            obs = json.loads(get('GET', 'metadata:categories:%s' % idx))

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
                obs = get('HGET', 'metadata:category:%s/%s' % (c, idx))
                self.assertEqual(obs, str(exp))


class MethodTests(unittest.TestCase):
    def test_float_or_nan(self):
        import math

        self.assertEqual(float_or_nan('123'), 123)
        self.assertEqual(float_or_nan('.123'), 0.123)
        self.assertIs(float_or_nan('x.123'), math.nan)
        self.assertEqual(float_or_nan('0.123'), 0.123)
        self.assertIs(float_or_nan(''), math.nan)

    def test_from_or_nargs(self):
        with self.assertRaises(SystemExit):
            from_or_nargs(None, None)
        with self.assertRaises(SystemExit):
            from_or_nargs(['foo'], ['bar'])

        self.assertEqual([1, 2, 3], list(from_or_nargs([1, 2, 3], None)))
        self.assertEqual([1, 2, 3], list(from_or_nargs(None, [1, 2, 3])))

    def test_exists(self):
        self.assertTrue(exists(['10317.000001405'], 'test'))
        self.assertTrue(exists(['10317.000001405', '10317.000046868'], 'test'))
        self.assertTrue(exists(['foo', '10317.000046868'], 'test'))
        self.assertFalse(exists(['foo', 'bar'], 'test'))
        self.assertFalse(exists([], 'test'))

    def test_samples_from_observations(self):
        from functools import reduce
        sample_ids = table.ids()
        ids = table.ids(axis='observation').copy()
        random.shuffle(ids)

        for i in range(1, 5):
            fetch = ids[:i]
            assoc_ids = []
            for id_ in fetch:
                assoc_data = table.data(id_, axis='observation')
                assoc_ids.append(sample_ids[assoc_data > 0])

            exp_exact = reduce(set.intersection, map(set, assoc_ids))
            exp_union = reduce(set.union, map(set, assoc_ids))

            obs_exact = samples_from_observations(iter(fetch), True, 'test')
            obs_union = samples_from_observations(iter(fetch), False, 'test')

            self.assertEqual(obs_exact, exp_exact)
            self.assertEqual(obs_union, exp_union)

    def test_has_sample_metadata(self):
        sample_ids = table.ids().copy()
        random.shuffle(sample_ids)

        for i in range(1, 5):
            self.assertTrue(has_sample_metadata(sample_ids[:i]))


if __name__ == '__main__':
    unittest.main()
