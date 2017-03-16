import unittest
import requests

import biom
import pandas as pd

import redbiom.admin
from redbiom.fetch import _biom_from_samples


table = biom.load_table('test.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str)


class FetchTests(unittest.TestCase):
    def setUp(self):
        host = redbiom.get_config()['hostname']
        req = requests.get(host + '/FLUSHALL')
        assert req.status_code == 200

    def test_biom_from_samples(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, 'test', tag=None)
        q = ('TACGGAGGATCCGAGCGTTATCCGGATTTATTGGGTTTAAAGGGAGCGTAGGCGGGTTGTTAA'
             'GTCAGTTGTGAAAGTTTGCGGCTCAACCGTAAATTTG')
        qidx = table.index(q, axis='observation')
        exp = table.filter(lambda v, i, md: v[qidx] > 0, inplace=False)
        exp.filter(lambda v, i, md: sum(v > 0) > 0, axis='observation')

        fetch = exp.ids()[:]

        exp_map = {k: ["UNTAGGED_%s" % k] for k in exp.ids()}
        exp.update_ids({k: "%s.UNTAGGED" % k for k in exp.ids()})

        obs, obs_map = _biom_from_samples('test', fetch)
        obs = obs.sort_order(exp.ids(axis='observation'), axis='observation')
        self.assertEqual(obs, exp)
        self.assertEqual(obs_map, exp_map)


if __name__ == '__main__':
    unittest.main()
