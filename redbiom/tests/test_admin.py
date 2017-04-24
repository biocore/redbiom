import unittest

import pandas as pd
import biom
import requests

import redbiom
import redbiom.admin
import redbiom._requests
import redbiom.fetch


table = biom.load_table('test.biom')
table_with_alt = biom.load_table('test_with_alts.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str)
metadata_with_alt = pd.read_csv('test_with_alts.txt', sep='\t', dtype=str)


class AdminTests(unittest.TestCase):
    def setUp(self):
        host = redbiom.get_config()['hostname']
        req = requests.get(host + '/flushall')
        assert req.status_code == 200
        self.get = redbiom._requests.make_get(redbiom.get_config())

    def test_create_context(self):
        obs = self.get('state', 'HGETALL', 'contexts')
        self.assertNotIn('another test', list(obs.keys()))
        redbiom.admin.create_context('another test', 'a nice test')
        obs = self.get('state', 'HGETALL', 'contexts')
        self.assertIn('another test', list(obs.keys()))

    def test_load_observations(self):
        context = 'load-observations-test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        n = redbiom.admin.load_observations(table, context, tag=None)
        for id_ in table.ids(axis='observation'):
            self.assertTrue(self.get(context, 'EXISTS', 'samples:%s' % id_))
        self.assertEqual(n, 10)

        tag = 'tagged'
        n = redbiom.admin.load_observations(table, context, tag=tag)
        tagged_samples = set(['%s_%s' % (tag, i) for i in table.ids()])
        for values, id_, _ in table.iter(axis='observation'):
            obs = self.get(context, 'SMEMBERS', 'samples:%s' % id_)
            obs_tagged = {o for o in obs if o.startswith(tag)}
            self.assertEqual(len(obs_tagged), sum(values > 0))
            self.assertTrue(obs_tagged.issubset(tagged_samples))
        self.assertEqual(n, 10)

        exp = {'UNTAGGED_%s' % i for i in table.ids()}
        exp.update({'tagged_%s' % i for i in table.ids()})
        obs = self.get(context, 'SMEMBERS', 'samples-represented-observations')
        self.assertEqual(set(obs), exp)

    def test_load_observations_partial(self):
        context = 'load-observations-partial'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        n = redbiom.admin.load_observations(table, context, tag=None)
        self.assertEqual(n, 10)

        with self.assertRaises(ValueError):
            # the metadata for the samples to load hasn't been added yet
            redbiom.admin.load_observations(table_with_alt, context, tag=None)

        redbiom.admin.load_sample_metadata(metadata_with_alt)
        n = redbiom.admin.load_observations(table_with_alt, context, tag=None)
        self.assertEqual(n, 2)

        exp = {'UNTAGGED_%s' % i for i in table.ids()}
        exp.update({'UNTAGGED_%s' % i for i in table_with_alt.ids()})
        obs = self.get(context, 'SMEMBERS', 'samples-represented-observations')
        self.assertEqual(set(obs), exp)

    def test_load_sample_data(self):
        context = 'load-sample-data'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)

        n = redbiom.admin.load_sample_data(table, context, tag=None)
        self.assertEqual(n, 10)

        with self.assertRaises(ValueError):
            # the metadata for the samples to load hasn't been added yet
            redbiom.admin.load_sample_data(table_with_alt, context, tag=None)

        redbiom.admin.load_sample_metadata(metadata_with_alt)
        n = redbiom.admin.load_sample_data(table_with_alt, context, tag=None)
        self.assertEqual(n, 2)

        for id_ in set(table.ids()) & set(table_with_alt.ids()):
            id_ = 'UNTAGGED_%s' % id_
            self.assertTrue(self.get(context, 'EXISTS', 'data:%s' % id_))

    def test_load_sample_metadata(self):
        redbiom.admin.load_sample_metadata(metadata)
        exp = set(metadata.columns) - set(['#SampleID'])
        obs = set(self.get('metadata', 'SMEMBERS', 'categories-represented'))
        self.assertEqual(obs, exp)
        exp = set(metadata['#SampleID'])
        obs = set(self.get('metadata', 'SMEMBERS', 'samples-represented'))
        self.assertEqual(obs, exp)

    def test_load_sample_metadata_full_search(self):
        pass


if __name__ == '__main__':
    unittest.main()
