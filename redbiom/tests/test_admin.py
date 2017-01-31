import unittest
import os

import biom
import numpy.testing as npt

import redbiom
import redbiom.admin
import redbiom.requests
import redbiom.fetch


get = redbiom.requests.make_get(redbiom.get_config())


table = 'test.biom'
metadata = 'test.txt'
if not os.path.exists(table):
    raise ValueError("Please drive suite from repo root")


class AdminTests(unittest.TestCase):
    def test_create_context(self):
        obs = get('state', 'HGETALL', 'contexts')
        self.assertNotIn('another test', list(obs.keys()))
        redbiom.admin.create_context('another test', 'a nice test')
        exp = ['another test', 'test']
        obs = get('state', 'HGETALL', 'contexts')
        self.assertIn('another test', list(obs.keys()))

    def test_load_observations(self):
        context = 'load-observations-test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_observations(table, context, tag=None)
        tab = biom.load_table(table)
        for id_ in tab.ids(axis='observation'):
            self.assertTrue(get(context, 'EXISTS', 'samples:%s' % id_))

        tag = 'tagged'
        redbiom.admin.load_sample_metadata(metadata, tag)
        redbiom.admin.load_observations(table, context, tag=tag)
        tab = biom.load_table(table)
        tagged_samples = set(['%s_%s' % (tag, i) for i in tab.ids()])
        for values, id_, _ in tab.iter(axis='observation'):
            obs = get(context, 'SMEMBERS', 'samples:%s' % id_)
            obs_tagged = {o for o in obs if o.startswith(tag)}
            self.assertEqual(len(obs_tagged), sum(values > 0))
            self.assertTrue(obs_tagged.issubset(tagged_samples))


if __name__ == '__main__':
    unittest.main()
