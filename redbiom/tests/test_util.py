import unittest
from functools import reduce
import random
import numpy as np
import pandas as pd
import requests

import biom

import redbiom
import redbiom.admin
from redbiom.util import (float_or_nan, from_or_nargs,
                          samples_from_observations, has_sample_metadata,
                          partition_samples_by_tags, resolve_ambiguities,
                          _stable_ids_from_ambig, _stable_ids_from_unambig)


table = biom.load_table('test.biom')
table_with_alt = biom.load_table('test_with_alts.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str)
metadata_with_alt = pd.read_csv('test_with_alts.txt', sep='\t', dtype=str)


class UtilTests(unittest.TestCase):
    def setUp(self):
        host = redbiom.get_config()['hostname']
        req = requests.get(host + '/FLUSHALL')
        assert req.status_code == 200

    def test_float_or_nan(self):
        import math

        self.assertEqual(float_or_nan('123'), 123)
        self.assertEqual(float_or_nan('.123'), 0.123)
        self.assertIs(float_or_nan('x.123'), math.nan)
        self.assertEqual(float_or_nan('0.123'), 0.123)
        self.assertIs(float_or_nan(''), math.nan)

    def test_from_or_nargs(self):
        with self.assertRaises(SystemExit):
            from_or_nargs(['foo'], ['bar'])

        self.assertEqual(["foo", "bar"], list(from_or_nargs(None,
                                                            ["foo\n",
                                                             "\nbar"])))
        self.assertEqual(['1', '2', '3'],
                         list(from_or_nargs(['1', '2', '3'], None)))
        self.assertEqual(['1', '2', '3'],
                         list(from_or_nargs(None, ['1', '2', '3'])))

        # deferring validation of inference of stdin to integration tests
        # as it would require overriding that standard file descriptor.

    def test_samples_from_observations(self):
        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        n = redbiom.admin.load_observations(table, 'test', tag=None)

        sample_ids = table.ids()[:]
        sample_ids = np.array(["UNTAGGED_%s" % i for i in sample_ids])
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
        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)

        sample_ids = table.ids().copy()
        random.shuffle(sample_ids)

        for i in range(1, 5):
            self.assertTrue(has_sample_metadata(sample_ids[:i]))

    def test_partition_samples_by_tags(self):
        exp_untagged = ['1']
        exp_tagged = ['xyz_2', 'xyz_3']
        exp_tags = ['xyz', 'xyz']
        exp_tagged_clean = ['2', '3']

        testset = ['xyz_2', '1', 'xyz_3']
        obs_untagged, obs_tagged, obs_tags, obs_tagged_clean = \
                partition_samples_by_tags(testset)

        self.assertEqual(obs_untagged, exp_untagged)
        self.assertEqual(obs_tagged, exp_tagged)
        self.assertEqual(obs_tags, exp_tags)
        self.assertEqual(obs_tagged_clean, exp_tagged_clean)

    def test_resolve_ambiguities(self):
        import redbiom.requests
        import redbiom
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        n = redbiom.admin.load_sample_data(table, 'test', tag=None)

        # all samples as ambiguous
        samples = {'10317.000047188', '10317.000046868', '10317.000051129',
                   '10317.000012975', '10317.000033804', '10317.000001405',
                   '10317.000022252', '10317.000001378', '10317.000005080',
                   '10317.000003302'}
        exp_stable = {"%s.UNTAGGED" % k: k for k in samples}
        exp_unobserved = []
        exp_ambiguous = {k: ["UNTAGGED_%s" % k] for k in samples}
        exp_ri = {'UNTAGGED_%s' % k: '%s.UNTAGGED' % k for k in samples}
        obs_stable, obs_unobserved, obs_ambiguous, obs_ri = \
            resolve_ambiguities('test', samples, get)

        self.assertEqual(obs_stable, exp_stable)
        self.assertEqual(obs_unobserved, exp_unobserved)
        self.assertEqual(obs_ambiguous, exp_ambiguous)
        self.assertEqual(obs_ri, exp_ri)

    def test_resolve_ambiguities_mixed(self):
        import redbiom.requests
        import redbiom
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        n = redbiom.admin.load_sample_data(table, 'test', tag=None)

        samples = {'10317.000047188', '10317.000046868', '10317.000051129',
                   '10317.000012975', '10317.000033804', '10317.000001405',
                   '10317.000022252', '10317.000001378',
                   'foo', 'UNTAGGED_bar',
                   'UNTAGGED_10317.000003302'}
        exp_stable = {"%s.UNTAGGED" % k: k for k in samples
                      if 'foo' not in k and 'bar' not in k}
        exp_stable.pop('UNTAGGED_10317.000003302.UNTAGGED')
        exp_stable['10317.000003302.UNTAGGED'] = 'UNTAGGED_10317.000003302'
        exp_unobserved = ['foo', 'UNTAGGED_bar']
        exp_ambiguous = {'10317.000047188': ['UNTAGGED_10317.000047188'],
                         '10317.000046868': ['UNTAGGED_10317.000046868'],
                         '10317.000051129': ['UNTAGGED_10317.000051129'],
                         '10317.000012975': ['UNTAGGED_10317.000012975'],
                         '10317.000033804': ['UNTAGGED_10317.000033804'],
                         '10317.000001405': ['UNTAGGED_10317.000001405'],
                         '10317.000022252': ['UNTAGGED_10317.000022252'],
                         '10317.000001378': ['UNTAGGED_10317.000001378'],
                         '10317.000003302': ['UNTAGGED_10317.000003302']}
        exp_ri = {'UNTAGGED_10317.000047188': '10317.000047188.UNTAGGED',
                  'UNTAGGED_10317.000046868': '10317.000046868.UNTAGGED',
                  'UNTAGGED_10317.000051129': '10317.000051129.UNTAGGED',
                  'UNTAGGED_10317.000012975': '10317.000012975.UNTAGGED',
                  'UNTAGGED_10317.000033804': '10317.000033804.UNTAGGED',
                  'UNTAGGED_10317.000001405': '10317.000001405.UNTAGGED',
                  'UNTAGGED_10317.000022252': '10317.000022252.UNTAGGED',
                  'UNTAGGED_10317.000001378': '10317.000001378.UNTAGGED',
                  'UNTAGGED_10317.000003302': '10317.000003302.UNTAGGED'}

        obs_stable, obs_unobserved, obs_ambiguous, obs_ri = \
            resolve_ambiguities('test', samples, get)

        self.assertEqual(obs_stable, exp_stable)
        self.assertEqual(obs_unobserved, exp_unobserved)
        self.assertEqual(obs_ambiguous, exp_ambiguous)
        self.assertEqual(obs_ri, exp_ri)

    def test_resolve_ambiguities_has_ambig(self):
        import redbiom.requests
        import redbiom
        config = redbiom.get_config()
        get = redbiom.requests.make_get(config)

        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        n = redbiom.admin.load_sample_data(table, 'test', tag='fromtest')
        redbiom.admin.load_sample_metadata(metadata_with_alt)
        n = redbiom.admin.load_sample_data(table_with_alt, 'test',
                                           tag='fromalt')

        # 000047188 is both fromtest and fromalt, but fully specified here
        # 000005080 is in both and ambiguouus
        samples = {'fromtest_10317.000047188', '10317.000005080', 'foo'}
        exp_stable = {'10317.000047188.fromtest': 'fromtest_10317.000047188',
                      '10317.000005080.fromtest': '10317.000005080',
                      '10317.000005080.fromalt': '10317.000005080'}
        exp_unobserved = ['foo']
        exp_ambiguous = {'10317.000047188': ['fromtest_10317.000047188'],
                         '10317.000005080': ['fromtest_10317.000005080',
                                             'fromalt_10317.000005080']}
        exp_ri = {'fromtest_10317.000047188': '10317.000047188.fromtest',
                  'fromtest_10317.000005080': '10317.000005080.fromtest',
                  'fromalt_10317.000005080': '10317.000005080.fromalt'}

        obs_stable, obs_unobserved, obs_ambiguous, obs_ri = \
            resolve_ambiguities('test', samples, get)

        self.assertEqual(obs_stable, exp_stable)
        self.assertEqual(obs_unobserved, exp_unobserved)
        self.assertEqual(obs_ambiguous, exp_ambiguous)
        self.assertEqual(obs_ri, exp_ri)

    def test_stable_ids_from_ambig(self):
        exp_stable = {'foo.bar': 'foo',
                      'foo.baz': 'foo'}
        exp_ri = {'bar_foo': 'foo.bar',
                  'baz_foo': 'foo.baz'}
        data = {'foo': ['bar_foo', 'baz_foo']}
        obs_stable, obs_ri = _stable_ids_from_ambig(data)
        self.assertEqual(obs_stable, exp_stable)
        self.assertEqual(obs_ri, exp_ri)

    def test_stable_ids_from_unambig(self):
        exp_stable = {'foo.bar': 'bar_foo',
                      'foo.baz': 'baz_foo'}
        exp_ri = {'bar_foo': 'foo.bar',
                  'baz_foo': 'foo.baz'}
        data = ['bar_foo', 'baz_foo']
        obs_stable, obs_ri = _stable_ids_from_unambig(data)
        self.assertEqual(obs_stable, exp_stable)
        self.assertEqual(obs_ri, exp_ri)


if __name__ == '__main__':
    unittest.main()
