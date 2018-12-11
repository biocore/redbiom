import unittest
import hashlib

import skbio
import pandas as pd
import biom
import requests

import redbiom
import redbiom.admin
import redbiom._requests
import redbiom.fetch
from redbiom.tests import assert_test_env

assert_test_env()

table = biom.load_table('test.biom')
table_with_alt = biom.load_table('test_with_alts.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str, na_values=[],
                       keep_default_na=False)
metadata_with_alt = pd.read_csv('test_with_alts.txt', sep='\t', dtype=str)


class ScriptManagerTests(unittest.TestCase):
    def setUp(self):
        self.host = redbiom.get_config()['hostname']
        requests.get(self.host + '/script/flush')
        redbiom.admin.ScriptManager.load_scripts(read_only=False)

    def test_load_scripts(self):
        for script in redbiom.admin.ScriptManager._scripts.values():
            sha = hashlib.sha1(script.encode('ascii')).hexdigest()
            req = requests.get(self.host + '/script/exists/%s' % sha)
            self.assertTrue(req.json()['script'][0])

    def test_load_scripts_readonly(self):
        redbiom.admin.ScriptManager.drop_scripts()
        redbiom.admin.ScriptManager.load_scripts(read_only=True)
        for name, script in redbiom.admin.ScriptManager._scripts.items():
            sha = hashlib.sha1(script.encode('ascii')).hexdigest()
            req = requests.get(self.host + '/script/exists/%s' % sha)
            if name in redbiom.admin.ScriptManager._admin_scripts:
                self.assertFalse(req.json()['script'][0])
            else:
                self.assertTrue(req.json()['script'][0])

    def test_get_script(self):
        exp = requests.get(self.host + '/hget/state:scripts/get-index')
        exp = exp.json()['hget']
        obs = redbiom.admin.ScriptManager.get('get-index')
        self.assertEqual(obs, exp)

    def test_get_script_missing(self):
        with self.assertRaisesRegexp(ValueError, "Unknown script"):
            redbiom.admin.ScriptManager.get('foobar')

    def test_drop_scripts(self):
        redbiom.admin.ScriptManager.get('get-index')
        redbiom.admin.ScriptManager.drop_scripts()
        with self.assertRaisesRegexp(ValueError, "Unknown script"):
            redbiom.admin.ScriptManager.get('get-index')


class AdminTests(unittest.TestCase):
    def setUp(self):
        self.host = redbiom.get_config()['hostname']
        req = requests.get(self.host + '/flushall')
        assert req.status_code == 200
        self.get = redbiom._requests.make_get(redbiom.get_config())
        self.se = redbiom._requests.make_script_exec(redbiom.get_config())
        redbiom.admin.ScriptManager.load_scripts(read_only=False)

    def test_metadata_to_taxonomy_tree(self):
        exp = None
        # no taxonomy
        obs = redbiom.admin._metadata_to_taxonomy_tree([1, 2, 3], None)
        self.assertEqual(obs, exp)

        input = [('1', '2', '3'),
                 [{'taxonomy': ['k__foo', 'p__bar', 'c__baz']},
                  {'taxonomy': ['k__foo', 'p__bar', 'c__']},
                  {'taxonomy': ['k__foo', 'p__bar', 'c__thing']}]]
        exp = u'((((1)c__baz,2,(3)c__thing)p__bar)k__foo);'
        exp = skbio.TreeNode.read([exp])
        obs = redbiom.admin._metadata_to_taxonomy_tree(*input)
        self.assertEqual(obs.compare_subsets(exp), 0.0)

    def test_get_index(self):
        context = 'load-features-test'
        redbiom.admin.create_context(context, 'foo')

        tests = [('A', 0), ('A', 0), ('B', 1), ('C', 2),
                 ('B', 1), ('Z', 3), ('A', 0)]
        for key, exp in tests:
            obs = redbiom.admin.get_index(context, key, 'feature')
            self.assertEqual(obs, exp)

    def test_create_context(self):
        obs = self.get('state', 'HGETALL', 'contexts')
        self.assertNotIn('another test', list(obs.keys()))
        redbiom.admin.create_context('another test', 'a nice test')
        obs = self.get('state', 'HGETALL', 'contexts')
        self.assertIn('another test', list(obs.keys()))

    def test_load_features(self):
        context = 'load-features-test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        n = redbiom.admin.load_sample_data(table, context, tag=None)
        for id_ in table.ids(axis='observation'):
            self.assertTrue(self.get(context, 'EXISTS', 'feature:%s' % id_))
        self.assertEqual(n, 10)

        tag = 'tagged'
        n = redbiom.admin.load_sample_data(table, context, tag=tag)

        tagged_samples = set(['%s_%s' % (tag, i) for i in table.ids()])
        fetch_feature = redbiom.admin.ScriptManager.get('fetch-feature')
        for values, id_, _ in table.iter(axis='observation'):
            obs = self.se(fetch_feature, 0, context, id_)
            obs_tagged = {o for o in obs if o.startswith(tag)}
            self.assertEqual(len(obs_tagged), sum(values > 0))
            self.assertTrue(obs_tagged.issubset(tagged_samples))
        self.assertEqual(n, 10)

        exp = {'UNTAGGED_%s' % i for i in table.ids()}
        exp.update({'tagged_%s' % i for i in table.ids()})
        obs = self.get(context, 'SMEMBERS', 'samples-represented')
        self.assertEqual(set(obs), exp)

    def test_load_sample_data_empty(self):
        context = 'load-data-empty'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        with self.assertRaises(ValueError):
            redbiom.admin.load_sample_data(biom.Table([], [], []), context,
                                           tag=None)

    def test_load_features_partial(self):
        context = 'load-features-partial'
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

        exp = {'UNTAGGED_%s' % i for i in table.ids()}
        exp.update({'UNTAGGED_%s' % i for i in table_with_alt.ids()})
        obs = self.get(context, 'SMEMBERS', 'samples-represented')
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
            self.assertTrue(self.get(context, 'EXISTS', 'sample:%s' % id_))

    def test_load_sample_data_taxonomy(self):
        context = 'load-sample-data'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, context, tag=None)

        k__Bacteria = {'p__Firmicutes',
                       'p__Actinobacteria',
                       'p__Proteobacteria',
                       'p__Cyanobacteria',
                       'p__Bacteroidetes',
                       'p__Fusobacteria',
                       'p__Verrucomicrobia',
                       'p__Tenericutes',
                       'p__Lentisphaerae',
                       'p__[Thermi]'}

        # has an unclassified genus, so it should have tips directly descending
        f__Actinomycetaceae = {'g__Varibaculum',
                               'g__Actinomyces',
                               'has-terminal'}
        f__Actinomycetaceae_terminal = 'TACGTAGGGCGCGAGCGTTGTCCGGAATTATTGGGCGTAAAGGGCTCGTAGGCGGCTTGTCGCGTCTGCTGTGAAAATGCGGGGCTTAACTCCGTACGTG'  # noqa

        obs_bacteria = self.get(context, 'SMEMBERS',
                                ':'.join(['taxonomy-children',
                                          'k__Bacteria']))
        self.assertEqual(set(obs_bacteria), k__Bacteria)
        obs_Actinomycetaceae = self.get(context, 'SMEMBERS',
                                        ':'.join(['taxonomy-children',
                                                  'f__Actinomycetaceae']))
        self.assertEqual(set(obs_Actinomycetaceae), f__Actinomycetaceae)

        key = 'terminal-of:f__Actinomycetaceae'
        obs_Actinomycetaceae_terminal = self.get(context, 'SMEMBERS', key)
        self.assertEqual(len(obs_Actinomycetaceae_terminal), 1)
        id_ = obs_Actinomycetaceae_terminal[0]
        key = 'feature-index-inverted/%d' % int(id_)
        obs_Actinomycetaceae_terminal = self.get(context, 'HGET', key)
        self.assertEqual(obs_Actinomycetaceae_terminal,
                         f__Actinomycetaceae_terminal)

        exp_parents = [('p__Firmicutes', 'k__Bacteria'),
                       ('p__Fusobacteria', 'k__Bacteria'),
                       ('g__Actinomyces', 'f__Actinomycetaceae'),
                       (id_, 'f__Actinomycetaceae')]
        for name, exp in exp_parents:
            obs = self.get(context, 'HGET', 'taxonomy-parents/%s' % name)
            self.assertEqual(obs, exp)

    def test_load_sample_metadata(self):
        redbiom.admin.load_sample_metadata(metadata)
        exp = set(metadata.columns) - set(['#SampleID'])
        obs = set(self.get('metadata', 'SMEMBERS', 'categories-represented'))
        self.assertEqual(obs, exp)
        exp = set(metadata['#SampleID'])
        obs = set(self.get('metadata', 'SMEMBERS', 'samples-represented'))
        self.assertEqual(obs, exp)

    def test_load_sample_metadata_full_search(self):
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_metadata_full_search(metadata)
        tests = [('agp-skin', {'10317.000003302', }),

                 # an example of a misleading query. only those AG samples
                 # which report as not having taken abx in the past year will
                 # be returned, as recent use is "1 week" etc.
                 ('antibiot', {'10317.000047188',
                               '10317.000051129',
                               '10317.000033804',
                               '10317.000001378',
                               '10317.000005080',
                               '10317.000003302'}),

                 ('australia', {'10317.000022252', }),

                 # two people live in NY
                 ('ny', {'10317.000033804',
                         '10317.000001405'})]

        for test, exp in tests:
            obs = set(self.get('metadata:text-search', 'SMEMBERS', test))
            self.assertEqual(obs, exp)

        # test we can search over categories too
        tests = [('antibiot', {'SUBSET_ANTIBIOTIC_HISTORY',
                               'ANTIBIOTIC_HISTORY'}),
                 ('hand', {'DOMINANT_HAND', }),
                 ('diseas', {'LIVER_DISEASE',
                              'CARDIOVASCULAR_DISEASE',
                              'LUNG_DISEASE',
                              'KIDNEY_DISEASE'}),

                 # note: this does not get "SEAFOOD" categories as that is its
                 # own stem. However, it will grab "SEA_FOOD" because that
                 # that stem in the Vioscreen category is split...
                 ('food', {'NON_FOOD_ALLERGIES_BEESTINGS',
                           'VIOSCREEN_HEI2010_PROTIEN_FOODS',
                           'ALLERGIC_TO_I_HAVE_NO_FOOD_ALLERGIES_THAT_I_KNOW_OF',  # noqa
                           'VIOSCREEN_FRIED_FOOD_SERVINGS',
                           'NON_FOOD_ALLERGIES_SUN',
                           'NON_FOOD_ALLERGIES',
                           'NON_FOOD_ALLERGIES_UNSPECIFIED',
                           'SPECIALIZED_DIET_RAW_FOOD_DIET',
                           'NON_FOOD_ALLERGIES_POISON_IVYOAK',
                           'NON_FOOD_ALLERGIES_PET_DANDER',
                           'NON_FOOD_ALLERGIES_DRUG_EG_PENICILLIN',
                           'VIOSCREEN_HEI2010_SEA_FOODS_PLANT_PROTIENS'})]

        for test, exp in tests:
            obs = set(self.get('metadata:category-search', 'SMEMBERS', test))
            self.assertEqual(obs, exp)


if __name__ == '__main__':
    unittest.main()
