import unittest
import requests
from future.moves.itertools import zip_longest

import biom
import pandas as pd
import pandas.util.testing as pdt

import redbiom.admin
import redbiom.fetch
from redbiom.fetch import (_biom_from_samples, sample_metadata,
                           samples_in_context, features_in_context,
                           sample_counts_per_category)
from redbiom.tests import assert_test_env

assert_test_env()


table = biom.load_table('test.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str)


class FetchTests(unittest.TestCase):
    def setUp(self):
        host = redbiom.get_config()['hostname']
        req = requests.get(host + '/FLUSHALL')
        assert req.status_code == 200
        redbiom.admin.ScriptManager.load_scripts(read_only=False)

    def test_samples_in_context(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, 'test', tag=None)

        table2 = table.subsample(5, by_id=True)
        redbiom.admin.create_context('test-2', 'a nice test')
        md = metadata[metadata['#SampleID'].isin(set(table2.ids()))]
        redbiom.admin.load_sample_metadata(md)
        redbiom.admin.load_sample_data(table2, 'test-2', tag='tagged')

        table3 = table.subsample(5, by_id=True)
        redbiom.admin.create_context('test-3', 'a nice test')
        md = metadata[metadata['#SampleID'].isin(set(table3.ids()))]
        redbiom.admin.load_sample_metadata(md)
        redbiom.admin.load_sample_data(table3, 'test-3', tag='tagged')

        obs = samples_in_context('test', unambiguous=False)
        self.assertEqual(obs, set(table.ids()))

        obs = samples_in_context('test-2', unambiguous=False)
        self.assertEqual(obs, set(table2.ids()))

        obs = samples_in_context('test-3', unambiguous=True)
        exp = {'tagged_%s' % i for i in table3.ids()}
        self.assertEqual(obs, exp)

    def test_features_in_context(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, 'test', tag=None)

        table2 = table.subsample(5, by_id=True)
        redbiom.admin.create_context('test-2', 'a nice test')
        md = metadata[metadata['#SampleID'].isin(set(table2.ids()))]
        redbiom.admin.load_sample_metadata(md)
        redbiom.admin.load_sample_data(table2, 'test-2', tag='tagged')

        obs = features_in_context('test')
        self.assertEqual(obs, set(table.ids(axis='observation')))

        obs = features_in_context('test-2')
        self.assertEqual(obs, set(table2.ids(axis='observation')))

    def test_biom_from_samples(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, 'test', tag=None)
        q = ('TACGGAGGATCCGAGCGTTATCCGGATTTATTGGGTTTAAAGGGAGCGTAGGCGGGTTGTTAA'
             'GTCAGTTGTGAAAGTTTGCGGCTCAACCGTAAATTTG')
        qidx = table.index(q, axis='observation')
        exp = table.filter(lambda v, i, md: v[qidx] > 0, inplace=False)
        exp.filter(lambda v, i, md: sum(v > 0) > 0, axis='observation')

        # the taxonomy is *not* normalized in the input test table. annoying.
        lineages = {}
        ranks = list('kpcofgs')
        for id_, md in zip(exp.ids(axis='observation'),
                           exp.metadata(axis='observation')):
            lineage = md['taxonomy']
            lineages[id_] = {'taxonomy': [l if l is not None else "%s__" % r
                                          for l, r in zip_longest(lineage,
                                                                  ranks)]}
        exp.add_metadata(lineages, axis='observation')

        fetch = exp.ids()[:]

        exp_map = {k: ["UNTAGGED_%s" % k] for k in exp.ids()}
        exp.update_ids({k: "%s.UNTAGGED" % k for k in exp.ids()})

        obs, obs_map = _biom_from_samples('test', fetch,
                                          normalize_taxonomy=list('kpcofgs'))
        obs = obs.sort_order(exp.ids(axis='observation'), axis='observation')

        self.assertEqual(obs, exp)
        self.assertEqual(obs_map, exp_map)

    def test_taxon_ancestors(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, 'test', tag=None)
        q = ('TACGGAGGATCCGAGCGTTATCCGGATTTATTGGGTTTAAAGGGAGCGTAGGCGGGTTGTTAA'
             'GTCAGTTGTGAAAGTTTGCGGCTCAACCGTAAATTTG')
        exp = [(['k__Bacteria',
                 'p__Bacteroidetes',
                 'c__Bacteroidia',
                 'o__Bacteroidales',
                 'f__Bacteroidaceae',
                 'g__Bacteroides',
                 's__']), (['%s__' % r for r in 'kpcofgs'])]
        obs = redbiom.fetch.taxon_ancestors('test', [q, 'foo'],
                                            normalize=list('kpcofgs'))
        self.assertEqual(obs, exp)

        # ancestors does not include self, like skbio.TreeNode.ancestors
        q = 'o__Bacteroidales'
        exp = [(['k__Bacteria',
                 'p__Bacteroidetes',
                 'c__Bacteroidia',
                 'o__',
                 'f__',
                 'g__',
                 's__'])]
        obs = redbiom.fetch.taxon_ancestors('test', [q],
                                            normalize=list('kpcofgs'))
        self.assertEqual(obs, exp)

    def test_taxon_descendents(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, 'test', tag=None)

        exp = {'TACGTAGGTGGCGAGCGTTATCCGGAATGATTGGGCGTAAAGGGTGCGTAGGTGGCAGAACAAGTCTGGAGTAAAAGGTATGGGCTCAACCCGTACTGGC',  # noqa
               'TACGTAGGTGGCGAGCGTTATCCGGAATGATTGGGCGTAAAGGGTGCGTAGGTGGCAGATCAAGTCTGGAGTAAAAGGTATGGGCTCAACCCGTACTGGC',  # noqa
               'TACGTAGGTGGCGAGCGTTATCCGGAATGATTGGGCGTAAAGGGTGCGTAGGTGGCAGATCAAGTCTGGAGTAAAAGGTATGGGCTCAACCCGTACTTGC'}  # noqa
        obs = redbiom.fetch.taxon_descendents('test', 's__biforme')
        self.assertEqual(obs, exp)

        exp = {'TACGAAGGGTGCAAGCGTTACTCGGAATTACTGGGCGTAAAGCGTGCGTAGGTGGTCGTTTAAGTCCGTTGTGAAAGCCCTGGGCTCAACCTGGGAACTG',  # noqa
               'TACGAAGGGTGCAAGCGTTACTCGGAATTACTGGGCGTAAAGCGTGCGTAGGTGGTTATTTAAGTCCGTTGTGAAAGCCCTGGGCTCAACCTGGGAACTG',  # noqa
               'TACGAAGGGTGCAAGCGTTACTCGGAATTACTGGGCGTAAAGCGTGCGTAGGTGGTTTGTTAAGTCTGATGTGAAAGCCCTGGGCTCAACCTGGGAATTG'}  # noqa
        obs = redbiom.fetch.taxon_descendents('test', 'o__Xanthomonadales')
        self.assertEqual(obs, exp)

    def test_taxonomy_no_taxonomy_entries(self):
        exp = None
        obs = redbiom.fetch.taxon_ancestors('test', ['foo', 'bar'],
                                            normalize=list('kpcofgs'))
        self.assertEqual(obs, exp)

    def test_sample_metadata_with_tagged(self):
        tagged_md = [(ix, 'abc', i % 2)
                     for i, ix in enumerate(metadata['#SampleID'])]
        tagged_md = pd.DataFrame(tagged_md,
                                 columns=['#SampleID', 'foo', 'bar'],
                                 dtype=str)

        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_metadata(tagged_md, 'testtag')
        redbiom.admin.load_sample_data(table, 'test', tag='testtag')

        exp = metadata.copy()
        exp['#SampleID'] = [i + '.testtag' for i in exp['#SampleID']]
        exp['foo'] = tagged_md['foo']
        exp['bar'] = tagged_md['bar']

        exp.set_index('#SampleID', inplace=True)
        obs, ambig = sample_metadata(table.ids(), common=False, context='test',
                                     tagged=True)
        obs.set_index('#SampleID', inplace=True)
        self.assertEqual(sorted(exp.index), sorted(obs.index))
        self.assertTrue(set(obs.columns).issubset(exp.columns))
        self.assertIn('foo', obs.columns)
        self.assertIn('bar', obs.columns)

    def test_sample_metadata_samples_not_represented_in_context(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        with self.assertRaisesRegexp(ValueError,
                                     "None of the samples"):
            # sample data have not been loaded into the context
            sample_metadata(['10317.000047188', '10317.000046868'],
                            context='test')

    def test_sample_metadata_all_cols(self):
        redbiom.admin.load_sample_metadata(metadata)
        exp = metadata.copy()
        exp.set_index('#SampleID', inplace=True)
        obs, ambig = sample_metadata(table.ids(), common=False)
        obs.set_index('#SampleID', inplace=True)
        self.assertEqual(sorted(exp.index), sorted(obs.index))
        self.assertTrue(set(obs.columns).issubset(exp.columns))

        for col in set(exp.columns) - set(obs.columns):
            self.assertTrue(set(exp[col].values), {'Unspecified', })
        obs = obs.loc[exp.index]

        # we cannot do a full table == table test. Round tripping is not
        # assured as we do not store null values in redbiom, and there is a
        # litany of possible null values.
        pdt.assert_series_equal(obs['BMI'], exp['BMI'])
        obs['AGE_YEARS'] = [v if v is not None else 'Unknown'
                            for v in obs['AGE_YEARS']]
        pdt.assert_series_equal(obs['AGE_YEARS'], exp['AGE_YEARS'])
        pdt.assert_series_equal(obs['SAMPLE_TYPE'], exp['SAMPLE_TYPE'])

    def test_sample_metadata_have_data(self):
        redbiom.admin.load_sample_metadata(metadata)
        exp = metadata.copy()
        exp.set_index('#SampleID', inplace=True)
        obs, ambig = sample_metadata(table.ids(), common=True)
        self.assertEqual(sorted(exp.index), sorted(obs.index))
        self.assertNotEqual(sorted(exp.columns), sorted(obs.columns))
        obs = obs.loc[exp.index]

        # we cannot do a full table == table test. Round tripping is not
        # assured as we do not store null values in redbiom, and there is a
        # litany of possible null values.
        pdt.assert_series_equal(obs['BMI'], exp['BMI'])
        pdt.assert_series_equal(obs['SAMPLE_TYPE'], exp['SAMPLE_TYPE'])

        # one sample has "Unknown" as its AGE_YEARS value. This means that
        # it is not informative for that column, so that value is not stored
        # for that sample. As a result, the AGE_YEARS columns is not considered
        # to be represented across all samples
        self.assertNotIn('AGE_YEARS', obs.columns)

    def test_sample_metadata_context(self):
        redbiom.admin.create_context('test', 'a nice test')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_data(table, 'test', tag='foo')

        exp = metadata.copy()
        exp.set_index('#SampleID', inplace=True)
        exp.index = ["%s.foo" % i for i in exp.index]

        obs, ambig = sample_metadata(table.ids(), common=False, context='test')
        obs.set_index('#SampleID', inplace=True)

        self.assertEqual(sorted(exp.index), sorted(obs.index))
        self.assertTrue(set(obs.columns).issubset(exp.columns))

        for col in set(exp.columns) - set(obs.columns):
            self.assertTrue(set(exp[col].values), {'Unspecified', })
        obs = obs.loc[exp.index]

        # we cannot do a full table == table test. Round tripping is not
        # assured as we do not store null values in redbiom, and there is a
        # litany of possible null values.
        pdt.assert_series_equal(obs['BMI'], exp['BMI'])
        obs['AGE_YEARS'] = [v if v is not None else 'Unknown'
                            for v in obs['AGE_YEARS']]
        pdt.assert_series_equal(obs['AGE_YEARS'], exp['AGE_YEARS'])
        pdt.assert_series_equal(obs['SAMPLE_TYPE'], exp['SAMPLE_TYPE'])

    def test_sample_metadata_restrict(self):
        redbiom.admin.load_sample_metadata(metadata)
        exp = metadata.copy()
        exp.set_index('#SampleID', inplace=True)
        exp = exp[['BMI', 'AGE_YEARS']]
        exp = exp.sort_values('BMI')
        obs, ambig = sample_metadata(table.ids(),
                                     restrict_to=['BMI', 'AGE_YEARS'])
        obs.set_index('#SampleID', inplace=True)
        obs = obs.sort_values('BMI')
        obs = obs[['BMI', 'AGE_YEARS']]
        obs['AGE_YEARS'] = [v if v is not None else 'Unknown'
                            for v in obs['AGE_YEARS']]
        pdt.assert_frame_equal(obs, exp)

    def test_sample_metadata_restrict_bad_cols(self):
        redbiom.admin.load_sample_metadata(metadata)
        with self.assertRaises(KeyError):
            sample_metadata(table.ids(), restrict_to=['BMI', 'foo'])

    def test_sample_counts_per_category(self):
        redbiom.admin.load_sample_metadata(metadata)
        obs = sample_counts_per_category()
        self.assertEqual(len(obs), 525)
        self.assertEqual(obs['LATITUDE'], 10)

    def test_sample_counts_per_category_specific(self):
        redbiom.admin.load_sample_metadata(metadata)
        obs = sample_counts_per_category(['LATITUDE'])
        self.assertEqual(len(obs), 1)
        self.assertEqual(obs['LATITUDE'], 10)

        obs = sample_counts_per_category(['LATITUDE', 'LONGITUDE'])
        self.assertEqual(len(obs), 2)
        self.assertEqual(obs['LATITUDE'], 10)
        self.assertEqual(obs['LONGITUDE'], 10)


if __name__ == '__main__':
    unittest.main()
