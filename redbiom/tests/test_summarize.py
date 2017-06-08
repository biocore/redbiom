import unittest
import requests

import biom
import pandas as pd
import pandas.util.testing as pdt

import redbiom
import redbiom.admin
import redbiom._requests
from redbiom.summarize import contexts
from redbiom.tests import assert_test_env

assert_test_env()


table = biom.load_table('test.biom')
table_with_alt = biom.load_table('test_with_alts.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str)
metadata_with_alt = pd.read_csv('test_with_alts.txt', sep='\t', dtype=str)


class SummarizeTests(unittest.TestCase):
    def setUp(self):
        host = redbiom.get_config()['hostname']
        req = requests.get(host + '/FLUSHALL')
        assert req.status_code == 200
        self.get = redbiom._requests.make_get(redbiom.get_config())

    def test_summarize_contexts_nodetail(self):
        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.create_context('test2', 'foo')

        exp = pd.DataFrame([('test',), ('test2',)],
                           columns=['ContextName'])
        obs = contexts(detail=False)
        pdt.assert_frame_equal(obs, exp)

    def test_summarize_contexts_no_contexts(self):
        exp = pd.DataFrame([], columns=['ContextName', 'SamplesWithData',
                                        'FeaturesWithData', 'Description'])
        obs = contexts()
        pdt.assert_frame_equal(obs, exp)

    def test_summarize_contexts_no_samples(self):
        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        exp = pd.DataFrame([('test', 0, 0, 'foo')],
                           columns=['ContextName', 'SamplesWithData',
                                    'FeaturesWithData', 'Description'])
        obs = contexts()
        pdt.assert_frame_equal(obs, exp)

    def test_summarize_contexts_full_load(self):
        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.ScriptManager.load_scripts(read_only=False)
        ndat = redbiom.admin.load_sample_data(table, 'test', tag=None)
        nfeat = len(table.ids(axis='observation'))
        exp = pd.DataFrame([('test', ndat, nfeat, 'foo')],
                           columns=['ContextName', 'SamplesWithData',
                                    'FeaturesWithData', 'Description'])

        obs = contexts()
        pdt.assert_frame_equal(obs, exp)

    def test_summarize_contexts_full_load_multiple(self):
        redbiom.admin.create_context('test', 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.ScriptManager.load_scripts(read_only=False)
        ndat_a = redbiom.admin.load_sample_data(table, 'test', tag=None)
        nfeat_a = len(table.ids(axis='observation'))

        redbiom.admin.create_context('test-alt', 'bar')
        redbiom.admin.load_sample_metadata(metadata_with_alt)
        ndat_b = redbiom.admin.load_sample_data(table_with_alt, 'test-alt',
                                                tag=None)
        nfeat_b = len(table_with_alt.ids(axis='observation'))
        exp = pd.DataFrame([('test', ndat_a, nfeat_a, 'foo'),
                            ('test-alt', ndat_b, nfeat_b, 'bar')],
                           columns=['ContextName', 'SamplesWithData',
                                    'FeaturesWithData', 'Description'])

        obs = contexts()
        obs = obs.sort_values('ContextName').set_index('ContextName')
        exp = exp.sort_values('ContextName').set_index('ContextName')
        pdt.assert_frame_equal(obs, exp)


if __name__ == '__main__':
    unittest.main()
