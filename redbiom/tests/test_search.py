import unittest

import pandas as pd
import requests

import redbiom._requests
import redbiom.admin
import redbiom.search
from redbiom.tests import assert_test_env

assert_test_env()


metadata = pd.read_csv('test.txt', sep='\t', dtype=str, na_values=[],
                       keep_default_na=False)


class SearchTests(unittest.TestCase):
    def setUp(self):
        host = redbiom.get_config()['hostname']
        req = requests.get(host + '/flushall')
        assert req.status_code == 200
        self.get = redbiom._requests.make_get(redbiom.get_config())
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.load_sample_metadata_full_search(metadata)

    def test_metadata_values(self):
        tests = [('ab', set()),
                 ('antibiotics', {'10317.000047188',
                                  '10317.000051129',
                                  '10317.000033804',
                                  '10317.000001378',
                                  '10317.000005080',
                                  '10317.000003302'}),
                 ('antibiotics where AGE_CAT in ("20s","30s")',
                     {'10317.000033804',
                      '10317.000051129',
                      '10317.000005080',
                      '10317.000001378'}),
                 ('antibiotics where AGE_CAT not in ("30s","50s")',
                     {'10317.000033804',
                      '10317.000001378'}),
                 ('antibiotics where AGE_CAT != "40s" and AGE_YEARS > 40',
                     {'10317.000047188', }),
                 ('antibiotics & NY', {'10317.000033804', }),
                 ('antibiotics - NY', {'10317.000047188',
                                       '10317.000051129',
                                       '10317.000001378',
                                       '10317.000005080',
                                       '10317.000003302'}),
                 ('antibiotics | NY', {'10317.000047188',
                                       '10317.000051129',
                                       '10317.000033804',
                                       '10317.000001405',
                                       '10317.000001378',
                                       '10317.000005080',
                                       '10317.000003302'}),
                 ('(antibiotics | NY) - MA', {'10317.000047188',
                                              '10317.000051129',
                                              '10317.000033804',
                                              '10317.000001405',
                                              '10317.000001378',
                                              '10317.000003302'}),
                 ('(antibiotics | NY) - MA where AGE_CAT in ("20s","30s","40s")',  # noqa
                     {'10317.000051129',
                      '10317.000033804',
                      '10317.000001405',
                      '10317.000001378'}),
                 ('where AGE_CAT not in ("20s","30s","40s")',
                     {'10317.000047188', })]

        for test, exp in tests:
            obs = redbiom.search.metadata_full(test)
            self.assertEqual(obs, exp)

        # TODO: return dataframes

    def test_metadata_values_fail(self):
        tests = [('antibiotics and NY', TypeError, "Unsupported node type"),
                 ('NY where age & bmi', TypeError,
                  "Unsupported node type")]
        for test, ex, msg in tests:
            with self.assertRaisesRegexp(ex, msg):
                redbiom.search.metadata_full(test)

    def test_metadata_categories(self):
        test_cat = [('antibiotics', {'SUBSET_ANTIBIOTIC_HISTORY',
                                     'ANTIBIOTIC_HISTORY'}),
                    ('disease', {'LIVER_DISEASE',
                                 'CARDIOVASCULAR_DISEASE',
                                 'LUNG_DISEASE',
                                 'KIDNEY_DISEASE'}),
                    ('disease - liver', {'CARDIOVASCULAR_DISEASE',
                                         'LUNG_DISEASE',
                                         'KIDNEY_DISEASE'}),
                    ('disease & liver', {'LIVER_DISEASE', })]
        for test, exp in test_cat:
            obs = redbiom.search.metadata_full(test, categories=True)
            self.assertEqual(obs, exp)

    def test_metadata_categories_fail(self):
        tests = [('antibiotics and NY', TypeError, "Unsupported node type"),
                 ('NY where age & bmi', ValueError,
                  "where clauses not allowed")]
        for test, ex, msg in tests:
            with self.assertRaisesRegexp(ex, msg):
                redbiom.search.metadata_full(test, categories=True)

    def test_query_plan(self):
        tests = [('a', [('set', 'a')]),
                 ('a & b | c', [('set', 'a & b | c')]),
                 ('where age > 10', [('where', 'age > 10')]),
                 ('where (foo > 10) and not in ("bar", "baz")',
                     [('where', '(foo > 10) and not in ("bar", "baz")')]),
                 ('a where age >= 123', [('set', 'a'),
                                         ('where', 'age >= 123')]),
                 ('(a & b) | c where (foo < 10) or bar',
                     [('set', '(a & b) | c'),
                      ('where', '(foo < 10) or bar')])]
        for test, exp in tests:
            obs = redbiom.search.query_plan(test)
            self.assertEqual(obs, exp)

    def test_query_plan_fails(self):
        tests = [('', 'No query'),
                 ('where', 'No query')]
        for test, exp in tests:
            with self.assertRaisesRegexp(ValueError, exp):
                redbiom.search.query_plan(test)


if __name__ == '__main__':
    unittest.main()
