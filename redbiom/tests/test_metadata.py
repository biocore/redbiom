# ----------------------------------------------------------------------------
# Copyright (c) 2016-2017, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

# This code was sourced and adapted on 4/5/17 from:
# https://github.com/qiime2/qiime2/blob/qiime2/tests/test_metadata.py

# The license for this code can be found within the licenses/ directory.

import sqlite3
import unittest

import pandas as pd

from redbiom.metadata import Metadata


class TestIDs(unittest.TestCase):
    def test_default(self):
        df = pd.DataFrame({'Subject': ['subject-1', 'subject-1', 'subject-2'],
                           'SampleType': ['gut', 'tongue', 'gut']},
                          index=pd.Index(['S1', 'S2', 'S3'], name='id'))
        metadata = Metadata(df)

        actual = metadata.ids()
        expected = {'S1', 'S2', 'S3'}
        self.assertEqual(actual, expected)

    def test_incomplete_where(self):
        df = pd.DataFrame({'Subject': ['subject-1', 'subject-1', 'subject-2'],
                           'SampleType': ['gut', 'tongue', 'gut']},
                          index=['S1', 'S2', 'S3'])
        metadata = Metadata(df)

        where = "Subject='subject-1' AND SampleType="
        with self.assertRaises(ValueError):
            metadata.ids(where)

        where = "Subject="
        with self.assertRaises(ValueError):
            metadata.ids(where)

    def test_invalid_where(self):
        df = pd.DataFrame({'Subject': ['subject-1', 'subject-1', 'subject-2'],
                           'SampleType': ['gut', 'tongue', 'gut']},
                          index=['S1', 'S2', 'S3'])
        metadata = Metadata(df)

        where = "not-a-column-name='subject-1'"
        with self.assertRaises(ValueError):
            metadata.ids(where)

    def test_empty_result(self):
        df = pd.DataFrame({'Subject': ['subject-1', 'subject-1', 'subject-2'],
                           'SampleType': ['gut', 'tongue', 'gut']},
                          index=pd.Index(['S1', 'S2', 'S3'], name='id'))
        metadata = Metadata(df)

        where = "Subject='subject-3'"
        actual = metadata.ids(where)
        expected = set()
        self.assertEqual(actual, expected)

    def test_simple_expression(self):
        df = pd.DataFrame({'Subject': ['subject-1', 'subject-1', 'subject-2'],
                           'SampleType': ['gut', 'tongue', 'gut']},
                          index=pd.Index(['S1', 'S2', 'S3'], name='id'))
        metadata = Metadata(df)

        where = "Subject='subject-1'"
        actual = metadata.ids(where)
        expected = {'S1', 'S2'}
        self.assertEqual(actual, expected)

        where = "Subject='subject-2'"
        actual = metadata.ids(where)
        expected = {'S3'}
        self.assertEqual(actual, expected)

        where = "Subject='subject-3'"
        actual = metadata.ids(where)
        expected = set()
        self.assertEqual(actual, expected)

        where = "SampleType='gut'"
        actual = metadata.ids(where)
        expected = {'S1', 'S3'}
        self.assertEqual(actual, expected)

        where = "SampleType='tongue'"
        actual = metadata.ids(where)
        expected = {'S2'}
        self.assertEqual(actual, expected)

    def test_more_complex_expressions(self):
        df = pd.DataFrame({'Subject': ['subject-1', 'subject-1', 'subject-2'],
                           'SampleType': ['gut', 'tongue', 'gut']},
                          index=pd.Index(['S1', 'S2', 'S3'], name='id'))
        metadata = Metadata(df)

        where = "Subject='subject-1' OR Subject='subject-2'"
        actual = metadata.ids(where)
        expected = {'S1', 'S2', 'S3'}
        self.assertEqual(actual, expected)

        where = "Subject='subject-1' AND Subject='subject-2'"
        actual = metadata.ids(where)
        expected = set()
        self.assertEqual(actual, expected)

        where = "Subject='subject-1' AND SampleType='gut'"
        actual = metadata.ids(where)
        expected = {'S1'}
        self.assertEqual(actual, expected)

    def test_index_with_column_name_clash(self):
        df = pd.DataFrame(
            {'Subject': ['subject-1', 'subject-1', 'subject-2'],
             'SampleType': ['gut', 'tongue', 'gut']},
            index=pd.Index(['S1', 'S2', 'S3'], name='SampleType'))
        metadata = Metadata(df)

        with self.assertRaises(sqlite3.OperationalError):
            metadata.ids(where="Subject='subject-1'")

    def test_duplicate_columns(self):
        df = pd.DataFrame([['subject-1', 'gut'],
                           ['subject-1', 'tongue'],
                           ['subject-2', 'gut']],
                          index=['S1', 'S2', 'S3'], columns=['foo', 'foo'])
        metadata = Metadata(df)

        with self.assertRaises(sqlite3.OperationalError):
            metadata.ids(where="foo='subject-2'")

    def test_query_by_index(self):
        df = pd.DataFrame({'Subject': ['subject-1', 'subject-1', 'subject-2'],
                           'SampleType': ['gut', 'tongue', 'gut']},
                          index=pd.Index(['S1', 'S2', 'S3'], name='id'))
        metadata = Metadata(df)

        actual = metadata.ids(where="id='S2' OR id='S1'")
        expected = {'S1', 'S2'}
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
