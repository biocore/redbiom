import unittest
from functools import reduce
import random

import biom

from redbiom.util import (float_or_nan, from_or_nargs,
                          samples_from_observations, has_sample_metadata)


table = biom.load_table('test.biom')


class UtilTests(unittest.TestCase):
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

    def test_samples_from_observations(self):
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
