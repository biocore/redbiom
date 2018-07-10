import unittest
import pandas as pd
import pandas.util.testing as pdt

from redbiom.where_expr import whereeval, _cast_retain_numeric


mock_db = {'age': {'A': '3', 'B': '20', 'C': '10', 'D': '5'},
           'other': {'B': '5', 'E': '10', 'C': '15'},
           'sex': {'A': 'female', 'B': 'female', 'C': 'unknown', 'D': 'male'},
           'realworld': {'A': '3', 'C': '5', 'D': 'foo'}}


def mock_get(ignored1, ignored2, arg):
    return mock_db.get(arg, dict())


class WhereTests(unittest.TestCase):
    def test_cast_retain_numeric(self):
        tests = [(pd.Series(['a', '10', '1.23']),
                  pd.Series([10.0, 1.23], index=[1, 2])),
                 (pd.Series(['a', 'b', 'c']), pd.Series([])),
                 (pd.Series(['1', '2', '3', '4']),
                  pd.Series([1, 2, 3, 4], index=[0, 1, 2, 3]))]

        for test, exp in tests:
            obs = _cast_retain_numeric(test).reindex()
            pdt.assert_series_equal(obs, exp)

    def test_eval_danger(self):
        tests = ["print('hi')",
                 "",
                 "import sys"]
        for test in tests:
            # both syntax and type can arise, and the critical thing is we just
            # dont want stupid code run
            with self.assertRaises(Exception):
                whereeval(test, get=mock_get)

    def test_eval_nonsense(self):
        tests = ["sex or",
                 "age >",
                 "foo bar"]
        for test in tests:
            with self.assertRaises(SyntaxError):
                whereeval(test, get=mock_get)

    def test_whereeval(self):
        tests = [("age < 10", {'A', 'D'}),
                 ("age > 0", {'A', 'B', 'C', 'D'}),
                 ("age == 5", {'D', }),
                 ("(age >= 5) <= 15", {'D', 'C'}),
                 ("sex == 'male'", {'D', }),
                 ("sex in ('male', 'female')", {'A', 'B', 'D'}),
                 ("sex is 'male' or age < 11", {'D', 'A', 'C'}),
                 ("(age <= 10) != 8 and sex is 'male'", {'D', }),
                 ("(age <= 10) != 8 or sex is 'male'", {'D', 'A', 'C'}),
                 ("(age <= 10) != 8 and sex is 'female'", {'A', }),
                 ("(age <= 10) != 8 or sex is 'female'", {'A', 'B', 'C', 'D'}),
                 ("(age <= 10) != 8", {'A', 'C', 'D'}),
                 ("(age <= 10) != 8 and sex is not 'female'", {'C', 'D'}),
                 ("sex is not 'female' and sex is not 'male'", {'C'}),
                 ("foo is bar", set()),
                 ("age > other", {'B', }),
                 ("realworld in ('5', 'foo')", {'C', 'D'}),
                 ("realworld > 4", {'C', }),
                 ("other is not None", {'B', 'E', 'C'})]

        for test, exp in tests:
            obs = whereeval(test, get=mock_get)
            self.assertEqual(set(obs.index), exp)


if __name__ == '__main__':
    unittest.main()
