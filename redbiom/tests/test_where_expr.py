import unittest

from redbiom.where_expr import whereeval


mock_db = {'age': {'A': '3', 'B': '20', 'C': '10', 'D': '5'},
           'sex': {'A': 'female', 'B': 'female', 'C': 'unknown', 'D': 'male'}}

def mock_get(ignored1, ignored2, arg):
    return mock_db.get(arg, dict())


class WhereTests(unittest.TestCase):
    def test_eval_danger(self):
        tests = ["print('hi')",
                 "",
                 "import sys",
                 "None"]
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
                 ("foo is bar", set())]

        for test, exp in tests:
            obs = whereeval(test, get=mock_get)
            self.assertEqual(set(obs.index), exp)


if __name__ == '__main__':
    unittest.main()
