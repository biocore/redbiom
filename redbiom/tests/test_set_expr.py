import unittest

from redbiom.set_expr import seteval


mock_db = {'W': {1, 2},
           'X': {1, 2, 3},
           'Y': {4, 5},
           'Z': {1, 3, 4}}


def mock_get(ignored1, ignored2, arg):
    return mock_db.get(arg, set())


class SetTests(unittest.TestCase):
    def test_eval_bad_types(self):
        tests = ["A & 10",
                 "10 & B",
                 "((A & B) | C) & 10.0"]
        for test in tests:
            with self.assertRaises(TypeError):
                seteval(test, get=mock_get)

    def test_eval_danger(self):
        tests = ["print('hi')",
                 "",
                 "import sys"]
        for test in tests:
            # both syntax and type can arise, and the critical thing is we just
            # dont want stupid code run
            with self.assertRaises(Exception):
                seteval(test, get=mock_get)

    def test_eval_nonsense(self):
        tests = ["A &",
                 "& B",
                 "A ^",
                 "(A & B"]
        for test in tests:
            with self.assertRaises(SyntaxError):
                seteval(test, get=mock_get)

    def test_eval(self):
        tests = [("X & Y", set()),
                 ("(W ^ X) | Y", {3, 4, 5}),
                 ("W | X | Y | Z", {1, 2, 3, 4, 5}),
                 ("W & X | Z", {1, 2, 3, 4})]
        for test, exp in tests:
            obs = seteval(test, get=mock_get)
            self.assertEqual(obs, exp)


if __name__ == '__main__':
    unittest.main()
