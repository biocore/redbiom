import unittest

import requests

from redbiom import get_config
from redbiom.requests import (valid, _parse_validate_request, _format_request,
                              make_post, make_get)


config = get_config()


class RequestsTests(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(valid('test'), None)
        with self.assertRaises(ValueError):
            valid('doesnt exist')

    def test_parse_valid_request(self):
        # Webdis does not leverage status codes too aggressively.
        # This design decision makes sense as Webdis does not need to concern
        # itself with the specifics of the Redis commands, however it also
        # makes it somewhat annoying to sanity check a response. As is, the
        # checking is light. Since the test environment does not utilize ACLs,
        # it is bit difficult to trigger a non-200...
        key = 'test:data:10317.000033804'
        req = requests.get('http://127.0.0.1:7379/EXISTS/%s' % key)
        exp = 1
        obs = _parse_validate_request(req, 'EXISTS')
        self.assertEqual(obs, exp)

        key = 'test:data:10317.000033804foobar'
        req = requests.get('http://127.0.0.1:7379/EXISTS/%s' % key)
        exp = 0
        obs = _parse_validate_request(req, 'EXISTS')
        self.assertEqual(obs, exp)

        # cram a massive URL
        key = 'test:data:10317.000033804foobar' + 'asdasdasd' * 10000
        req = requests.get('http://127.0.0.1:7379/EXISTS/%s' % key)
        with self.assertRaises(requests.HTTPError):
            _parse_validate_request(req, 'EXISTS')

        # issue a post against a URL
        key = 'test:data:10317.000033804foobar' + 'asdasdasd' * 10000
        req = requests.post('http://127.0.0.1:7379/EXISTS/%s' % key)
        with self.assertRaises(requests.HTTPError):
            _parse_validate_request(req, 'EXISTS')

    def test_format_request(self):
        self.assertEqual(_format_request(None, 'foo', 'bar'), "foo/bar")
        self.assertEqual(_format_request(None, 'foo', ''), "foo/")
        self.assertEqual(_format_request(None, '', 'bar'), "/bar")
        self.assertEqual(_format_request('baz', 'foo', 'bar'), "foo/baz:bar")

    def test_make_post(self):
        post = make_post(config)

        exp = [True, "OK"]
        obs = post('test', 'SET', 'foo/10')
        self.assertEqual(obs, exp)

    def test_make_get(self):
        get = make_get(config)

        exp = 'UBERON:feces'
        obs = get('metadata', 'HGET', 'category:BODY_SITE/10317.000033804')
        self.assertEqual(obs, exp)


if __name__ == '__main__':
    unittest.main()
