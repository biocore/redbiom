import unittest

import requests
import biom
import pandas as pd

from redbiom import get_config
import redbiom.admin
from redbiom._requests import (valid, _parse_validate_request, _format_request,
                               make_post, make_get, make_put, buffered)
from redbiom.tests import assert_test_env

assert_test_env()


config = get_config()
table = biom.load_table('test.biom')
metadata = pd.read_csv('test.txt', sep='\t', dtype=str)


class RequestsTests(unittest.TestCase):
    def setUp(self):
        host = config['hostname']
        req = requests.get(host + '/FLUSHALL')
        assert req.status_code == 200

    def test_valid(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        self.assertEqual(valid('test'), None)
        with self.assertRaises(ValueError):
            valid('doesnt exist')

    def test_parse_valid_request(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.ScriptManager.load_scripts(read_only=False)
        redbiom.admin.load_sample_data(table, context, tag=None)
        # Webdis does not leverage status codes too aggressively.
        # This design decision makes sense as Webdis does not need to concern
        # itself with the specifics of the Redis commands, however it also
        # makes it somewhat annoying to sanity check a response. As is, the
        # checking is light. Since the test environment does not utilize ACLs,
        # it is bit difficult to trigger a non-200...
        key = 'test:sample:UNTAGGED_10317.000033804'
        req = requests.get('http://127.0.0.1:7379/EXISTS/%s' % key)
        exp = 1
        obs = _parse_validate_request(req, 'EXISTS')
        self.assertEqual(obs, exp)

        key = 'test:data:UNTAGGED_10317.000033804foobar'
        req = requests.get('http://127.0.0.1:7379/EXISTS/%s' % key)
        exp = 0
        obs = _parse_validate_request(req, 'EXISTS')
        self.assertEqual(obs, exp)

        # cram a massive URL
        key = 'test:data:UNTAGGED_10317.000033804foobar' + 'asdasdasd' * 10000
        req = requests.get('http://127.0.0.1:7379/EXISTS/%s' % key)
        with self.assertRaises(requests.HTTPError):
            _parse_validate_request(req, 'EXISTS')

        # issue a post against a URL
        key = 'test:data:UNTAGGED_10317.000033804foobar' + 'asdasdasd' * 10000
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
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        get = make_get(config)

        exp = 'UBERON:feces'
        obs = get('metadata', 'HGET', 'category:BODY_SITE/10317.000033804')
        self.assertEqual(obs, exp)

    def test_make_put(self):
        put = make_put(config)

        exp = [True, "OK"]
        obs = put('test', 'SET', 'bar', '1234')
        self.assertEqual(obs, exp)

    def test_buffered_not_multi(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        redbiom.admin.ScriptManager.load_scripts(read_only=False)
        redbiom.admin.load_sample_data(table, context, tag=None)

        samples = iter(['UNTAGGED_10317.000033804', 'does not exist'])
        exp_items = ['UNTAGGED_10317.000033804', 'does not exist']
        exp = 1  # because only 1 exists
        gen = buffered(samples, 'sample', 'EXISTS', 'test')
        obs_items, obs = next(gen)
        self.assertEqual(obs, exp)
        self.assertEqual(obs_items, exp_items)

        with self.assertRaises(StopIteration):
            next(gen)

    def test_buffered_multi(self):
        context = 'test'
        redbiom.admin.create_context(context, 'foo')
        redbiom.admin.load_sample_metadata(metadata)
        samples = iter(['10317.000033804', 'does not exist'])
        exp_items = ['10317.000033804', 'does not exist']
        exp = ['UBERON:feces', None]
        gen = buffered(samples, None, 'HMGET', 'metadata',
                       multikey='category:BODY_SITE')
        obs_items, obs = next(gen)
        self.assertEqual(obs, exp)
        self.assertEqual(obs_items, exp_items)

        with self.assertRaises(StopIteration):
            next(gen)


if __name__ == '__main__':
    unittest.main()
