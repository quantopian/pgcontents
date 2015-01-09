#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Run IPython's TestContentsManager using PostgresContentsManager.
"""
from __future__ import unicode_literals

from base64 import b64encode

from IPython.html.services.contents.tests.test_manager import TestContentsManager  # noqa
from tornado.web import HTTPError

from ..pgmanager import PostgresContentsManager
from .utils import TEST_DB_URL


class PostgresContentsManagerTestCase(TestContentsManager):

    def setUp(self):
        self.contents_manager = PostgresContentsManager(
            user_id='test',
            db_url=TEST_DB_URL,
        )
        self.contents_manager.purge_db()
        self.contents_manager.ensure_user()
        self.contents_manager.ensure_root_directory()

    def tearDown(self):
        self.contents_manager.purge_db()

    def make_dir(self, api_path):
        self.contents_manager.new(
            model={'type': 'directory'},
            path=api_path,
        )

    def test_max_file_size(self):
        cm = self.contents_manager
        max_size = 68
        cm.max_file_size_bytes = max_size

        good = 'a' * 51
        self.assertEqual(len(b64encode(good)), max_size)
        cm.save(
            model={
                'content': good,
                'format': 'text',
                'type': 'file',
            },
            path='good.txt',
        )
        result = cm.get('good.txt')
        self.assertEqual(result['content'], good)

        bad = 'a' * 52
        self.assertGreater(bad, max_size)
        with self.assertRaises(HTTPError) as ctx:
            cm.save(
                model={
                    'content': bad,
                    'format': 'text',
                    'type': 'file',
                },
                path='bad.txt',
            )
        err = ctx.exception
        self.assertEqual(err.status_code, 413)


# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
