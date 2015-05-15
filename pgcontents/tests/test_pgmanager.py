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
from pgcontents.pgmanager import PostgresContentsManager
from .utils import (
    assertRaisesHTTPError,
    drop_testing_db_tables,
    migrate_testing_db,
    TEST_DB_URL,
)


class PostgresContentsManagerTestCase(TestContentsManager):

    def setUp(self):
        drop_testing_db_tables()
        migrate_testing_db()

        self.contents_manager = PostgresContentsManager(
            user_id='test',
            db_url=TEST_DB_URL,
        )
        self.contents_manager.ensure_user()
        self.contents_manager.ensure_root_directory()

    def set_pgmgr_attribute(self, name, value):
        """
        Overridable method for setting attributes on our pgmanager.

        This exists so that HybridContentsManager can use
        """
        setattr(self.contents_manager, name, value)

    def make_dir(self, api_path):
        self.contents_manager.new(
            model={'type': 'directory'},
            path=api_path,
        )

    def tearDown(self):
        drop_testing_db_tables()
        migrate_testing_db()

    def test_modified_date(self):

        cm = self.contents_manager

        # Create a new notebook.
        nb, name, path = self.new_notebook()
        model = cm.get(path)

        # Add a cell and save.
        self.add_code_cell(model['content'])
        cm.save(model, path)

        # Reload notebook and verify that last_modified incremented.
        saved = cm.get(path)
        self.assertGreater(saved['last_modified'], model['last_modified'])

        # Move the notebook and verify that last_modified incremented.
        new_path = 'renamed.ipynb'
        cm.rename(path, new_path)
        renamed = cm.get(new_path)
        self.assertGreater(renamed['last_modified'], saved['last_modified'])

    def test_rename_directory(self):
        cm = self.contents_manager

        # Create an untitled directory
        foo_dir = cm.new_untitled(type='directory')
        old_foo_dir_path = foo_dir['path']

        # Change the path on the model and call cm.update to rename
        foo_dir_path = 'foo'
        foo_dir['path'] = foo_dir_path
        foo_dir = cm.update(foo_dir, old_foo_dir_path)

        # Check that the cm.update returns a model
        assert isinstance(foo_dir, dict)

        # Make sure the untitled directory is gone
        self.assertRaises(HTTPError, cm.get, old_foo_dir_path)

        # Create a subdirectory
        bar_dir = cm.new(
            model={'type': 'directory'},
            path='foo/bar',
        )
        old_bar_dir_path = bar_dir['path']

        # Create a file in the subdirectory
        bar_file = cm.new_untitled(path='foo/bar', type='notebook')
        old_bar_file_path = bar_file['path']

        # Create another subdirectory one level deeper.  Use 'foo' for the name
        # again to catch issues with replacing all instances of a substring
        # instead of just the first.
        bar2_dir = cm.new(
            model={'type': 'directory'},
            path='foo/bar/bar',
        )
        old_bar2_dir_path = bar2_dir['path']

        # Create a file in the two-level deep directory we just created
        bar2_file = cm.new_untitled(path=old_bar2_dir_path, type='notebook')
        old_bar2_file_path = bar2_file['path']

        # Change the path of the first bar directory
        new_bar_dir_path = 'foo/bar_changed'
        bar_dir['path'] = new_bar_dir_path
        bar_dir = cm.update(bar_dir, old_bar_dir_path)
        self.assertIn('name', bar_dir)
        self.assertIn('path', bar_dir)
        self.assertEqual(bar_dir['name'], 'bar_changed')

        # Make sure calling cm.get on any old paths throws an exception
        self.assertRaises(HTTPError, cm.get, old_bar_dir_path)
        self.assertRaises(HTTPError, cm.get, old_bar2_dir_path)
        self.assertRaises(HTTPError, cm.get, old_bar_file_path)
        self.assertRaises(HTTPError, cm.get, old_bar2_file_path)

        def try_get_new_path(full_old_path):
            # replace the first occurence of the old path with the new one
            new_path = full_old_path.replace(
                old_bar_dir_path,
                new_bar_dir_path,
                1
            )
            new_model = cm.get(new_path)
            self.assertIn('name', new_model)
            self.assertIn('path', new_model)

        # Make sure the directories and files can be found at their new paths
        try_get_new_path(foo_dir_path) # top level foo dir should be unchanged
        try_get_new_path(old_bar_file_path)
        try_get_new_path(old_bar2_dir_path)
        try_get_new_path(old_bar2_file_path)

    def test_max_file_size(self):

        cm = self.contents_manager
        max_size = 68
        self.set_pgmgr_attribute('max_file_size_bytes', max_size)

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
        with assertRaisesHTTPError(self, 413):
            cm.save(
                model={
                    'content': bad,
                    'format': 'text',
                    'type': 'file',
                },
                path='bad.txt',
            )

    def test_relative_paths(self):
        cm = self.contents_manager

        nb, name, path = self.new_notebook()
        self.assertEqual(cm.get(path), cm.get('/a/../' + path))
        self.assertEqual(cm.get(path), cm.get('/a/../b/c/../../' + path))

        with assertRaisesHTTPError(self, 404):
            cm.get('..')
        with assertRaisesHTTPError(self, 404):
            cm.get('foo/../../../bar')
        with assertRaisesHTTPError(self, 404):
            cm.delete('../foo')
        with assertRaisesHTTPError(self, 404):
            cm.rename('../foo', '../bar')
        with assertRaisesHTTPError(self, 404):
            cm.save(model={
                'type': 'file',
                'content': u'',
                'format': 'text',
            }, path='../foo')


# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
