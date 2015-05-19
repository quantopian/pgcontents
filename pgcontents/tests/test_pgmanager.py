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
from itertools import combinations

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

    def make_populated_dir(self, api_path):
        """
        Create a directory at api_path with a notebook and a text file.
        """
        self.make_dir(api_path)
        self.contents_manager.new(
            path='/'.join([api_path, 'nb.ipynb'])
        )
        self.contents_manager.new(
            path='/'.join([api_path, 'file.txt'])
        )

    def check_populated_dir_files(self, api_path):
        """
        Check that a directory created with make_populated_dir has a
        notebook and a text file with expected names.
        """
        dirmodel = self.contents_manager.get(api_path)
        self.assertEqual(dirmodel['path'], api_path)
        self.assertEqual(dirmodel['type'], 'directory')
        for entry in dirmodel['content']:
            # Skip any subdirectories created after the fact.
            if entry['type'] == 'directory':
                continue
            elif entry['type'] == 'file':
                self.assertEqual(entry['name'], 'file.txt')
                self.assertEqual(
                    entry['path'],
                    '/'.join([api_path, 'file.txt']),
                )
            elif entry['type'] == 'notebook':
                self.assertEqual(entry['name'], 'nb.ipynb')
                self.assertEqual(
                    entry['path'],
                    '/'.join([api_path, 'nb.ipynb']),
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
        """
        Create a directory hierarchy that looks like:

        foo/
          ...
          bar/
            ...
            foo/
              ...
              bar/
                ...
        bar/

        then rename /foo/bar -> /foo/bar_changed and verify that all changes
        propagate correctly.
        """
        cm = self.contents_manager

        all_dirs = ['foo', 'bar', 'foo/bar', 'foo/bar/foo', 'foo/bar/foo/bar']
        unchanged_dirs = all_dirs[:2]
        changed_dirs = all_dirs[2:]

        for dir_ in all_dirs:
            self.make_populated_dir(dir_)
            self.check_populated_dir_files(dir_)

        # Renaming to an extant directory should raise
        for src, dest in combinations(all_dirs, 2):
            with assertRaisesHTTPError(self, 409):
                cm.rename(src, dest)

        # Verify that we can't create a new notebook in the (nonexistent)
        # target directory
        with assertRaisesHTTPError(self, 404):
            cm.new_untitled('foo/bar_changed', ext='.ipynb')

        cm.rename('foo/bar', 'foo/bar_changed')

        # foo/ and bar/ should be unchanged
        for unchanged in unchanged_dirs:
            self.check_populated_dir_files(unchanged)

        # foo/bar/ and subdirectories should have leading prefixes changed
        for changed_dirname in changed_dirs:
            with assertRaisesHTTPError(self, 404):
                cm.get(changed_dirname)
            new_dirname = changed_dirname.replace(
                'foo/bar', 'foo/bar_changed', 1
            )
            self.check_populated_dir_files(new_dirname)

        # Verify that we can now create a new notebook in the changed directory
        cm.new_untitled('foo/bar_changed', ext='.ipynb')

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
