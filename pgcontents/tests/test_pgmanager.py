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
from cryptography.fernet import Fernet
from itertools import combinations

from pgcontents.pgmanager import PostgresContentsManager
from .utils import (
    assertRaisesHTTPError,
    clear_test_db,
    make_fernet,
    _norm_unicode,
    TEST_DB_URL,
    remigrate_test_schema,
)
from ..crypto import FernetEncryption
from ..utils.ipycompat import TestContentsManager
from ..utils.sync import walk_files_with_content

setup_module = remigrate_test_schema


class PostgresContentsManagerTestCase(TestContentsManager):

    @classmethod
    def tearDownClass(cls):
        # Override the superclass teardown.
        pass

    def setUp(self):
        self.crypto = make_fernet()
        self.contents_manager = PostgresContentsManager(
            user_id='test',
            db_url=TEST_DB_URL,
            crypto=self.crypto,
        )
        self.contents_manager.ensure_user()
        self.contents_manager.ensure_root_directory()

        # We need to dispose of any engines created during tests or else the
        # engine's QueuePool will leak connections even once this suite has
        # finished running. Then as other test suites start to run, the number
        # of connections will eventually creep up to the maximum number that
        # postgres allows. For reference, see the SQLAlchemy docs here:
        # https://docs.sqlalchemy.org/en/13/core/connections.html#engine-disposal
        #
        # This pattern should be repeated in any test class that creates a
        # PostgresContentsManager or a PostgresCheckpoints object (note that
        # even though the checkpoints manager lives on the contents manager it
        # still creates its own engine). An alternative solution to calling
        # dispose here would be to have these classes create engines with a
        # NullPool when testing, but that 1) adds more latency, and 2) adds
        # test-specific behavior to the classes themselves.
        self.addCleanup(self.contents_manager.engine.dispose)
        self.addCleanup(self.contents_manager.checkpoints.engine.dispose)

    def tearDown(self):
        clear_test_db()

    def set_pgmgr_attribute(self, name, value):
        """
        Overridable method for setting attributes on our pgmanager.

        This exists so that we can re-use the tests here in
        test_hybrid_manager.
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

    def test_walk_files_with_content(self):
        all_dirs = ['foo', 'bar', 'foo/bar', 'foo/bar/foo', 'foo/bar/foo/bar']
        for dir in all_dirs:
            self.make_populated_dir(dir)

        expected_file_paths = [
            u'bar/file.txt',
            u'bar/nb.ipynb',
            u'foo/file.txt',
            u'foo/nb.ipynb',
            u'foo/bar/file.txt',
            u'foo/bar/nb.ipynb',
            u'foo/bar/foo/file.txt',
            u'foo/bar/foo/nb.ipynb',
            u'foo/bar/foo/bar/file.txt',
            u'foo/bar/foo/bar/nb.ipynb',
        ]

        cm = self.contents_manager

        filepaths = []
        for file in walk_files_with_content(cm):
            self.assertEqual(
                file,
                cm.get(file['path'], content=True)
            )
            filepaths.append(_norm_unicode(file['path']))

        self.assertEqual(
            filepaths.sort(),
            expected_file_paths.sort()
        )

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

    def test_get_file_id(self):
        cm = self.contents_manager

        # Create a new notebook.
        nb, name, path = self.new_notebook()
        model = cm.get(path)

        # Make sure we can get the id and it's not none.
        id_ = cm.get_file_id(path)
        self.assertIsNotNone(id_)

        # Make sure the id stays the same after we edit and save.
        self.add_code_cell(model['content'])
        cm.save(model, path)
        self.assertEqual(id_, cm.get_file_id(path))

        # Make sure the id stays the same after a rename.
        updated_path = "updated_name.ipynb"
        cm.rename(path, updated_path)
        self.assertEqual(id_, cm.get_file_id(updated_path))

    def test_rename_file(self):
        cm = self.contents_manager
        nb, nb_name, nb_path = self.new_notebook()
        assert nb_name == 'Untitled.ipynb'

        # A simple rename of the file within the same directory.
        cm.rename(nb_path, 'new_name.ipynb')
        assert cm.get('new_name.ipynb')['path'] == 'new_name.ipynb'

        # The old file name should no longer be found.
        with assertRaisesHTTPError(self, 404):
            cm.get(nb_name)

        # Test that renaming outside of the root fails.
        with assertRaisesHTTPError(self, 404):
            cm.rename('../foo', '../bar')

        # Test that renaming something to itself fails.
        with assertRaisesHTTPError(self, 409):
            cm.rename('new_name.ipynb', 'new_name.ipynb')

        # Test that renaming a non-existent file fails.
        with assertRaisesHTTPError(self, 404):
            cm.rename('non_existent.ipynb', 'some_name.ipynb')

        # Now test moving a file.
        self.make_dir('My Folder')
        nb_destination = 'My Folder/new_name.ipynb'
        cm.rename('new_name.ipynb', nb_destination)

        updated_notebook_model = cm.get(nb_destination)
        assert updated_notebook_model['name'] == 'new_name.ipynb'
        assert updated_notebook_model['path'] == nb_destination

        # The old file name should no longer be found.
        with assertRaisesHTTPError(self, 404):
            cm.get('new_name.ipynb')

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

        # Renaming the root directory should raise
        with assertRaisesHTTPError(self, 409):
            cm.rename('', 'baz')

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

    def test_move_empty_directory(self):
        cm = self.contents_manager

        self.make_dir('Parent Folder')
        self.make_dir('Child Folder')

        # A rename moving one folder into the other.
        child_folder_destination = 'Parent Folder/Child Folder'
        cm.rename('Child Folder', child_folder_destination)

        updated_parent_model = cm.get('Parent Folder')
        assert updated_parent_model['path'] == 'Parent Folder'
        assert len(updated_parent_model['content']) == 1

        with assertRaisesHTTPError(self, 404):
            # Should raise a 404 because the contents manager should not be
            # able to find a folder with this path.
            cm.get('Child Folder')

        # Confirm that the child folder has moved into the parent folder.
        updated_child_model = cm.get(child_folder_destination)
        assert updated_child_model['name'] == 'Child Folder'
        assert updated_child_model['path'] == child_folder_destination

        # Test moving it back up.
        cm.rename('Parent Folder/Child Folder', 'Child Folder')

        updated_parent_model = cm.get('Parent Folder')
        assert len(updated_parent_model['content']) == 0

        with assertRaisesHTTPError(self, 404):
            cm.get('Parent Folder/Child Folder')

        updated_child_model = cm.get('Child Folder')
        assert updated_child_model['name'] == 'Child Folder'
        assert updated_child_model['path'] == 'Child Folder'

    def test_move_populated_directory(self):
        cm = self.contents_manager

        all_dirs = [
            'foo', 'foo/bar', 'foo/bar/populated_dir',
            'biz', 'biz/buz',
        ]

        for dir_ in all_dirs:
            if dir_ == 'foo/bar/populated_dir':
                self.make_populated_dir(dir_)
                self.check_populated_dir_files(dir_)
            else:
                self.make_dir(dir_)

        # Move the populated directory over to "biz".
        cm.rename('foo/bar/populated_dir', 'biz/populated_dir')

        bar_model = cm.get('foo/bar')
        assert len(bar_model['content']) == 0

        biz_model = cm.get('biz')
        assert len(biz_model['content']) == 2

        with assertRaisesHTTPError(self, 404):
            cm.get('foo/bar/populated_dir')

        populated_dir_model = cm.get('biz/populated_dir')
        assert populated_dir_model['name'] == 'populated_dir'
        assert populated_dir_model['path'] == 'biz/populated_dir'
        self.check_populated_dir_files('biz/populated_dir')

        # Test moving a directory with sub-directories and files that go
        # multiple layers deep.
        self.make_populated_dir('biz/populated_dir/populated_sub_dir')
        self.make_dir('biz/populated_dir/populated_sub_dir/empty_dir')
        cm.rename('biz/populated_dir', 'populated_dir')

        populated_dir_model = cm.get('populated_dir')
        assert populated_dir_model['name'] == 'populated_dir'
        assert populated_dir_model['path'] == 'populated_dir'
        self.check_populated_dir_files('populated_dir')
        self.check_populated_dir_files('populated_dir/populated_sub_dir')

        empty_dir_model = cm.get('populated_dir/populated_sub_dir/empty_dir')
        assert empty_dir_model['name'] == 'empty_dir'
        assert (
            empty_dir_model['path'] ==
            'populated_dir/populated_sub_dir/empty_dir'
        )
        assert len(empty_dir_model['content']) == 0

    def test_max_file_size(self):

        cm = self.contents_manager
        max_size = 120
        self.set_pgmgr_attribute('max_file_size_bytes', max_size)

        def size_in_db(s):
            return len(self.crypto.encrypt(b64encode(s.encode('utf-8'))))

        # max_file_size_bytes should be based on the size in the database, not
        # the size of the input.
        good = 'a' * 10
        self.assertEqual(size_in_db(good), max_size)
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

        bad = 'a' * 30
        self.assertGreater(size_in_db(bad), max_size)
        with assertRaisesHTTPError(self, 413):
            cm.save(
                model={
                    'content': bad,
                    'format': 'text',
                    'type': 'file',
                },
                path='bad.txt',
            )

    def test_changing_crypto_disables_ability_to_read(self):
        cm = self.contents_manager

        _, _, nb_path = self.new_notebook()
        nb_model = cm.get(nb_path)

        file_path = 'file.txt'
        cm.save(
            model={
                'content': 'not encrypted',
                'format': 'text',
                'type': 'file',
            },
            path=file_path,
        )
        file_model = cm.get(file_path)

        alt_key = b64encode(b'fizzbuzz' * 4)
        self.set_pgmgr_attribute('crypto', FernetEncryption(Fernet(alt_key)))

        with assertRaisesHTTPError(self, 500):
            cm.get(nb_path)

        with assertRaisesHTTPError(self, 500):
            cm.get(file_path)

        # Restore the original crypto instance and verify that we can still
        # decrypt.
        self.set_pgmgr_attribute('crypto', self.crypto)

        decrypted_nb_model = cm.get(nb_path)
        self.assertEqual(nb_model, decrypted_nb_model)

        decrypted_file_model = cm.get(file_path)
        self.assertEqual(file_model, decrypted_file_model)

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
