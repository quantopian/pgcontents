# encoding: utf-8
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
Run IPython's APITest for ContentsManager using PostgresContentsManager.
"""
from __future__ import unicode_literals
from base64 import (
    b64encode,
)
from dateutil.parser import parse

from IPython.config import Config
from IPython.html.services.contents.tests.test_contents_api import APITest

from ..constants import UNLIMITED
from ..pgmanager import (
    PostgresContentsManager,
    writes_base64,
)
from ..checkpoints import PostgresCheckpoints
from ..query import (
    create_directory,
    dir_exists,
    file_exists,
    save_file,
)
from .utils import TEST_DB_URL
from ..utils.sync import walk


class PGContentsAPITestBase(APITest):
    """
    APITest that also runs a test for our implementation of `walk`.
    """

    def test_walk(self):
        """
        Test ContentsManager.walk.
        """
        results = list(walk(self.notebook.contents_manager))
        expected = [
            (
                '',
                [
                    'Directory with spaces in',
                    'foo',
                    'ordering',
                    u'unicodé',
                    u'å b',
                ],
                ['inroot.blob', 'inroot.ipynb', 'inroot.txt'],
            ),
            (
                'Directory with spaces in',
                [],
                ['inspace.blob', 'inspace.ipynb', 'inspace.txt'],
            ),
            (
                'foo',
                ['bar'],
                [
                    'a.blob', 'a.ipynb', 'a.txt',
                    'b.blob', 'b.ipynb', 'b.txt',
                    'name with spaces.blob',
                    'name with spaces.ipynb',
                    'name with spaces.txt',
                    u'unicodé.blob', u'unicodé.ipynb', u'unicodé.txt'
                ]
            ),
            (
                'foo/bar',
                [],
                ['baz.blob', 'baz.ipynb', 'baz.txt'],
            ),
            (
                'ordering',
                [],
                [
                    'A.blob', 'A.ipynb', 'A.txt',
                    'C.blob', 'C.ipynb', 'C.txt',
                    'b.blob', 'b.ipynb', 'b.txt',
                ],
            ),
            (
                u'unicodé',
                [],
                ['innonascii.blob', 'innonascii.ipynb', 'innonascii.txt'],
            ),
            (
                u'å b',
                [],
                [u'ç d.blob', u'ç d.ipynb', u'ç d.txt'],
            ),
        ]

        for idx, (dname, subdirs, files) in enumerate(expected):
            result_dname, result_subdirs, result_files = results[idx]
            if dname == '':
                sep = ''
            else:
                sep = '/'
            self.assertEqual(
                dname,
                result_dname,
            )
            self.assertEqual(
                [sep.join([dname, sub]) for sub in subdirs],
                result_subdirs,
            )
            self.assertEqual(
                [sep.join([dname, fname]) for fname in files],
                result_files,
            )

    def test_list_checkpoints_sorting(self):
        """
        Test that list_checkpoints returns results sorted by last_modified.
        """
        for i in range(5):
            self.api.new_checkpoint('foo/a.ipynb')
        cps = self.api.get_checkpoints('foo/a.ipynb').json()

        self.assertEqual(
            cps,
            sorted(
                cps,
                key=lambda cp: parse(cp['last_modified']),
            )
        )


class PostgresContentsAPITest(PGContentsAPITestBase):

    config = Config()
    config.NotebookApp.contents_manager_class = PostgresContentsManager
    config.PostgresContentsManager.user_id = 'test'
    config.PostgresContentsManager.db_url = TEST_DB_URL

    # Don't support hidden directories.
    hidden_dirs = []

    @property
    def contents_manager(self):
        return self.notebook.contents_manager

    @property
    def user_id(self):
        return self.contents_manager.user_id

    @property
    def engine(self):
        return self.contents_manager.engine

    # Superclass method overrides.
    def make_dir(self, api_path):
        with self.engine.begin() as db:
            create_directory(db, self.user_id, api_path)

    def make_txt(self, api_path, txt):
        with self.engine.begin() as db:
            save_file(
                db,
                self.user_id,
                api_path,
                b64encode(txt.encode('utf-8')),
                UNLIMITED,
            )

    def make_blob(self, api_path, blob):
        with self.engine.begin() as db:
            save_file(db, self.user_id, api_path, b64encode(blob), UNLIMITED)

    def make_nb(self, api_path, nb):
        with self.engine.begin() as db:
            save_file(db, self.user_id, api_path, writes_base64(nb), UNLIMITED)

    # TODO: Use these rather than relying on `purge_db`.
    def delete_dir(self, api_path):
        raise NotImplementedError()

    def delete_file(self, api_path):
        raise NotImplementedError()

    def isfile(self, api_path):
        with self.engine.begin() as db:
            return file_exists(db, self.user_id, api_path)

    def isdir(self, api_path):
        with self.engine.begin() as db:
            return dir_exists(db, self.user_id, api_path)

    def setUp(self):
        self.contents_manager.purge_db()
        self.contents_manager.ensure_user()
        self.contents_manager.ensure_root_directory()
        super(PostgresContentsAPITest, self).setUp()

    def tearDown(self):
        self.contents_manager.purge_db()
    # End superclass method overrides.

    # Test overrides.
    def test_mkdir_hidden_400(self):
        """
        We don't support hidden directories.
        """
        pass

    def test_checkpoints_separate_root(self):
        pass


class PostgresCheckpointsAPITest(PGContentsAPITestBase):
    """
    Test using PostgresCheckpoints with the built-in
    PostgresContentsManager.
    """

    config = Config()
    config.FileContentsManager.checkpoints_class = PostgresCheckpoints
    config.PostgresCheckpoints.user_id = 'test'
    config.PostgresCheckpoints.db_url = TEST_DB_URL

    @property
    def checkpoints(self):
        return self.notebook.contents_manager.checkpoints

    def setUp(self):
        super(PostgresCheckpointsAPITest, self).setUp()
        self.checkpoints.purge_db()
        self.checkpoints.ensure_user()

    def tearDown(self):
        super(PostgresCheckpointsAPITest, self).tearDown()
        self.checkpoints.purge_db()

    def test_checkpoints_separate_root(self):
        pass


# This needs to be removed or else we'll run the main IPython tests as well.
del APITest
