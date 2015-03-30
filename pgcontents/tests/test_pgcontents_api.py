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
from six import iteritems

from IPython.config import Config
from IPython.html.services.contents.filemanager import FileContentsManager
from IPython.html.services.contents.filecheckpoints import \
    GenericFileCheckpoints
from IPython.html.services.contents.tests.test_contents_api import APITest
from IPython.html.utils import to_os_path
from IPython.utils.tempdir import TemporaryDirectory
from requests import HTTPError

from ..constants import UNLIMITED
from ..hybridmanager import HybridContentsManager
from ..pgmanager import (
    PostgresContentsManager,
    writes_base64,
)
from ..checkpoints import PostgresCheckpoints
from ..query import (
    create_directory,
    delete_directory,
    delete_file,
    dir_exists,
    file_exists,
    save_file,
)
from .utils import (
    _norm_unicode,
    drop_testing_db_tables,
    migrate_testing_db,
    TEST_DB_URL,
)
from ..utils.sync import walk, walk_dirs


class _APITestBase(APITest):
    """
    APITest that also runs a test for our implementation of `walk`.
    """

    def test_walk(self):
        """
        Test ContentsManager.walk.
        """
        results = {
            _norm_unicode(dname): (subdirs, files)
            for dname, subdirs, files in walk(self.notebook.contents_manager)
        }
        # This is a dictionary because the ordering of these is all messed up
        # on OSX.
        expected = {
            '': (
                [
                    'Directory with spaces in',
                    'foo',
                    'ordering',
                    u'unicodé',
                    u'å b',
                ],
                ['inroot.blob', 'inroot.ipynb', 'inroot.txt'],
            ),
            'Directory with spaces in': (
                [],
                ['inspace.blob', 'inspace.ipynb', 'inspace.txt'],
            ),
            'foo': (
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
            'foo/bar': (
                [],
                ['baz.blob', 'baz.ipynb', 'baz.txt'],
            ),
            'ordering': (
                [],
                [
                    'A.blob', 'A.ipynb', 'A.txt',
                    'C.blob', 'C.ipynb', 'C.txt',
                    'b.blob', 'b.ipynb', 'b.txt',
                ],
            ),
            u'unicodé': (
                [],
                ['innonascii.blob', 'innonascii.ipynb', 'innonascii.txt'],
            ),
            u'å b': (
                [],
                [u'ç d.blob', u'ç d.ipynb', u'ç d.txt'],
            ),
        }

        for dname, (subdirs, files) in iteritems(expected):
            result_subdirs, result_files = results.pop(_norm_unicode(dname))
            if dname == '':
                sep = ''
            else:
                sep = '/'
            self.assertEqual(
                set(
                    map(
                        _norm_unicode,
                        [sep.join([dname, sub]) for sub in subdirs]
                    )
                ),
                set(map(_norm_unicode, result_subdirs)),
            )
            self.assertEqual(
                set(
                    map(
                        _norm_unicode,
                        [sep.join([dname, fname]) for fname in files]
                    ),
                ),
                set(map(_norm_unicode, result_files)),
            )
        self.assertEqual(results, {})

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
                reverse=True,
            )
        )


class PostgresContentsAPITest(_APITestBase):

    config = Config()
    config.NotebookApp.contents_manager_class = PostgresContentsManager
    config.PostgresContentsManager.user_id = 'test'
    config.PostgresContentsManager.db_url = TEST_DB_URL

    # Don't support hidden directories.
    hidden_dirs = []

    @classmethod
    def setup_class(cls):
        drop_testing_db_tables()
        migrate_testing_db()
        super(PostgresContentsAPITest, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        drop_testing_db_tables()
        super(PostgresContentsAPITest, cls).teardown_class()

    @property
    def pg_manager(self):
        return self.notebook.contents_manager

    @property
    def user_id(self):
        return self.pg_manager.user_id

    @property
    def engine(self):
        return self.pg_manager.engine

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

    def delete_dir(self, api_path, db=None):
        if self.isdir(api_path):
            dirs, files = [], []
            for dir_, _, fs in walk_dirs(self.pg_manager, [api_path]):
                dirs.append(dir_)
                files.extend(fs)

            with self.engine.begin() as db:
                for file_ in files:
                    delete_file(db, self.user_id, file_)
                for dir_ in reversed(dirs):
                    delete_directory(db, self.user_id, dir_)

    def delete_file(self, api_path):
        if self.isfile(api_path):
            with self.engine.begin() as db:
                delete_file(db, self.user_id, api_path)

    def isfile(self, api_path):
        with self.engine.begin() as db:
            return file_exists(db, self.user_id, api_path)

    def isdir(self, api_path):
        with self.engine.begin() as db:
            return dir_exists(db, self.user_id, api_path)

    # End superclass method overrides.

    # Test overrides.
    def test_mkdir_hidden_400(self):
        """
        We don't support hidden directories.
        """
        pass

    def test_checkpoints_separate_root(self):
        pass


class PostgresContentsFileCheckpointsAPITest(PostgresContentsAPITest):

    config = Config()
    config.NotebookApp.contents_manager_class = PostgresContentsManager
    config.PostgresContentsManager.checkpoints_class = GenericFileCheckpoints
    config.PostgresContentsManager.user_id = 'test'
    config.PostgresContentsManager.db_url = TEST_DB_URL

    # Don't support hidden directories.
    hidden_dirs = []

    @classmethod
    def setup_class(cls):
        drop_testing_db_tables()
        migrate_testing_db()
        cls.td = TemporaryDirectory()
        cls.config.GenericFileCheckpoints.root_dir = cls.td.name
        super(PostgresContentsFileCheckpointsAPITest, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        super(PostgresContentsFileCheckpointsAPITest, cls).teardown_class()
        cls.td.cleanup()
        drop_testing_db_tables()


class PostgresCheckpointsAPITest(_APITestBase):
    """
    Test using PostgresCheckpoints with the built-in FileContentsManager.
    """

    config = Config()
    config.NotebookApp.contents_manager_class = FileContentsManager
    config.ContentsManager.checkpoints_class = PostgresCheckpoints
    config.PostgresCheckpoints.user_id = 'test'
    config.PostgresCheckpoints.db_url = TEST_DB_URL

    @property
    def checkpoints(self):
        return self.notebook.contents_manager.checkpoints

    @classmethod
    def setup_class(cls):
        drop_testing_db_tables()
        migrate_testing_db()
        super(PostgresCheckpointsAPITest, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        super(PostgresCheckpointsAPITest, cls).teardown_class()
        drop_testing_db_tables()

    def setUp(self):
        super(PostgresCheckpointsAPITest, self).setUp()
        self.checkpoints.purge_db()
        self.checkpoints.ensure_user()

    def tearDown(self):
        super(PostgresCheckpointsAPITest, self).tearDown()
        self.checkpoints.purge_db()

    def test_pgcheckpoints_is_used(self):
        self.assertIsInstance(self.checkpoints, PostgresCheckpoints)

    def test_checkpoints_separate_root(self):
        pass


class HybridContentsPGRootAPITest(PostgresContentsAPITest):
    """
    Test using a HybridContentsManager splitting between files and Postgres.
    """
    files_prefix = 'foo'
    files_test_cls = APITest

    @classmethod
    def setup_class(cls):

        drop_testing_db_tables()
        migrate_testing_db()
        cls.td = TemporaryDirectory()

        cls.config = Config()
        cls.config.NotebookApp.contents_manager_class = HybridContentsManager
        cls.config.HybridContentsManager.manager_classes = {
            '': PostgresContentsManager,
            cls.files_prefix: FileContentsManager,
        }
        cls.config.HybridContentsManager.manager_kwargs = {
            '': {'user_id': 'test', 'db_url': TEST_DB_URL},
            cls.files_prefix: {'root_dir': cls.td.name},
        }
        super(HybridContentsPGRootAPITest, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        super(HybridContentsPGRootAPITest, cls).teardown_class()
        drop_testing_db_tables()

    @property
    def pg_manager(self):
        return self.notebook.contents_manager.root_manager

    def to_os_path(self, api_path):
        return to_os_path(api_path, root=self.td.name)

    # Autogenerate setup methods by dispatching on api_path.
    def __api_path_dispatch(method_name):
        """
        For a given method name, create a method which either uses the
        PostgresContentsAPITest implementation of that method name, or the base
        APITest implementation, depending on whether the given path starts with
        self.files_prefix.
        """
        def _method(self, api_path, *args):
            parts = api_path.strip('/').split('/')
            if parts[0] == self.files_prefix:
                # Dispatch to filesystem.
                return getattr(self.files_test_cls, method_name)(
                    self, '/'.join(parts[1:]), *args
                )
            else:
                # Dispatch to Postgres.
                return getattr(PostgresContentsAPITest, method_name)(
                    self, api_path, *args
                )
        return _method

    __methods_to_multiplex = [
        'make_txt',
        'make_blob',
        'make_dir',
        'make_nb',
        'delete_dir',
        'delete_file',
        'isfile',
        'isdir',
    ]
    l = locals()
    for method_name in __methods_to_multiplex:
        l[method_name] = __api_path_dispatch(method_name)
    del __methods_to_multiplex
    del __api_path_dispatch

    # Override to not delete the root of the file subsystem.
    def test_delete_dirs(self):
        # depth-first delete everything, so we don't try to delete empty
        # directories
        for name in sorted(self.dirs + ['/'], key=len, reverse=True):
            listing = self.api.list(name).json()['content']
            for model in listing:
                # Expect delete to fail on root of file subsystem.
                if model['path'] == self.files_prefix:
                    with self.assertRaises(HTTPError) as err:
                        self.api.delete(model['path'])
                    self.assertEqual(err.exception.response.status_code, 400)
                else:
                    self.api.delete(model['path'])

        listing = self.api.list('/').json()['content']
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0]['path'], self.files_prefix)


# This needs to be removed or else we'll run the main IPython tests as well.
del APITest
