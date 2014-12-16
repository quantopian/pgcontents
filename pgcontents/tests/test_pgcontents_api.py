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
from base64 import (
    b64encode,
)
from IPython.config import Config
from IPython.html.services.contents.tests.test_contents_api import APITest

from ..pgmanager import (
    writes_base64,
)
from ..schema import (
    create_directory,
    dir_exists,
    notebook_exists,
    save_notebook,
)


class PGContentsAPITest(APITest):

    config = Config()
    config.NotebookApp.contents_manager_class = \
        'pgcontents.pgmanager.PostgresContentsManager'
    config.PostgresContentsManager.user_id = 'test'

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
            save_notebook(
                db,
                self.user_id,
                api_path,
                b64encode(txt.encode('utf-8')),
            )

    def make_blob(self, api_path, blob):
        with self.engine.begin() as db:
            save_notebook(db, self.user_id, api_path, b64encode(blob))

    def make_nb(self, api_path, nb):
        with self.engine.begin() as db:
            save_notebook(db, self.user_id, api_path, writes_base64(nb))

    # TODO: Use these rather than relying on `purge`.
    def delete_dir(self, api_path):
        raise NotImplementedError()

    def delete_file(self, api_path):
        raise NotImplementedError()

    def isfile(self, api_path):
        with self.engine.begin() as db:
            return notebook_exists(db, self.user_id, api_path)

    def isdir(self, api_path):
        with self.engine.begin() as db:
            return dir_exists(db, self.user_id, api_path)

    def setUp(self):
        self.contents_manager.purge()
        self.contents_manager.ensure_user()
        super(PGContentsAPITest, self).setUp()

    def tearDown(self):
        self.contents_manager.purge()
    # End superclass method overrides.

del APITest
