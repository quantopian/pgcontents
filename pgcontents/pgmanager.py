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
PostgreSQL implementation of IPython ContentsManager API.
"""

from base64 import (
    b64decode,
    b64encode,
)

from IPython.nbformat import (
    from_dict,
    reads,
    writes,
)
from IPython.utils.traitlets import (
    Instance,
    Unicode,
)
from IPython.html.services.contents.manager import ContentsManager

from sqlalchemy import (
    and_,
    create_engine,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError

from schema import (
    adduser_idempotent,
    dir_exists,
    ensure_root_dir,
    get_notebook,
    listdir,
    notebooks,
    save_notebook,
)


NBFORMAT_VERSION = 4


def writes_base64(nb, version=NBFORMAT_VERSION):
    """
    Write a notebook as base64.
    """
    return b64encode(writes(nb, version=version))


def reads_base64(nb, as_version=NBFORMAT_VERSION):
    """
    Read a notebook from base64.
    """
    return reads(b64decode(nb), as_version=as_version)


class PostgresContentsManager(ContentsManager):
    """
    ContentsManager that persists to a postgres database rather than to the
    local filesystem.
    """
    db_url = Unicode(
        default_value="postgresql://ssanderson@/pgcontents",
        help="Connection string for the database.",
    )

    user_id = Unicode(
        default_value="ssanderson",
        help="Username for the server we're managing."
    )

    engine = Instance(Engine)
    def _engine_default(self):
        return create_engine(self.db_url)

    def __init__(self, *args, **kwargs):
        super(PostgresContentsManager, self).__init__(*args, **kwargs)
        self.ensure_user()

    def ensure_user(self):
        with self.engine.begin() as db:
            adduser_idempotent(db, self.user_id)

        with self.engine.begin() as db:
            ensure_root_dir(db, self.user_id)

    # Begin ContentsManager API.
    def dir_exists(self, path):
        with self.engine.begin() as db:
            return dir_exists(db, path, self.user_id)

    def is_hidden(self, path):
        return False

    def file_exists(self, path):
        with self.engine.begin() as db:
            return get_notebook(db, self.user_id, path, include_content=False)

    def _base_model(self, path):
        """
        Return model keys shared by all types.
        """
        return  {
            "name": path.rsplit('/', 1)[-1],
            "path": path,
            "writable": True,
            "last_modified": None,
            "created": None,
            "content": None,
            "format": None,
            "mimetype": None,
        }

    def get(self, path, content=True, type_=None, format=None):
        if type_ == "notebook":
            return self._get_notebook(path, content, format)
        elif type_ == "directory":
            return self._get_directory(path, content, format)
        elif type_ == "file":
            return self._get_file(path, content, format)
        else:
            raise ValueError("Unknown type passed: {}".format(type_))

    def _get_notebook(self, path, content, format):
        model = self._base_model(path)
        model['type'] = 'notebook'
        with self.engine.begin() as db:
            nb = get_notebook(db, self.user_id, path, content)

        if content:
            content = reads_base64(nb['content'])
            self.mark_trusted_cells(content, path)
            model['content'] = nb['content']
            model['format'] = 'json'
            model['last_modified'] = model['created'] = nb['created_at']
            self.validate_notebook_model(model)
        return model

    def _get_directory(self, path, content, format):
        pass

    def _get_file(self, path, content, format):
        raise NotImplementedError()

    def save(self, model, path):
        if model['type'] != 'notebook':
            raise ValueError("Only notebooks can be saved.")

        self.validate_notebook_model(model)
        nb_contents = from_dict(model['content'])
        self.check_and_sign(nb_contents, path)

        with self.engine.begin() as db:
            save_notebook(db, self.user_id, path, writes_base64(nb_contents))

    def update(self, model, path):
        pass

    def delete(self, path):
        pass

    def create_checkpoint(self, path):
        pass

    def list_checkpoints(self, path):
        pass

    def restore_checkpoint(self, checkpoint_id, path):
        pass
    # End ContentsManager API.

    def query_notebook(self, path):
        return and_(
            notebooks.c.user_id == self.user_id,
            notebooks.c.path == path,
        )
