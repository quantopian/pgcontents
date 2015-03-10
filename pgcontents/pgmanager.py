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
from __future__ import unicode_literals
from datetime import datetime
from itertools import chain

from IPython.nbformat import (
    from_dict,
)
from IPython.utils.traitlets import (
    Bool,
    Integer,
)
from IPython.html.services.contents.manager import ContentsManager
from tornado import web

from .api_utils import (
    to_api_path,
    writes_base64,
    reads_base64,
    from_b64,
    to_b64,
)
from .checkpoints import PostgresCheckpoints
from .constants import UNLIMITED
from .error import (
    DirectoryNotEmpty,
    FileExists,
    FileTooLarge,
    NoSuchDirectory,
    NoSuchFile,
)
from .managerbase import PostgresManagerMixin
from .query import (
    delete_file,
    delete_directory,
    dir_exists,
    ensure_directory,
    get_directory,
    get_file,
    purge_user,
    rename_file,
    save_file,
)

# We don't currently track created/modified dates for directories, so this
# value is always used instead.
DUMMY_CREATED_DATE = datetime.fromtimestamp(0)


class PostgresContentsManager(PostgresManagerMixin, ContentsManager):
    """
    ContentsManager that persists to a postgres database rather than to the
    local filesystem.
    """
    max_file_size_bytes = Integer(
        default_value=UNLIMITED,
        config=True,
        help="Maximum size in bytes of a file that will be saved.",
    )

    create_directory_on_startup = Bool(
        config=True,
        help="Create a user for user_id automatically?",
    )

    def _checkpoints_class_default(self):
        return PostgresCheckpoints

    def _checkpoints_kwargs_default(self):
        kw = super(PostgresContentsManager, self)._checkpoints_kwargs_default()
        kw.update(
            {
                'db_url': self.db_url,
                'user_id': self.user_id,
                'create_user_on_startup': self.create_user_on_startup,
            }
        )
        return kw

    def _create_directory_on_startup_default(self):
        return self.create_user_on_startup

    def __init__(self, *args, **kwargs):
        super(PostgresContentsManager, self).__init__(*args, **kwargs)
        if self.create_directory_on_startup:
            self.ensure_root_directory()

    def ensure_root_directory(self):
        with self.engine.begin() as db:
            ensure_directory(db, self.user_id, '')

    def purge_db(self):
        """
        Clear all matching our user_id.
        """
        with self.engine.begin() as db:
            purge_user(db, self.user_id)

    def _base_model(self, path):
        """
        Return model keys shared by all types.
        """
        return {
            "name": path.rsplit('/', 1)[-1],
            "path": path,
            "writable": True,
            "last_modified": None,
            "created": None,
            "content": None,
            "format": None,
            "mimetype": None,
        }

    def guess_type(self, path):
        """
        Guess the type of a file.
        """
        if path.endswith('.ipynb'):
            return 'notebook'
        elif self.dir_exists(path):
            return 'directory'
        else:
            return 'file'

    # Begin ContentsManager API.
    def dir_exists(self, path):
        with self.engine.begin() as db:
            return dir_exists(db, self.user_id, path)

    def is_hidden(self, path):
        return False

    def file_exists(self, path):
        with self.engine.begin() as db:
            try:
                get_file(db, self.user_id, path, include_content=False)
                return True
            except NoSuchFile:
                return False

    def get(self, path, content=True, type=None, format=None):
        if type is None:
            type = self.guess_type(path)
        try:
            return {
                'notebook': self._get_notebook,
                'directory': self._get_directory,
                'file': self._get_file,
            }[type](path=path, content=content, format=format)
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(type))

    def _get_notebook(self, path, content, format):
        """
        Get a notebook from the database.
        """
        with self.engine.begin() as db:
            try:
                record = get_file(db, self.user_id, path, content)
            except NoSuchFile:
                self.no_such_entity(path)

        return self._notebook_model_from_db(record, content)

    def _notebook_model_from_db(self, record, content):
        """
        Build a notebook model from database record.
        """
        path = to_api_path(record['parent_name'] + record['name'])
        model = self._base_model(path)
        model['type'] = 'notebook'
        model['last_modified'] = model['created'] = record['created_at']
        if content:
            content = reads_base64(record['content'])
            self.mark_trusted_cells(content, path)
            model['content'] = content
            model['format'] = 'json'
            self.validate_notebook_model(model)
        return model

    def _get_directory(self, path, content, format):
        """
        Get a directory from the database.
        """
        with self.engine.begin() as db:
            try:
                record = get_directory(
                    db, self.user_id, path, content
                )
            except NoSuchDirectory:
                if self.file_exists(path):
                    # TODO: It's awkward/expensive to have to check this to
                    # return a 400 instead of 404. Consider just 404ing.
                    self.do_400("Wrong type: %s" % path)
                else:
                    self.no_such_entity(path)

        return self._directory_model_from_db(record, content)

    def _convert_file_records(self, file_records):
        """
        Apply _notebook_model_from_db or _file_model_from_db to each entry
        in file_records, depending on the result of `guess_type`.
        """
        for record in file_records:
            type_ = self.guess_type(record['name'])
            if type_ == 'notebook':
                yield self._notebook_model_from_db(record, False)
            elif type_ == 'file':
                yield self._file_model_from_db(record, False, None)
            else:
                self.do_500("Unknown file type %s" % type_)

    def _directory_model_from_db(self, record, content):
        """
        Build a directory model from database directory record.
        """
        model = self._base_model(to_api_path(record['name']))
        model['type'] = 'directory'
        # TODO: Track directory modifications and fill in a real value for
        # this.
        model['last_modified'] = model['created'] = DUMMY_CREATED_DATE

        if content:
            model['format'] = 'json'
            model['content'] = list(
                chain(
                    self._convert_file_records(record['files']),
                    (
                        self._directory_model_from_db(subdir, False)
                        for subdir in record['subdirs']
                    ),
                )
            )
        return model

    def _file_model_from_db(self, record, content, format):
        """
        Build a file model from database record.
        """
        # TODO: Most of this is shared with _notebook_model_from_db.
        path = to_api_path(record['parent_name'] + record['name'])
        model = self._base_model(path)
        model['type'] = 'file'
        model['last_modified'] = model['created'] = record['created_at']
        if content:
            bcontent = record['content']
            model['content'], model['format'], model['mimetype'] = from_b64(
                path,
                bcontent,
                format,
            )
        return model

    def _get_file(self, path, content, format):
        with self.engine.begin() as db:
            try:
                record = get_file(db, self.user_id, path, content)
            except NoSuchFile:
                if self.dir_exists(path):
                    # TODO: It's awkward/expensive to have to check this to
                    # return a 400 instead of 404. Consider just 404ing.
                    self.do_400(u"Wrong type: %s" % path)
                else:
                    self.no_such_entity(path)
        return self._file_model_from_db(record, content, format)

    def _save_notebook(self, db, model, path):
        """
        Save a notebook.

        Returns a validation message.
        """
        nb_contents = from_dict(model['content'])
        self.check_and_sign(nb_contents, path)
        save_file(
            db,
            self.user_id,
            path,
            writes_base64(nb_contents),
            self.max_file_size_bytes,
        )
        # It's awkward that this writes to the model instead of returning.
        self.validate_notebook_model(model)
        return model.get('message')

    def _save_file(self, db, model, path):
        """
        Save a non-notebook file.
        """
        save_file(
            db,
            self.user_id,
            path,
            to_b64(model['content'], model.get('format', None)),
            self.max_file_size_bytes,
        )
        return None

    def _save_directory(self, db, path):
        """
        'Save' a directory.
        """
        ensure_directory(db, self.user_id, path)

    def save(self, model, path):
        if 'type' not in model:
            raise web.HTTPError(400, u'No file type provided')
        if 'content' not in model and model['type'] != 'directory':
            raise web.HTTPError(400, u'No file content provided')

        path = path.strip('/')

        # Almost all of this is duplicated with FileContentsManager :(.
        self.log.debug("Saving %s", path)
        if model['type'] not in ('file', 'directory', 'notebook'):
            self.do_400("Unhandled contents type: %s" % model['type'])
        try:
            with self.engine.begin() as db:
                if model['type'] == 'notebook':
                    validation_message = self._save_notebook(db, model, path)
                elif model['type'] == 'file':
                    validation_message = self._save_file(db, model, path)
                else:
                    validation_message = self._save_directory(db, path)
        except web.HTTPError:
            raise
        except FileTooLarge:
            self.file_too_large(path)
        except Exception as e:
            self.log.error(u'Error while saving file: %s %s',
                           path, e, exc_info=True)
            self.do_500(
                u'Unexpected error while saving file: %s %s' % (path, e)
            )

        # TODO: Consider not round-tripping to the database again here.
        model = self.get(path, type=model['type'], content=False)
        if validation_message is not None:
            model['message'] = validation_message
        return model

    def rename(self, old_path, path):
        """
        Rename a file.
        """
        with self.engine.begin() as db:
            try:
                rename_file(db, self.user_id, old_path, path)
            except FileExists:
                self.already_exists(path)

    def _delete_file(self, path):
        with self.engine.begin() as db:
            deleted_count = delete_file(db, self.user_id, path)
            if not deleted_count:
                self.no_such_entity(path)

    def _delete_directory(self, path):
        with self.engine.begin() as db:
            try:
                deleted_count = delete_directory(db, self.user_id, path)
            except DirectoryNotEmpty:
                self.not_empty(path)
            if not deleted_count:
                self.no_such_entity(path)

    def delete(self, path):
        """
        Delete file at path.
        """
        if self.file_exists(path):
            self._delete_file(path)
        elif self.dir_exists(path):
            self._delete_directory(path)
        else:
            self.no_such_entity(path)
