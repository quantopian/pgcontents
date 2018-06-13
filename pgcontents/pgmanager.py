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
PostgreSQL implementation of IPython/Jupyter ContentsManager API.
"""
from __future__ import unicode_literals
from itertools import chain
from tornado import web

from .api_utils import (
    base_model,
    base_directory_model,
    from_b64,
    outside_root_to_404,
    reads_base64,
    to_api_path,
    to_b64,
    writes_base64,
)
from .checkpoints import PostgresCheckpoints
from .error import (
    CorruptedFile,
    DirectoryExists,
    DirectoryNotEmpty,
    FileExists,
    FileTooLarge,
    NoSuchDirectory,
    NoSuchFile,
    PathOutsideRoot,
    RenameRoot,
)
from .managerbase import PostgresManagerMixin
from .query import (
    delete_directory,
    delete_file,
    dir_exists,
    ensure_directory,
    file_exists,
    get_directory,
    get_file,
    get_file_id,
    purge_user,
    rename_directory,
    rename_file,
    save_file,
)
from .utils.ipycompat import Bool, ContentsManager, from_dict


class PostgresContentsManager(PostgresManagerMixin, ContentsManager):
    """
    ContentsManager that persists to a postgres database rather than to the
    local filesystem.
    """
    create_directory_on_startup = Bool(
        config=True,
        help="Create a root directory automatically?",
    )

    def _checkpoints_class_default(self):
        return PostgresCheckpoints

    def _checkpoints_kwargs_default(self):
        try:
            klass = PostgresContentsManager
            kw = super(klass, self)._checkpoints_kwargs_default()
        except AttributeError:
            kw = {'parent': self, 'log': self.log}
        kw.update({
            'create_user_on_startup': self.create_user_on_startup,
            'crypto': self.crypto,
            'db_url': self.db_url,
            'max_file_size_bytes': self.max_file_size_bytes,
            'user_id': self.user_id,
        })
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

    @outside_root_to_404
    def guess_type(self, path, allow_directory=True):
        """
        Guess the type of a file.

        If allow_directory is False, don't consider the possibility that the
        file is a directory.
        """
        if path.endswith('.ipynb'):
            return 'notebook'
        elif allow_directory and self.dir_exists(path):
            return 'directory'
        else:
            return 'file'

    # Begin ContentsManager API.
    @outside_root_to_404
    def dir_exists(self, path):
        with self.engine.begin() as db:
            return dir_exists(db, self.user_id, path)

    def is_hidden(self, path):
        return False

    @outside_root_to_404
    def file_exists(self, path):
        with self.engine.begin() as db:
            return file_exists(db, self.user_id, path)

    @outside_root_to_404
    def get(self, path, content=True, type=None, format=None):
        if type is None:
            type = self.guess_type(path)
        try:
            fn = {
                'notebook': self._get_notebook,
                'directory': self._get_directory,
                'file': self._get_file,
            }[type]
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(type))

        try:
            return fn(path=path, content=content, format=format)
        except CorruptedFile as e:
            self.log.error(
                u'Corrupted file encountered at path %r. %s',
                path, e, exc_info=True,
            )
            self.do_500("Unable to read stored content at path %r." % path)

    @outside_root_to_404
    def get_file_id(self, path):
        """
        Get the id of a file in the database.  This function is specific to
        this implementation of ContentsManager and is not in the base class.
        """
        with self.engine.begin() as db:
            try:
                file_id = get_file_id(db, self.user_id, path)
            except NoSuchFile:
                self.no_such_entity(path)

        return file_id

    def _get_notebook(self, path, content, format):
        """
        Get a notebook from the database.
        """
        with self.engine.begin() as db:
            try:
                record = get_file(
                    db,
                    self.user_id,
                    path,
                    content,
                    self.crypto.decrypt,
                )
            except NoSuchFile:
                self.no_such_entity(path)

        return self._notebook_model_from_db(record, content)

    def _notebook_model_from_db(self, record, content):
        """
        Build a notebook model from database record.
        """
        path = to_api_path(record['parent_name'] + record['name'])
        model = base_model(path)
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
            type_ = self.guess_type(record['name'], allow_directory=False)
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
        model = base_directory_model(to_api_path(record['name']))
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
        model = base_model(path)
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
                record = get_file(
                    db,
                    self.user_id,
                    path,
                    content,
                    self.crypto.decrypt,
                )
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
            self.crypto.encrypt,
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
            self.crypto.encrypt,
            self.max_file_size_bytes,
        )
        return None

    def _save_directory(self, db, path):
        """
        'Save' a directory.
        """
        ensure_directory(db, self.user_id, path)

    @outside_root_to_404
    def save(self, model, path):
        if 'type' not in model:
            raise web.HTTPError(400, u'No model type provided')
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
        except (web.HTTPError, PathOutsideRoot):
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

    @outside_root_to_404
    def rename_file(self, old_path, path):
        """
        Rename object from old_path to path.

        NOTE: This method is unfortunately named on the base class.  It
        actually moves a file or a directory.
        """
        with self.engine.begin() as db:
            try:
                if self.file_exists(old_path):
                    rename_file(db, self.user_id, old_path, path)
                elif self.dir_exists(old_path):
                    rename_directory(db, self.user_id, old_path, path)
                else:
                    self.no_such_entity(path)
            except (FileExists, DirectoryExists):
                self.already_exists(path)
            except RenameRoot as e:
                self.do_409(str(e))

    def _delete_non_directory(self, path):
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

    @outside_root_to_404
    def delete_file(self, path):
        """
        Delete object corresponding to path.
        """
        if self.file_exists(path):
            self._delete_non_directory(path)
        elif self.dir_exists(path):
            self._delete_directory(path)
        else:
            self.no_such_entity(path)
