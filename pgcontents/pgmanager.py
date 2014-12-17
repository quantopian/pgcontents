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
from datetime import datetime
from getpass import getuser
from itertools import chain

import mimetypes

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

from six import text_type
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.engine.base import Engine
from tornado import web

from .error import (
    DirectoryNotEmpty,
    FileExists,
    NoSuchDirectory,
    NoSuchFile,
)
from .schema import (
    all_checkpoints,
    delete_file,
    delete_directory,
    dir_exists,
    ensure_db_user,
    ensure_directory,
    get_directory,
    get_file,
    current_checkpoint,
    purge_user,
    rename_file,
    restore_checkpoint,
    save_file,
    to_api_path,
)

# We don't currently track created/modified dates for directories, so this
# value is always used instead.
DUMMY_CREATED_DATE = datetime.fromtimestamp(0)
NBFORMAT_VERSION = 4


def writes_base64(nb, version=NBFORMAT_VERSION):
    """
    Write a notebook as base64.
    """
    return b64encode(writes(nb, version=version).encode('utf-8'))


def reads_base64(nb, as_version=NBFORMAT_VERSION):
    """
    Read a notebook from base64.
    """
    return reads(b64decode(nb).decode('utf-8'), as_version=as_version)


def _decode_text_from_base64(path, bcontent):
    content = b64decode(bcontent)
    try:
        return (content.decode('utf-8'), 'text')
    except UnicodeError:
        raise web.HTTPError(
            400,
            "%s is not UTF-8 encoded" % path, reason='bad format'
        )


def _decode_unknown_from_base64(path, bcontent):
    content = b64decode(bcontent)
    try:
        return (content.decode('utf-8'), 'text')
    except UnicodeError:
        pass
    return bcontent.decode('ascii'), 'base64'


def from_b64(path, bcontent, format):
    """
    Decode base64 content for a file.

    format:
      If 'text', the contents will be decoded as UTF-8.
      If 'base64', do nothing.
      If not specified, try to decode as UTF-8, and fall back to base64

    Returns a triple of decoded_content, format, and mimetype.
    """
    decoders = {
        'base64': lambda path, bcontent: (bcontent.decode('ascii'), 'base64'),
        'text': _decode_text_from_base64,
        None: _decode_unknown_from_base64,
    }
    content, real_format = decoders[format](path, bcontent)

    default_mimes = {
        'text': 'text/plain',
        'base64': 'application/octet-stream',
    }
    mimetype = mimetypes.guess_type(path)[0] or default_mimes[real_format]

    return content, real_format, mimetype


class PostgresContentsManager(ContentsManager):
    """
    ContentsManager that persists to a postgres database rather than to the
    local filesystem.
    """
    db_url = Unicode(
        default_value="postgresql://{user}@/pgcontents".format(
            user=getuser(),
        ),
        config=True,
        help="Connection string for the database.",
    )

    user_id = Unicode(
        default_value=getuser(),
        config=True,
        help="Username for the server we're managing.",
    )

    engine = Instance(Engine)

    def _engine_default(self):
        return create_engine(self.db_url)

    def __init__(self, *args, **kwargs):
        super(PostgresContentsManager, self).__init__(*args, **kwargs)
        self.ensure_user()

    def ensure_user(self):
        with self.engine.begin() as db:
            ensure_db_user(db, self.user_id)

        with self.engine.begin() as db:
            ensure_directory(db, self.user_id, '')

    def purge(self):
        """
        Clear all matching our user_id.
        """
        with self.engine.begin() as db:
            purge_user(db, self.user_id)

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

    def get(self, path, content=True, type=None, format=None):
        if type is None:
            type = self.guess_type(path)
        if type == "notebook":
            return self._get_notebook(path, content, format)
        elif type == "directory":
            return self._get_directory(path, content, format)
        elif type == "file":
            return self._get_file(path, content, format)
        else:
            raise ValueError("Unknown type passed: {}".format(type))

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

    def _save_notebook(self, db, model, path):
        """
        Save a notebook.

        Returns a validation message.
        """
        nb_contents = from_dict(model['content'])
        self.check_and_sign(nb_contents, path)
        save_file(db, self.user_id, path, writes_base64(nb_contents))
        # It's awkward that this writes to the model instead of returning.
        self.validate_notebook_model(model)
        return model.get('message')

    def _save_file(self, db, model, path):
        """
        Save a non-notebook file.
        """
        fmt = model.get('format', None)
        allowed_formats = {'text', 'base64'}
        if fmt not in allowed_formats:
            self.do_400(
                "Expected file contents in {allowed}, got {fmt}".format(
                    allowed=allowed_formats,
                    fmt=fmt,
                )
            )
        if fmt == 'text':
            bcontent = b64encode(model['content'].encode('utf8'))
        else:
            bcontent = model['content'].encode('ascii')

        save_file(db, self.user_id, path, bcontent)
        return None

    def _save_directory(self, db, path):
        """
        'Save' a directory.
        """
        ensure_directory(db, self.user_id, path)

    def update(self, model, path):
        """
        Update the file's path

        For use in PATCH requests, to enable renaming a file without
        re-uploading its contents. Only used for renaming at the moment.
        """
        # NOTE: This implementation is identical to the one in
        # FileContentsManager.
        new_path = model.get('path', path)
        if path != new_path:
            self.rename(path, new_path)
        model = self.get(new_path, content=False)
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

    def create_checkpoint(self, path):
        """
        We create 'checkpoints' automatically on every save, so just return the
        most recent checkpoint id.
        """
        with self.engine.begin() as db:
            try:
                record = current_checkpoint(db, self.user_id, path)
            except NoSuchFile:
                self.no_such_entity(path)
        return self._checkpoint_from_record(record)

    def _checkpoint_from_record(self, record):
        """
        Convert a database record to a checkpoint.
        """
        return {
            'id': text_type(record['id']),
            'last_modified': record['created_at'],
        }

    def list_checkpoints(self, path):
        with self.engine.begin() as db:
            try:
                records = all_checkpoints(db, self.user_id, path)
            except NoSuchFile:
                self.no_such_entity(path)

        return [self._checkpoint_from_record(record) for record in records]

    def restore_checkpoint(self, checkpoint_id, path):
        with self.engine.begin() as db:
            try:
                restore_checkpoint(db, self.user_id, checkpoint_id, path)
            except NoSuchFile:
                self.no_such_entity(
                    "Path: {path}, Checkpoint: {checkpoint_id}".format(
                        path=path, checkpoint_id=checkpoint_id,
                    )
                )

    def delete_checkpoint(self, checkpoint_id, path):
        raise NotImplementedError()
    # End ContentsManager API.

    def no_such_entity(self, path):
        self.do_404(
            u"No such entity: [{path}]".format(path=path)
        )

    def not_empty(self, path):
        self.do_400(
            u"Directory not empty: [{path}]".format(path=path)
        )

    def already_exists(self, path):
        self.do_409(u"File already exists: [{path}]".format(path=path))

    def do_400(self, msg):
        raise web.HTTPError(400, msg)

    def do_404(self, msg):
        raise web.HTTPError(404, msg)

    def do_409(self, msg):
        raise web.HTTPError(409, msg)

    def do_500(self, msg):
        raise web.HTTPError(500, msg)
