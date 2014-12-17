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

from __future__ import unicode_literals
from itertools import izip
from textwrap import dedent

from psycopg2.errorcodes import FOREIGN_KEY_VIOLATION
from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Unicode,
    and_,
    desc,
    func,
    null,
    select,
)
from sqlalchemy.exc import IntegrityError

from db_utils import ignore_unique_violation
from .error import (
    DirectoryNotEmpty,
    FileExists,
    NoSuchDirectory,
    NoSuchFile,
)

metadata = MetaData()

# Shared Types
UserID = Unicode(30)
DirectoryName = Unicode(70)

users = Table(
    'users',
    metadata,
    Column('id', UserID, primary_key=True),
)

"""
We need to be able to query:
1. Does a directory exist?
2. Does a file exists?
3. What are the contents of a directory.  This must include both files **and**
other directories.

Having just directory_name and suffix on files doesn't work because there are
no entities that represent just directories themselves, which means there's no
way to determine if a directory is a child of another directory.
"""
directories = Table(
    'directories',
    metadata,
    # ======= #
    # Columns #
    # ======= #
    Column(
        'user_id',
        UserID,
        ForeignKey(users.c.id),
        nullable=False,
        primary_key=True
    ),
    Column('name', DirectoryName, nullable=False, primary_key=True),
    Column('parent_user_id', UserID, nullable=True),
    Column('parent_name', DirectoryName, nullable=True),

    # =========== #
    # Constraints #
    # =========== #
    ForeignKeyConstraint(
        ['parent_user_id', 'parent_name'],
        ['directories.user_id', 'directories.name'],
    ),
    CheckConstraint(
        'user_id = parent_user_id',
        name='directories_match_user_id',
    ),
    # Assert that parent_name is a prefix of name.
    CheckConstraint(
        "position(parent_name in name) != 0",
        name='directories_parent_name_prefix',
    ),
    # Assert that all directories begin or end with '/'.
    CheckConstraint(
        "left(name, 1) = '/'",
        name='directories_startwith_slash',
    ),
    CheckConstraint(
        "right(name, 1) = '/'",
        name='directories_endwith_slash',
    ),
    # Assert that the name of this directory has one more '/' than its parent.
    CheckConstraint(
        "length(regexp_replace(name, '[^/]+', '', 'g')) - 1"
        "= length(regexp_replace(parent_name, '[^/]+', '', 'g'))",
        name='directories_slash_count',
    ),
    # Assert that parent_user_id is NULL iff parent_name is NULL.  This should
    # be true only for each user's root directory.
    CheckConstraint(
        ''.join(
            [
                '(parent_name IS NULL AND parent_user_id IS NULL)'
                ' OR ',
                '(parent_name IS NOT NULL AND parent_user_id IS NOT NULL)'
            ],
        ),
        name='directories_null_user_id_match',
    ),
)


files = Table(
    'files',
    metadata,
    Column('id', Integer(), nullable=False, primary_key=True),
    Column('name', Unicode(40), nullable=False),
    Column(
        'user_id',
        UserID,
        ForeignKey(users.c.id),
        nullable=False,
    ),
    Column('parent_name', DirectoryName, nullable=False),
    Column('content', LargeBinary(100000), nullable=False),
    Column(
        'created_at',
        DateTime,
        default=func.now(),
        nullable=False,
    ),
    ForeignKeyConstraint(
        ['user_id', 'parent_name'],
        ['directories.user_id', 'directories.name'],
    ),
)


def from_api_dirname(api_dirname):
    """
    Convert API-style directory name into the format stored in the database.

    TODO: Implement this with a SQLAlchemy TypeDecorator.
    """
    # Special case for root directory.
    if api_dirname == '':
        return '/'
    return ''.join(
        [
            '' if api_dirname.startswith('/') else '/',
            api_dirname,
            '' if api_dirname.endswith('/') else '/',
        ]
    )


def to_api_path(db_path):
    """
    Convert database path into API-style path.

    TODO: Implement this with a SQLAlchemy TypeDecorator.
    """
    return db_path.strip('/')


def split_api_filepath(path):
    """
    Split an API file path into directory and name.
    """
    parts = path.rsplit('/', 1)
    if len(parts) == 1:
        name = parts[0]
        dirname = '/'
    else:
        name = parts[1]
        dirname = parts[0] + '/'

    return from_api_dirname(dirname), name


def ensure_db_user(db, user_id):
    """
    Add a new user if they don't already exist.
    """
    with ignore_unique_violation():
        db.execute(
            users.insert().values(id=user_id),
        )


def purge_user(db, user_id):
    """
    Delete a user and all of their resources.
    """
    db.execute(files.delete().where(
        files.c.user_id == user_id
    ))
    db.execute(directories.delete().where(
        directories.c.user_id == user_id
    ))
    db.execute(users.delete().where(
        users.c.id == user_id
    ))


def ensure_directory(db, user_id, api_path):
    """
    Ensure that the given user has the given directory.
    """
    with ignore_unique_violation():
        create_directory(db, user_id, api_path)


def create_directory(db, user_id, api_path):
    """
    Create a directory.
    """
    name = from_api_dirname(api_path)
    if name == '/':
        parent_name = null()
        parent_user_id = null()
    else:
        # Convert '/foo/bar/buzz/' -> '/foo/bar/'
        parent_name = name[:name.rindex('/', 0, -1) + 1]
        parent_user_id = user_id

    db.execute(
        directories.insert().values(
            name=name,
            user_id=user_id,
            parent_name=parent_name,
            parent_user_id=parent_user_id,
        )
    )


def to_dict(fields, row):
    """
    Convert a SQLAlchemy row to a dict.

    If row is None, return None.
    """
    assert(len(fields) == len(row))
    return {
        field.name: value
        for field, value in izip(fields, row)
    }


def _file_where(user_id, api_path):
    """
    Return a WHERE clause matching the given API path and user_id.
    """
    directory, name = split_api_filepath(api_path)
    return and_(
        files.c.name == name,
        files.c.user_id == user_id,
        files.c.parent_name == directory,
    )


def _is_in_directory(table, user_id, db_dirname):
    """
    Return a WHERE clause that matches entries in a directory.

    Parameterized on table because this clause is re-used between files and
    directories.
    """
    return and_(
        table.c.parent_name == db_dirname,
        table.c.user_id == user_id,
    )


def _file_default_fields():
    """
    Default fields returned by a file query.
    """
    return [
        files.c.name,
        files.c.created_at,
        files.c.parent_name,
    ]


def _directory_default_fields():
    """
    Default fields returned by a directory query.
    """
    return [
        directories.c.name,
    ]


def get_file(db, user_id, api_path, include_content):
    """
    Get file data for the given user_id and path.

    Include content only if include_content=True.
    """
    query_fields = _file_default_fields()
    if include_content:
        query_fields.append(files.c.content)

    result = db.execute(
        select(query_fields).where(
            _file_where(user_id, api_path),
        ).order_by(
            desc(files.c.created_at)
        ).limit(
            1
        )
    ).first()

    if result is None:
        raise NoSuchFile(api_path)
    return to_dict(query_fields, result)


def delete_file(db, user_id, api_path):
    """
    Delete a file.

    TODO: Consider making this a soft delete.
    """
    directory, name = split_api_filepath(api_path)
    result = db.execute(
        files.delete().where(
            _file_where(user_id, api_path)
        )
    )

    rowcount = result.rowcount
    if not rowcount:
        raise NoSuchFile(api_path)

    # TODO: This is misleading because we allow multiple files with the same
    # user_id/name as checkpoints.  Consider de-duping this in some way?
    return rowcount


def delete_directory(db, user_id, api_path):
    """
    Delete a directory.

    TODO: Consider making this a soft delete.
    """
    db_dirname = from_api_dirname(api_path)
    try:
        result = db.execute(
            directories.delete().where(
                and_(
                    directories.c.user_id == user_id,
                    directories.c.name == db_dirname,
                )
            )
        )
    except IntegrityError as error:
        if error.orig.pgcode != FOREIGN_KEY_VIOLATION:
            raise
        raise DirectoryNotEmpty(api_path)

    rowcount = result.rowcount
    if not rowcount:
        raise NoSuchDirectory(api_path)

    return rowcount


def file_exists(db, user_id, path):
    """
    Check if a file exists.
    """
    try:
        get_file(db, user_id, path, include_content=False)
        return True
    except NoSuchFile:
        return False


def rename_file(db, user_id, old_api_path, new_api_path):
    """
    Rename a file. The file must stay in the same directory.

    TODO: Consider allowing renames to existing directories.
    TODO: Don't do anything if paths are the same.
    """
    old_dir, old_name = split_api_filepath(old_api_path)
    new_dir, new_name = split_api_filepath(new_api_path)
    if old_dir != new_dir:
        raise ValueError(
            dedent(
                """
                Can't rename file to new directory.
                Old Path: {old_api_path}
                New Path: {new_api_path}
                """.format(
                    old_api_path=old_api_path,
                    new_api_path=new_api_path
                )
            )
        )

    if file_exists(db, user_id, new_api_path):
        raise FileExists(new_api_path)

    db.execute(
        files.update().where(
            (files.c.user_id == user_id)
            & (files.c.parent_name == new_dir)
        ).values(
            name=new_name,
        )
    )


def save_file(db, user_id, path, content):
    """
    Save a file.
    """
    directory, name = split_api_filepath(path)
    res = db.execute(
        files.insert().values(
            name=name,
            user_id=user_id,
            parent_name=directory,
            content=content,
        )
    )
    return res


def dir_exists(db, user_id, api_dirname):
    """
    Check if a directory exists.
    """
    return _dir_exists(db, user_id, from_api_dirname(api_dirname))


def _dir_exists(db, user_id, db_dirname):
    """
    Internal implementation of dir_exists.

    Expects a db-style path name.
    """
    return db.execute(
        select(
            [func.count(directories.c.name)],
        ).where(
            and_(
                directories.c.user_id == user_id,
                directories.c.name == db_dirname,
            ),
        )
    ).scalar() != 0


def files_in_directory(db, user_id, db_dirname):
    """
    Return files in a directory.
    """
    fields = _file_default_fields()
    rows = db.execute(
        select(
            fields,
        ).where(
            _is_in_directory(files, user_id, db_dirname),
        ).order_by(
            files.c.user_id,
            files.c.parent_name,
            files.c.name,
            files.c.created_at,
        ).distinct(
            files.c.user_id, files.c.parent_name, files.c.name,
        )
    )
    return [to_dict(fields, row) for row in rows]


def directories_in_directory(db, user_id, db_dirname):
    """
    Return subdirectories of a directory.
    """
    fields = _directory_default_fields()
    rows = db.execute(
        select(
            fields,
        ).where(
            _is_in_directory(directories, user_id, db_dirname),
        )
    )
    return [to_dict(fields, row) for row in rows]


def get_directory(db, user_id, api_dirname, content):
    """
    Return the names of all files/directories that are direct children of
    api_dirname.

    If content is False, return a bare model containing just a database-style
    name.
    """
    db_dirname = from_api_dirname(api_dirname)
    if not _dir_exists(db, user_id, db_dirname):
        raise NoSuchDirectory(api_dirname)
    if content:
        files = files_in_directory(
            db,
            user_id,
            db_dirname,
        )
        subdirectories = directories_in_directory(
            db,
            user_id,
            db_dirname,
        )
    else:
        files, subdirectories = None, None

    # TODO: Consider using namedtuples for these return values.
    return {
        'name': db_dirname,
        'files': files,
        'subdirs': subdirectories,
    }


def _checkpoint_default_fields():
    return files.c.id, files.c.created_at


def _get_checkpoints(db, user_id, api_path, limit=None):
    """
    Get checkpoints from the database.
    """
    query_fields = _checkpoint_default_fields()
    query = select(
        query_fields,
    ).where(
        _file_where(user_id, api_path),
    ).order_by(
        desc(files.c.created_at)
    )
    if limit is not None:
        query = query.limit(limit)

    results = [to_dict(query_fields, record) for record in db.execute(query)]
    if not results:
        raise NoSuchFile(api_path)

    return results


def current_checkpoint(db, user_id, api_path):
    """
    Get the most recent checkpoint.
    """
    return _get_checkpoints(db, user_id, api_path, limit=1)[0]


def all_checkpoints(db, user_id, api_path):
    """
    Get all checkpoints for an api_path.
    """
    return _get_checkpoints(db, user_id, api_path)


def restore_checkpoint(db, user_id, checkpoint_id, api_path):
    """
    Restore a checkpoint by bumping its created_at date to now.
    """
    result = db.execute(
        files.update().where(
            and_(
                _file_where(user_id, api_path),
                files.c.id == int(checkpoint_id),
            )
        ).values(
            created_at=func.now(),
        )
    )
    if not result.rowcount:
        raise NoSuchFile()
