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

from db_utils import ignore_unique_violation
from .error import (
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


notebooks = Table(
    'notebooks',
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


def ensure_directory(db, user_id, api_path):
    """
    Ensure that the given user has the given directory.
    """
    name = from_api_dirname(api_path)
    if name == '/':
        parent_name = null()
        parent_user_id = null()
    else:
        # Convert '/foo/bar/buzz/' -> '/foo/bar/'
        parent_name = name[:name.rindex('/', 0, -1) + 1]
        parent_user_id = user_id

    with ignore_unique_violation():
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


def _notebook_where(user_id, api_path):
    """
    Return a WHERE clause matching the given API path and user_id.
    """
    directory, name = split_api_filepath(api_path)
    return and_(
        notebooks.c.name == name,
        notebooks.c.user_id == user_id,
        notebooks.c.parent_name == directory,
    )


def _notebook_default_fields():
    """
    Default fields returned by a notebook query.
    """
    return [
        notebooks.c.name,
        notebooks.c.created_at,
        notebooks.c.parent_name,
    ]


def _directory_default_fields():
    """
    Default fields returned by a notebook query.
    """
    return [
        directories.c.name,
    ]


def get_notebook(db, user_id, api_path, include_content):
    """
    Get notebook data for the given user_id and path.

    Include content only if include_content=True.
    """
    query_fields = _notebook_default_fields()
    if include_content:
        query_fields.append(notebooks.c.content)

    result = db.execute(
        select(query_fields).where(
            _notebook_where(user_id, api_path),
        ).order_by(
            desc(notebooks.c.created_at)
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
        notebooks.delete().where(
            _notebook_where(user_id, api_path)
        )
    )
    if not result.rowcount:
        raise NoSuchFile(api_path)

    # TODO: This is misleading because we allow multiple files with the same
    # user_id/name as checkpoints.  Consider de-duping this in some way?
    return result.rowcount


def delete_directory(db, user_id, api_path):
    """
    Delete a directory.

    TODO: Consider making this a soft delete.
    """
    raise NotImplementedError()


def notebook_exists(db, user_id, path):
    """
    Check if a notebook exists.
    """
    return get_notebook(db, user_id, path, include_content=False) is None


def rename_file(db, user_id, old_path, new_path):
    """
    Rename a file. The file must stay in the same directory.

    TODO: Consider allowing renames to existing directories.
    TODO: Don't do anything if paths are the same.
    """
    old_dir, old_name = split_api_filepath(old_path)
    new_dir, new_name = split_api_filepath(new_path)
    if not old_dir == new_dir:
        raise ValueError(
            dedent(
                """
                Can't rename file to new directory.
                Old Path: {old_path}
                New Path: {new_path}
                """.format(old_path=old_path, new_path=new_path)
            )
        )

    db.execute(
        notebooks.update().where(
            (notebooks.c.user_id == user_id)
            & (notebooks.c.parent_name == new_dir)
        ).values(
            name=new_name,
        )
    )


def save_notebook(db, user_id, path, content):
    """
    Save a notebook.
    """
    directory, name = split_api_filepath(path)
    res = db.execute(
        notebooks.insert().values(
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


def _directory_contents(db, table, fields, user_id, db_dirname):
    """
    Return names of entries in the given directory.

    Parameterized by table/fields because this has the same query structure for
    notebooks and directories.
    """
    rows = db.execute(
        select(
            fields,
        ).where(
            and_(
                table.c.parent_name == db_dirname,
                table.c.user_id == user_id,
            )
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
        files = _directory_contents(
            db,
            notebooks,
            _notebook_default_fields(),
            user_id,
            db_dirname,
        )
        subdirectories = _directory_contents(
            db,
            directories,
            _directory_default_fields(),
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
