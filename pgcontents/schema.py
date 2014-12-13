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
from collections import namedtuple
from itertools import izip

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Unicode,
    UniqueConstraint,
    and_,
    desc,
    func,
    null,
    select,
)
from sqlalchemy.exc import IntegrityError

from db_utils import ignore_unique_violation

metadata = MetaData()


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
    Column('user_id', UserID, ForeignKey(users.c.id), nullable=False, primary_key=True),
    Column('name', DirectoryName, nullable=False, primary_key=True),
    Column('parent_user_id', UserID, nullable=True),
    Column('parent_name', DirectoryName, nullable=True),
    ForeignKeyConstraint(
        ['parent_user_id', 'parent_name'],
        ['directories.user_id', 'directories.name'],
    ),
    CheckConstraint('user_id = parent_user_id'),
    # Assert that parent_name is a prefix of name.
    CheckConstraint("position(parent_name in name) != 0"),
    # Assert that all directories begin and end with /.
    CheckConstraint("left(name, 1) = '/'"),
    CheckConstraint("right(name, 1) = '/'"),
    # Assert that the name of this directory has one more '/' than its parent.
    CheckConstraint(
        "length(regexp_replace(name, '[^/]+', '', 'g')) - 1"
        "= length(regexp_replace(parent_name, '[^/]+', '', 'g'))"
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
        )
    ),
)


notebooks = Table(
    'notebooks',
    metadata,
    Column('name', Unicode(40), nullable=False, primary_key=True),
    Column('user_id', UserID, ForeignKey(users.c.id), nullable=False, primary_key=True),
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

def _from_api_dirname(api_dirname):
    if api_dirname == '':
        return '/'
    else:
        return '/' + api_dirname + '/'


def split_api_path(path):
    """
    Split an API path into directory and name.
    """
    parts = path.rsplit('/', 1)
    if len(parts) == 1:
        name = parts[0]
        dirname = ''
    else:
        name = parts[1]
        dirname = parts[0]

    return _from_api_dirname(dirname), name


def adduser_idempotent(db, user_id):
    """
    Add a new user if they don't already exist.
    """
    with ignore_unique_violation():
        db.execute(
            users.insert().values(id=user_id),
        )


def ensure_root_dir(db, user_id):
    """
    Ensure that the given user has a root directory.
    """
    with ignore_unique_violation():
        db.execute(
            directories.insert().values(
                name='/',
                user_id=user_id,
                parent_name=null(),
                parent_user_id=null(),
            )
        )


def to_dict(fields, row):
    """
    Convert a SQLAlchemy row to a dict.
    """
    assert(len(fields) == len(row))
    return {
        field.name: value
        for field, value in izip(fields, row)
    }


def get_notebook(db, user_id, path, include_content):
    """
    Get notebook data for the given user_id and path.

    Include content only if include_content=True.
    """
    directory, name = split_api_path(path)
    query_fields = [notebooks.c.name, notebooks.c.created_at]
    if include_content:
        query_fields.append(notebooks.c.content)

    result = db.execute(
        select(query_fields).where(
            and_(
                notebooks.c.name == name,
                notebooks.c.user_id == user_id,
                notebooks.c.parent_name == directory,
            )
        ).order_by(
            desc(notebooks.c.created_at)
        ).limit(
            1
        )
    ).first()
    return to_dict(query_fields, result)


def save_notebook(db, user_id, path, content):
    """
    Save a notebook.
    """
    directory, name = split_api_path(path)
    res = db.execute(
        notebooks.insert().values(
            name=name,
            user_id=user_id,
            parent_name=directory,
            content=content,
        )
    )
    return res


def dir_exists(db, dirname, user_id):
    """
    Check if a directory exists.
    """
    dirname = _from_api_dirname(dirname)
    return db.execute(
        select(
            [func.count(directories.c.name)],
        ).where(
            and_(
                directories.c.user_id == user_id,
                directories.c.name == dirname,
            ),
        )
    ).scalar() != 0


def listdir(db, dirname, user_id):
    """
    Return file/directory names.
    """
    # file_query = _listdir_files(dirname, user_id)
    # dir_query = _listdir_directories(dirname, user_id)
    # return db.execute(
    #     select(file_query)
    # )
    # query = db.execute(
    #     select(
    #         [notebooks.c.name],
    #     ).where(
    #         and_(
    #             notebooks.c.dirname == dirname,
    #             notebooks.c.user_id == user_id,
    #         ),
    #     ).union_all(
    #     )
    # )
    # return list(query)
