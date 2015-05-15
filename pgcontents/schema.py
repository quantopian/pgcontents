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

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    func,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Unicode,
    UniqueConstraint,
)

metadata = MetaData(schema='pgcontents')

# Shared Types
UserID = Unicode(30)
FilePath = Unicode(300)

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
    Column('name', FilePath, nullable=False, primary_key=True),
    Column('parent_user_id', UserID, nullable=True),
    Column('parent_name', FilePath, nullable=True),
    # =========== #
    # Constraints #
    # =========== #
    ForeignKeyConstraint(
        ['parent_user_id', 'parent_name'],
        ['directories.user_id', 'directories.name'],
        deferrable=True,
        initially=u'IMMEDIATE'
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
    Column('name', FilePath, nullable=False),
    Column(
        'user_id',
        UserID,
        ForeignKey(users.c.id),
        nullable=False,
    ),
    Column('parent_name', FilePath, nullable=False),
    Column('content', LargeBinary(100000), nullable=False),
    Column(
        'created_at',
        DateTime,
        default=func.now(),
        nullable=False,
    ),
    UniqueConstraint(
        'user_id',
        'parent_name',
        'name',
        name="uix_filepath_username"
    ),
    ForeignKeyConstraint(
        ['user_id', 'parent_name'],
        [directories.c.user_id, directories.c.name],
        onupdate=u'CASCADE'
    ),
)


# Alternate checkpoint table used by PostgresCheckpointsManager.
remote_checkpoints = Table(
    'remote_checkpoints',
    metadata,
    Column('id', Integer(), nullable=False, primary_key=True),
    Column(
        'user_id',
        UserID,
        ForeignKey(users.c.id),
        nullable=False,
    ),
    Column('path', FilePath, nullable=False),
    Column('content', LargeBinary(100000), nullable=False),
    Column('last_modified', DateTime, default=func.now(), nullable=False),
)
