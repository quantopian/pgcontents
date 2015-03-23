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
Utilities for working with databases.
"""

from contextlib import contextmanager
from six.moves import zip

from psycopg2.errorcodes import (
    FOREIGN_KEY_VIOLATION,
    UNIQUE_VIOLATION,
)
from sqlalchemy import Column
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.elements import Cast


def is_unique_violation(error):
    return error.orig.pgcode == UNIQUE_VIOLATION


def is_foreign_key_violation(error):
    return error.orig.pgcode == FOREIGN_KEY_VIOLATION


@contextmanager
def ignore_unique_violation():
    """
    Context manager for gobbling unique violations.

    NOTE: If a unique violation is raised, the existing psql connection will
    not accept new commands.  This just silences the python-level error.  If
    you need emit another command after possibly ignoring a unique violation,
    you should explicitly use savepoints.
    """
    try:
        yield
    except IntegrityError as error:
        if not is_unique_violation(error):
            raise


def _get_name(column_like):
    """
    Get the name from a column-like SQLAlchemy expression.

    Works for Columns and Cast expressions.
    """
    if isinstance(column_like, Column):
        return column_like.name
    elif isinstance(column_like, Cast):
        return column_like.clause.name


def to_dict(fields, row):
    """
    Convert a SQLAlchemy row to a dict.

    If row is None, return None.
    """
    assert(len(fields) == len(row))
    return {
        _get_name(field): value
        for field, value in zip(fields, row)
    }
