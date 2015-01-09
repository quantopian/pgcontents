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
from itertools import izip

from psycopg2.errorcodes import UNIQUE_VIOLATION
from sqlalchemy import Column
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.elements import Cast


@contextmanager
def ignore_unique_violation():
    try:
        yield
    except IntegrityError as error:
        if error.orig.pgcode != UNIQUE_VIOLATION:
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
        for field, value in izip(fields, row)
    }
