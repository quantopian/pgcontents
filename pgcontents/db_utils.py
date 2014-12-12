"""
Utilities for working with databases.
"""

from contextlib import contextmanager

from psycopg2.errorcodes import UNIQUE_VIOLATION
from sqlalchemy.exc import IntegrityError


@contextmanager
def ignore_unique_violation():
    try:
        yield
    except IntegrityError as error:
        if error.orig.pgcode != UNIQUE_VIOLATION:
            raise
