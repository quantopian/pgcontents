# encoding: utf-8
"""
Utilities for testing.
"""
from __future__ import unicode_literals
from contextlib import contextmanager
from cryptography.fernet import Fernet
from getpass import getuser
from itertools import starmap
import os
import posixpath
from unicodedata import normalize

from IPython.utils import py3compat
from nose.tools import nottest
from sqlalchemy import create_engine
from tornado.web import HTTPError

from ..api_utils import api_path_join
from ..crypto import FernetEncryption
from ..schema import metadata
from ..utils.ipycompat import (
    new_code_cell,
    new_markdown_cell,
    new_notebook,
    new_raw_cell,
)
from ..utils.migrate import upgrade


TEST_DB_URL = os.environ.get('PGCONTENTS_TEST_DB_URL')
if TEST_DB_URL is None:
    TEST_DB_URL = "postgresql://{user}@/pgcontents_testing".format(
        user=getuser(),
    )


def make_fernet():
    return FernetEncryption(Fernet(Fernet.generate_key()))


def _norm_unicode(s):
    """Normalize unicode strings"""
    return normalize('NFC', py3compat.cast_unicode(s))


@contextmanager
def assertRaisesHTTPError(testcase, status, msg=None):
    msg = msg or "Should have raised HTTPError(%i)" % status
    try:
        yield
    except HTTPError as e:
        testcase.assertEqual(e.status_code, status)
    else:
        testcase.fail(msg)


_tables = (
    'pgcontents.remote_checkpoints',
    'pgcontents.files',
    'pgcontents.directories',
    'pgcontents.users',
)
unexpected_tables = set(metadata.tables) - set(_tables)
if unexpected_tables:
    raise Exception("Unexpected tables in metadata: %s" % unexpected_tables)


@nottest
def clear_test_db():
    engine = create_engine(TEST_DB_URL)
    with engine.connect() as conn:
        for table in map(metadata.tables.__getitem__, _tables):
            conn.execute(table.delete())


@nottest
def remigrate_test_schema():
    """
    Drop recreate the test db schema.
    """
    drop_testing_db_tables()
    migrate_testing_db()


@nottest
def drop_testing_db_tables():
    """
    Drop all tables from the testing db.
    """
    engine = create_engine(TEST_DB_URL)
    conn = engine.connect()
    trans = conn.begin()
    conn.execute('DROP SCHEMA IF EXISTS pgcontents CASCADE')
    conn.execute('DROP TABLE IF EXISTS alembic_version CASCADE')

    trans.commit()


@nottest
def migrate_testing_db(revision='head'):
    """
    Migrate the testing db to the latest alembic revision.
    """
    upgrade(TEST_DB_URL, revision)


@nottest
def test_notebook(name):
    """
    Make a test notebook for the given name.
    """
    nb = new_notebook()
    nb.cells.append(new_code_cell("'code_' + '{}'".format(name)))
    nb.cells.append(new_raw_cell("raw_{}".format(name)))
    nb.cells.append(new_markdown_cell('markdown_{}'.format(name)))
    return nb


def populate(contents_mgr):
    """
    Populate a test directory with a ContentsManager.
    """
    dirs_nbs = [
        ('', 'inroot.ipynb'),
        ('Directory with spaces in', 'inspace.ipynb'),
        ('unicodé', 'innonascii.ipynb'),
        ('foo', 'a.ipynb'),
        ('foo', 'name with spaces.ipynb'),
        ('foo', 'unicodé.ipynb'),
        ('foo/bar', 'baz.ipynb'),
        ('å b', 'ç d.ipynb'),
    ]

    for dirname, nbname in dirs_nbs:
        contents_mgr.save({'type': 'directory'}, path=dirname)
        contents_mgr.save(
            {'type': 'notebook', 'content': test_notebook(nbname)},
            path=api_path_join(dirname, nbname),
        )
    return list(starmap(posixpath.join, dirs_nbs))
