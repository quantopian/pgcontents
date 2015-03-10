# encoding: utf-8
"""
Utilities for testing.
"""
from __future__ import unicode_literals
from getpass import getuser
from itertools import starmap
import posixpath

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.schema import (
    MetaData,
    Table,
    DropSchema,
    DropTable,
    ForeignKeyConstraint,
    DropConstraint,
)
from IPython.nbformat.v4.nbbase import (
    new_code_cell,
    new_markdown_cell,
    new_notebook,
    new_raw_cell,
)


from ..api_utils import api_path_join
from ..utils.migrate import upgrade

TEST_DB_URL = "postgresql://{user}@/pgcontents_testing".format(
    user=getuser(),
)


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


def migrate_testing_db(revision='head'):
    """
    Migrate the testing db to the latest alembic revision.
    """
    upgrade(TEST_DB_URL, revision)


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
