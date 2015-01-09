"""
Setup/Teardown for tests.
"""

from .utils import (
    drop_testing_db_tables,
    migrate_testing_db,
)


def setup_module():
    drop_testing_db_tables()
    migrate_testing_db()


def teardown_module():
    drop_testing_db_tables()
