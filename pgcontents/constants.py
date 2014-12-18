"""
Shared constants.
"""
from os.path import (
    dirname,
    join,
)

ALEMBIC_DIR_LOCATION = join(dirname(__file__), 'alembic')
with open(join(dirname(__file__), 'alembic.ini.template')) as f:
    ALEMBIC_INI_TEMPLATE = f.read()

DB_URL_ENVVAR = 'PGCONTENTS_DB'
UNLIMITED = 0
