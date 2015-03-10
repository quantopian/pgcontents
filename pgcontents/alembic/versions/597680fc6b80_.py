"""Move pgcontents data to its own schema.

Revision ID: 597680fc6b80
Revises: 551f95fbd4a2
Create Date: 2015-03-17 20:18:34.371236

"""

# revision identifiers, used by Alembic.
revision = '597680fc6b80'
down_revision = '551f95fbd4a2'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade():
    conn = op.get_bind()
    conn.execute('CREATE SCHEMA pgcontents')
    conn.execute('ALTER TABLE users SET SCHEMA pgcontents')
    conn.execute('ALTER TABLE directories SET SCHEMA pgcontents')
    conn.execute('ALTER TABLE files SET SCHEMA pgcontents')
    conn.execute('ALTER TABLE checkpoints SET SCHEMA pgcontents')
    conn.execute('ALTER TABLE remote_checkpoints SET SCHEMA pgcontents')


def downgrade():
    conn = op.get_bind()
    conn.execute('ALTER TABLE pgcontents.users SET SCHEMA public')
    conn.execute('ALTER TABLE pgcontents.directories SET SCHEMA public')
    conn.execute('ALTER TABLE pgcontents.files SET SCHEMA public')
    conn.execute('ALTER TABLE pgcontents.checkpoints SET SCHEMA public')
    conn.execute('ALTER TABLE pgcontents.remote_checkpoints SET SCHEMA public')
    conn.execute('DROP SCHEMA pgcontents')
