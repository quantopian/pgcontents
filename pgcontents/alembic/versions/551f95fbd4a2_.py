"""Increase max size on FilePath.

Revision ID: 551f95fbd4a2
Revises: 3d5ea85fc44f
Create Date: 2015-01-16 00:15:50.761819

"""

# revision identifiers, used by Alembic.
revision = '551f95fbd4a2'
down_revision = '3d5ea85fc44f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


OldFilePath = sa.Unicode(70)
NewFilePath = sa.Unicode(300)

tables_cols = [
    ('directories', 'name'),
    ('directories', 'parent_name'),
    ('files', 'parent_name'),
    ('remote_checkpoints', 'path'),
]


def upgrade():
    op.alter_column('files', 'name', type_=NewFilePath)
    for tablename, colname in tables_cols:
        op.alter_column(tablename, colname, type_=NewFilePath)

def downgrade():
    op.alter_column('files', 'name', type_=sa.Unicode(40))
    for tablename, colname in tables_cols:
        op.alter_column(tablename, colname, type_=OldFilePath)

