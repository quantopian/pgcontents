"""Remove extra checkpoints table.

Revision ID: 32518998055d
Revises: 597680fc6b80
Create Date: 2015-03-23 14:35:24.572173

"""

# revision identifiers, used by Alembic.
revision = '32518998055d'
down_revision = '597680fc6b80'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade():
    op.drop_table('checkpoints', schema='pgcontents')


def downgrade():
    op.create_table(
        'checkpoints',
        sa.Column(
            'id',
            sa.INTEGER(),
            server_default=sa.text(
                u"nextval('pgcontents.checkpoints_id_seq'::regclass)"
            ),
            nullable=False,
        ),
        sa.Column(
            'file_id',
            sa.INTEGER(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            'created_at',
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False
        ),
        sa.ForeignKeyConstraint(
            ['file_id'],
            [u'pgcontents.files.id'],
            name=u'checkpoints_file_id_fkey',
            onupdate=u'CASCADE',
            ondelete=u'CASCADE'
        ),
        sa.PrimaryKeyConstraint(
            'id',
            name=u'checkpoints_pkey'
        ),
        schema='pgcontents',
    )
