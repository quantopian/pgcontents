"""Add unique files constraint.

Revision ID: 1217e5fbdbd9
Revises: 32518998055d
Create Date: 2015-03-23 14:49:33.176862

"""
from textwrap import dedent

# revision identifiers, used by Alembic.
revision = '1217e5fbdbd9'
down_revision = '32518998055d'
branch_labels = None
depends_on = None

from alembic import op


def upgrade():

    temp_select = dedent(
        """
        SELECT DISTINCT ON
            (f.user_id, f.parent_name, f.name)
            id, name, user_id, parent_name, content, created_at
        INTO TEMP TABLE migrate_temp
        FROM
            pgcontents.files AS f
        ORDER BY
            f.user_id, f.parent_name, f.name, f.created_at
        """
    )

    drop_existing_rows = "TRUNCATE TABLE pgcontents.files"
    copy_from_temp_table = dedent(
        """
        INSERT INTO pgcontents.files
        SELECT id, name, user_id, parent_name, content, created_at
        FROM migrate_temp
        """
    )
    drop_temp_table = "DROP TABLE migrate_temp"

    op.execute(temp_select)
    op.execute(drop_existing_rows)
    op.execute(copy_from_temp_table)
    op.execute(drop_temp_table)

    op.create_unique_constraint(
        u'uix_filepath_username',
        'files',
        ['user_id', 'parent_name', 'name'],
        schema='pgcontents',
    )


def downgrade():
    op.drop_constraint(
        u'uix_filepath_username',
        'files',
        schema='pgcontents',
        type_='unique'
    )
