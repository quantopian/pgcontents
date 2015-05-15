"""Change properties on foreign key constraints.

Revision ID: 2d46c89138b0
Revises: 1217e5fbdbd9
Create Date: 2015-05-14 16:53:00.073652

"""

# revision identifiers, used by Alembic.
revision = '2d46c89138b0'
down_revision = '1217e5fbdbd9'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():

    # Drop the existing foreign key
    op.drop_constraint(
        'directories_parent_user_id_fkey',
        'directories',
        type_='foreignkey',
        schema='pgcontents'
    )

    # Add the foreign key back, make it DEFERRABLE INITIALLY IMMEDIATE
    op.create_foreign_key(
        'directories_parent_user_id_fkey',
        'directories',
        'directories',
        ['parent_user_id', 'parent_name'],
        ['user_id', 'name'],
        deferrable=True,
        initially='IMMEDIATE',
        source_schema='pgcontents',
        referent_schema='pgcontents',
    )

    # Drop the existing foreign key
    op.drop_constraint(
        'files_user_id_fkey',
        'files',
        type_='foreignkey',
        schema='pgcontents'
    )

    # Add the foreign key back, make it cascade on update
    op.create_foreign_key(
        'files_user_id_fkey',
        'files',
        'directories',
        ['user_id', 'parent_name'],
        ['user_id', 'name'],
        onupdate='CASCADE',
        source_schema='pgcontents',
        referent_schema='pgcontents',
    )

def downgrade():

    op.drop_constraint(
        'directories_parent_user_id_fkey',
        'directories',
        type_='foreignkey',
        schema='pgcontents'
    )

    # Add the foreign key back, without any deferrable settings
    op.create_foreign_key(
        'directories_parent_user_id_fkey',
        'directories',
        'directories',
        ['parent_user_id', 'parent_name'],
        ['user_id', 'name'],
        source_schema='pgcontents',
        referent_schema='pgcontents',
    )

    op.drop_constraint(
        'files_user_id_fkey',
        'files',
        type_='foreignkey',
        schema='pgcontents'
    )

    # Add the foreign key back, without any onupdate setting
    op.create_foreign_key(
        'files_user_id_fkey',
        'files',
        'directories',
        ['user_id', 'parent_name'],
        ['user_id', 'name'],
        source_schema='pgcontents',
        referent_schema='pgcontents',
    )
