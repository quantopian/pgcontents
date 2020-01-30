"""empty message

Revision ID: 33b3645dc7f5
Revises:
Create Date: 2014-12-17 11:37:24.122882

"""

# revision identifiers, used by Alembic.
revision = '33b3645dc7f5'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('users',
    sa.Column('id', sa.Unicode(length=30), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('directories',
    sa.Column('user_id', sa.Unicode(length=30), nullable=False),
    sa.Column('name', sa.Unicode(length=70), nullable=False),
    sa.Column('parent_user_id', sa.Unicode(length=30), nullable=True),
    sa.Column('parent_name', sa.Unicode(length=70), nullable=True),
    sa.CheckConstraint(u"left(name, 1) = '/'", name=u'directories_startwith_slash'),
    sa.CheckConstraint(u"length(regexp_replace(name, '[^/]+', '', 'g')) - 1= length(regexp_replace(parent_name, '[^/]+', '', 'g'))", name=u'directories_slash_count'),
    sa.CheckConstraint(u"right(name, 1) = '/'", name=u'directories_endwith_slash'),
    sa.CheckConstraint(u'(parent_name IS NULL AND parent_user_id IS NULL) OR (parent_name IS NOT NULL AND parent_user_id IS NOT NULL)', name=u'directories_null_user_id_match'),
    sa.CheckConstraint(u'position(parent_name in name) != 0', name=u'directories_parent_name_prefix'),
    sa.CheckConstraint(u'user_id = parent_user_id', name=u'directories_match_user_id'),
    sa.ForeignKeyConstraint(['parent_user_id', 'parent_name'], [u'directories.user_id', u'directories.name'], ),
    sa.ForeignKeyConstraint(['user_id'], [u'users.id'], ),
    sa.PrimaryKeyConstraint('user_id', 'name')
    )
    op.create_table('files',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.Unicode(length=40), nullable=False),
    sa.Column('user_id', sa.Unicode(length=30), nullable=False),
    sa.Column('parent_name', sa.Unicode(length=70), nullable=False),
    sa.Column('content', sa.LargeBinary(length=100000), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['user_id', 'parent_name'], [u'directories.user_id', u'directories.name'], ),
    sa.ForeignKeyConstraint(['user_id'], [u'users.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('checkpoints',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('file_id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['file_id'], [u'files.id'], onupdate=u'CASCADE', ondelete=u'CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('checkpoints')
    op.drop_table('files')
    op.drop_table('directories')
    op.drop_table('users')
    ### end Alembic commands ###