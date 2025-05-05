"""Add customer_id to users

Revision ID: add_customer_id
Create Date: 2025-05-05

"""
from alembic import op
import sqlalchemy as sa
import uuid

# revision identifiers, used by Alembic.
revision = 'add_customer_id'
down_revision = None  # Change this to match your previous migration
branch_labels = None
depends_on = None


def upgrade():
    # Add column
    op.add_column('users', sa.Column('customer_id', sa.String(36), nullable=True, unique=True))

    # Update existing users
    connection = op.get_bind()
    users = connection.execute(sa.text('SELECT id FROM users')).fetchall()
    for user in users:
        connection.execute(
            sa.text(f"UPDATE users SET customer_id = :cid WHERE id = :id"),
            {'cid': str(uuid.uuid4()), 'id': user.id}
        )

    # Make it non-nullable
    op.alter_column('users', 'customer_id', nullable=False)


def downgrade():
    op.drop_column('users', 'customer_id')
