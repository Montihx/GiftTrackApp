# db/migrations/versions/0007_users_is_banned.py — бан пользователя (админка)
"""users.is_banned

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-06

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("is_banned", sa.Boolean(), nullable=False, server_default=sa.text("FALSE")))


def downgrade() -> None:
    op.drop_column("users", "is_banned")
