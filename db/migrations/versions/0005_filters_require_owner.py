# db/migrations/versions/0005_filters_require_owner.py — флаг «слать только с контактом владельца»
"""filters.require_owner

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-06

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("filters", sa.Column("require_owner", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")))


def downgrade() -> None:
    op.drop_column("filters", "require_owner")
