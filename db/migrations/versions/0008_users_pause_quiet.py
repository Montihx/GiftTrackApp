# db/migrations/versions/0008_users_pause_quiet.py — пауза уведомлений, тихие часы, часовой пояс
"""users: is_paused, quiet_start, quiet_end, tz_offset

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-06

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: Union[str, None] = "0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("is_paused", sa.Boolean(), nullable=False, server_default=sa.text("FALSE")))
    op.add_column("users", sa.Column("quiet_start", sa.SmallInteger(), nullable=True))
    op.add_column("users", sa.Column("quiet_end", sa.SmallInteger(), nullable=True))
    op.add_column("users", sa.Column("tz_offset", sa.SmallInteger(), nullable=False, server_default=sa.text("3")))


def downgrade() -> None:
    for col in ("tz_offset", "quiet_end", "quiet_start", "is_paused"):
        op.drop_column("users", col)
