# db/migrations/versions/0006_filters_split_owner_number_price.py — раздельные «только с контактом» и диапазоны номера/цены
"""filters: require_owner_upgrade, number_min/max, price_min/max

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-06

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # require_owner теперь только для листингов; для апгрейдов — отдельный флаг (значение берём из прежнего)
    op.add_column("filters", sa.Column("require_owner_upgrade", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")))
    op.execute("UPDATE filters SET require_owner_upgrade = require_owner")
    op.add_column("filters", sa.Column("number_min", sa.BigInteger(), nullable=True))
    op.add_column("filters", sa.Column("number_max", sa.BigInteger(), nullable=True))
    op.add_column("filters", sa.Column("price_min", sa.Numeric(20, 6), nullable=True))
    op.add_column("filters", sa.Column("price_max", sa.Numeric(20, 6), nullable=True))


def downgrade() -> None:
    for col in ("price_max", "price_min", "number_max", "number_min", "require_owner_upgrade"):
        op.drop_column("filters", col)
