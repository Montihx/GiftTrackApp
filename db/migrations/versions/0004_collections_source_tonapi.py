# db/migrations/versions/0004_collections_source_tonapi.py — source='tonapi' для автообнаруженных коллекций
"""collections.source += 'tonapi' (автообнаружение коллекций Telegram Gifts через TonAPI)

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-06

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("ck_collections_source", "collections", type_="check")
    op.create_check_constraint(
        "ck_collections_source", "collections",
        "source IN ('telegram_api','getgems','manual','seed','tonapi')",
    )


def downgrade() -> None:
    op.execute("UPDATE collections SET source = 'telegram_api' WHERE source = 'tonapi'")
    op.drop_constraint("ck_collections_source", "collections", type_="check")
    op.create_check_constraint(
        "ck_collections_source", "collections",
        "source IN ('telegram_api','getgems','manual','seed')",
    )
