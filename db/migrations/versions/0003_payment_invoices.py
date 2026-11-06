# db/migrations/versions/0003_payment_invoices.py — обработанные инвойсы CryptoBot
"""payment_invoices: идемпотентность вебхука CryptoBot

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-06

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "payment_invoices",
        sa.Column("invoice_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("tg_id", sa.BigInteger(), nullable=False),
        sa.Column("days", sa.Integer(), nullable=False),
        sa.Column("paid_asset", sa.Text(), nullable=True),
        sa.Column("paid_amount", sa.Numeric(30, 9), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("invoice_id"),
    )


def downgrade() -> None:
    op.drop_table("payment_invoices")
