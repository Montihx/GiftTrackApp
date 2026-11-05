# db/migrations/versions/0002_showcase_tables.py — витрина: seed-источник, nullable floor,
# floor_snapshots, sales, favorites
"""showcase: collections.source += 'seed', floor_price nullable, floor_snapshots, sales, favorites

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-05

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- collections: source 'seed' + floor_price nullable ---
    op.drop_constraint("ck_collections_source", "collections", type_="check")
    op.create_check_constraint(
        "ck_collections_source", "collections",
        "source IN ('telegram_api','getgems','manual','seed')",
    )
    op.alter_column("collections", "floor_price", existing_type=sa.Numeric(20, 6),
                    nullable=True, server_default=None)
    # 0 раньше означал «неизвестно» — теперь это NULL
    op.execute("UPDATE collections SET floor_price = NULL WHERE floor_price = 0")

    # --- floor_snapshots ---
    op.create_table(
        "floor_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("collection_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("price", sa.Numeric(20, 6), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["collection_id"], ["collections.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_floor_snapshots_coll_ts", "floor_snapshots", ["collection_id", "ts"])

    # --- sales ---
    op.create_table(
        "sales",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("collection_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("item_address", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(20, 6), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("buyer_tg_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["collection_id"], ["collections.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("item_address", "ts", name="uq_sales_item_ts"),
    )
    op.create_index("ix_sales_coll_ts", "sales", ["collection_id", "ts"])

    # --- favorites ---
    op.create_table(
        "favorites",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("collection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("item_address", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.tg_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["collection_id"], ["collections.id"], ondelete="CASCADE"),
        sa.CheckConstraint("(collection_id IS NOT NULL) <> (item_address IS NOT NULL)",
                           name="ck_favorites_exactly_one_target"),
    )
    op.create_index("uq_favorites_user_collection", "favorites", ["user_id", "collection_id"],
                    unique=True, postgresql_where=sa.text("collection_id IS NOT NULL"))
    op.create_index("uq_favorites_user_item", "favorites", ["user_id", "item_address"],
                    unique=True, postgresql_where=sa.text("item_address IS NOT NULL"))


def downgrade() -> None:
    op.drop_table("favorites")
    op.drop_table("sales")
    op.drop_table("floor_snapshots")
    op.execute("UPDATE collections SET floor_price = 0 WHERE floor_price IS NULL")
    op.alter_column("collections", "floor_price", existing_type=sa.Numeric(20, 6),
                    nullable=False, server_default=sa.text("0"))
    op.execute("UPDATE collections SET source = 'manual' WHERE source = 'seed'")
    op.drop_constraint("ck_collections_source", "collections", type_="check")
    op.create_check_constraint(
        "ck_collections_source", "collections",
        "source IN ('telegram_api','getgems','manual')",
    )
