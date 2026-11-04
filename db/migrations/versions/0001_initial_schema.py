# /workspace/db/migrations/versions/0001_initial_schema.py — начальная схема БД (Этап 1)
"""initial schema: collections, attributes, users, filters, processed_nfts,
nft_ownership_history, model_sales, wallet_tg_map, listener_state

Revision ID: 0001
Revises:
Create Date: 2026-11-04

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # gen_random_uuid() требует pgcrypto на PG < 13; на PG16 встроен в core
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # --- collections ---
    op.create_table(
        "collections",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("contract_address", sa.String(255), nullable=False),
        sa.Column("icon_url", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), server_default="telegram_api", nullable=False),
        sa.Column("floor_price", sa.Numeric(20, 6), server_default=sa.text("0"), nullable=False),
        sa.Column("items_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_updated", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("source IN ('telegram_api','getgems','manual')", name="ck_collections_source"),
        sa.UniqueConstraint("contract_address", name="uq_collections_contract"),
    )
    op.create_index("ix_collections_floor", "collections", ["floor_price"])

    # --- attributes ---
    op.create_table(
        "attributes",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("collection_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("icon_url", sa.Text(), nullable=True),
        sa.Column("floor_price", sa.Numeric(20, 6), server_default=sa.text("0"), nullable=False),
        sa.Column("items_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.ForeignKeyConstraint(["collection_id"], ["collections.id"], ondelete="CASCADE"),
        sa.CheckConstraint("type IN ('model','background','symbol','pattern')", name="ck_attributes_type"),
        sa.UniqueConstraint("collection_id", "type", "value", name="uq_attributes_coll_type_value"),
    )
    op.create_index("ix_attributes_collection_type", "attributes", ["collection_id", "type"])

    # --- users ---
    op.create_table(
        "users",
        sa.Column("tg_id", sa.BigInteger(), primary_key=True),
        sa.Column("username", sa.Text(), nullable=True),
        sa.Column("first_name", sa.Text(), nullable=True),
        sa.Column("sub_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sub_plan", sa.Text(), server_default=sa.text("'free'"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- filters ---
    op.create_table(
        "filters",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("collection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("model_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("background_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("symbol_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("pattern_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("floor_min", sa.Numeric(20, 6), nullable=True),
        sa.Column("floor_max", sa.Numeric(20, 6), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("TRUE"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.tg_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["collection_id"], ["collections.id"]),
        sa.ForeignKeyConstraint(["model_id"], ["attributes.id"]),
        sa.ForeignKeyConstraint(["background_id"], ["attributes.id"]),
        sa.ForeignKeyConstraint(["symbol_id"], ["attributes.id"]),
        sa.ForeignKeyConstraint(["pattern_id"], ["attributes.id"]),
    )
    op.create_index("ix_filters_user_active", "filters", ["user_id", "is_active"])
    op.create_index("ix_filters_collection", "filters", ["collection_id"])

    # --- processed_nfts ---
    op.create_table(
        "processed_nfts",
        sa.Column("nft_address", sa.String(255), primary_key=True),
        sa.Column("collection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("nft_index", sa.BigInteger(), nullable=False),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("background", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("pattern", sa.Text(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(), nullable=True),
        sa.Column("current_owner_ton", sa.Text(), nullable=True),
        sa.Column("current_owner_tg_id", sa.BigInteger(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["collection_id"], ["collections.id"]),
        sa.UniqueConstraint("collection_id", "nft_index", name="uq_processed_nfts_coll_index"),
    )
    op.create_index("ix_processed_nfts_owner_tg", "processed_nfts", ["current_owner_tg_id"])
    op.create_index("ix_processed_nfts_first_seen", "processed_nfts", ["first_seen_at"])

    # --- nft_ownership_history ---
    op.create_table(
        "nft_ownership_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("nft_address", sa.String(255), nullable=False),
        sa.Column("owner_ton", sa.Text(), nullable=False),
        sa.Column("owner_tg_id", sa.BigInteger(), nullable=True),
        sa.Column("owner_username", sa.Text(), nullable=True),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sold_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["nft_address"], ["processed_nfts.nft_address"], ondelete="CASCADE"),
    )
    op.create_index("ix_ownership_nft_acquired", "nft_ownership_history", ["nft_address", "acquired_at"])

    # --- model_sales ---
    op.create_table(
        "model_sales",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("collection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("nft_index", sa.BigInteger(), nullable=True),
        sa.Column("nft_address", sa.Text(), nullable=True),
        sa.Column("price", sa.Numeric(20, 6), nullable=False),
        sa.Column("marketplace", sa.Text(), nullable=False),
        sa.Column("buyer_ton", sa.Text(), nullable=True),
        sa.Column("seller_ton", sa.Text(), nullable=True),
        sa.Column("sold_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["collection_id"], ["collections.id"]),
    )
    op.create_index("ix_model_sales_coll_model_time", "model_sales", ["collection_id", "model", "sold_at"])

    # --- wallet_tg_map ---
    op.create_table(
        "wallet_tg_map",
        sa.Column("ton_address", sa.String(255), primary_key=True),
        sa.Column("tg_user_id", sa.BigInteger(), nullable=True),
        sa.Column("username", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), server_default="tx_history", nullable=False),
        sa.Column("confidence", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "source IN ('ton_dns','getgems','tx_history','telethon','manual')",
            name="ck_wallet_tg_map_source",
        ),
    )
    op.create_index("ix_wallet_tg_map_tg_id", "wallet_tg_map", ["tg_user_id"])

    # --- listener_state ---
    op.create_table(
        "listener_state",
        sa.Column("key", sa.String(255), primary_key=True),
        sa.Column("last_index", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    # Удаляем в обратном порядке с учётом FK
    for table in (
        "listener_state",
        "wallet_tg_map",
        "model_sales",
        "nft_ownership_history",
        "processed_nfts",
        "filters",
        "users",
        "attributes",
        "collections",
    ):
        op.drop_table(table)
