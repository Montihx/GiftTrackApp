# /workspace/db/models.py — SQLAlchemy 2.0 модели (mapping-declarative)
# Полностью соответствуют схеме из ТЗ (10 таблиц).
from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Базовый класс всех моделей."""

    # Типы по умолчанию для PostgreSQL
    type_annotation_map = {
        dict: JSONB,
    }


def _uuid_pk() -> mapped_column[Any]:  # type: ignore[valid-type]
    return mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


# ---------------------------------------------------------------------------
# 1. collections — NFT-коллекции Telegram-подарков
# ---------------------------------------------------------------------------
class Collection(Base):
    __tablename__ = "collections"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    contract_address: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    icon_url: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="telegram_api",
        comment="telegram_api | getgems | manual | seed | tonapi",
    )
    # NULL = цена неизвестна (нет TonAPI-ключа / не удалось получить) — витрина показывает «—»
    floor_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    items_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_updated: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    attributes: Mapped[list["Attribute"]] = relationship(
        back_populates="collection", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint(
            "source IN ('telegram_api','getgems','manual','seed','tonapi')",
            name="ck_collections_source",
        ),
        Index("ix_collections_floor", "floor_price"),
    )


# ---------------------------------------------------------------------------
# 2. attributes — трейты коллекций (model/background/symbol/pattern)
# ---------------------------------------------------------------------------
class Attribute(Base):
    __tablename__ = "attributes"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    collection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("collections.id", ondelete="CASCADE"),
        nullable=False,
    )
    type: Mapped[str] = mapped_column(Text, nullable=False, comment="model|background|symbol|pattern")
    value: Mapped[str] = mapped_column(Text, nullable=False)
    icon_url: Mapped[str | None] = mapped_column(Text)
    floor_price: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False, server_default=text("0"))
    items_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    collection: Mapped[Collection] = relationship(back_populates="attributes")

    __table_args__ = (
        CheckConstraint(
            "type IN ('model','background','symbol','pattern')",
            name="ck_attributes_type",
        ),
        UniqueConstraint("collection_id", "type", "value", name="uq_attributes_coll_type_value"),
        Index("ix_attributes_collection_type", "collection_id", "type"),
    )


# ---------------------------------------------------------------------------
# 3. users — пользователи Telegram
# ---------------------------------------------------------------------------
class User(Base):
    __tablename__ = "users"

    tg_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(Text)
    first_name: Mapped[str | None] = mapped_column(Text)
    sub_expires_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    sub_plan: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'free'"))
    is_banned: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("FALSE"))
    # пауза уведомлений (/pause, /resume) и тихие часы (/quiet) по часовому поясу пользователя (/tz, часы от UTC)
    is_paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("FALSE"))
    quiet_start: Mapped[int | None] = mapped_column(SmallInteger)
    quiet_end: Mapped[int | None] = mapped_column(SmallInteger)
    tz_offset: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=3, server_default=text("3"))
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    filters: Mapped[list["Filter"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    def is_subscribed(self, now: dt.datetime | None = None) -> bool:
        """Активна ли платная подписка (free — всегда 'да' для базовых команд)."""
        now = now or dt.datetime.now(dt.timezone.utc)
        if self.sub_plan == "free":
            return True
        return self.sub_expires_at is not None and self.sub_expires_at > now


# ---------------------------------------------------------------------------
# 4. filters — подписки пользователя на события
# ---------------------------------------------------------------------------
class Filter(Base):
    __tablename__ = "filters"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.tg_id", ondelete="CASCADE"), nullable=False
    )
    collection_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("collections.id"))
    model_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("attributes.id"))
    background_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("attributes.id"))
    symbol_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("attributes.id"))
    pattern_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("attributes.id"))
    floor_min: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    floor_max: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("TRUE"))
    # TRUE: слать листинги/апгрейды только если известен контакт владельца (можно написать)
    require_owner: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("TRUE"))
    # то же для апгрейдов (раньше был один флаг на оба типа событий)
    require_owner_upgrade: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("TRUE")
    )
    # номер подарка (#727) и цена листинга (в монете): диапазоны, пусто — без ограничения
    number_min: Mapped[int | None] = mapped_column(BigInteger)
    number_max: Mapped[int | None] = mapped_column(BigInteger)
    price_min: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    price_max: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    user: Mapped[User] = relationship(back_populates="filters")

    __table_args__ = (
        Index("ix_filters_user_active", "user_id", "is_active"),
        Index("ix_filters_collection", "collection_id"),
    )


# ---------------------------------------------------------------------------
# 5. processed_nfts — замеченные NFT (mint/upgrade)
# ---------------------------------------------------------------------------
class ProcessedNft(Base):
    __tablename__ = "processed_nfts"

    nft_address: Mapped[str] = mapped_column(String(255), primary_key=True)
    collection_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("collections.id"))
    nft_index: Mapped[int] = mapped_column(BigInteger, nullable=False)
    model: Mapped[str | None] = mapped_column(Text)
    background: Mapped[str | None] = mapped_column(Text)
    symbol: Mapped[str | None] = mapped_column(Text)
    pattern: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict | None] = mapped_column(JSONB)
    current_owner_ton: Mapped[str | None] = mapped_column(Text)
    current_owner_tg_id: Mapped[int | None] = mapped_column(BigInteger)
    first_seen_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    ownership_history: Mapped[list["NftOwnershipHistory"]] = relationship(
        back_populates="nft", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("collection_id", "nft_index", name="uq_processed_nfts_coll_index"),
        Index("ix_processed_nfts_owner_tg", "current_owner_tg_id"),
        Index("ix_processed_nfts_first_seen", "first_seen_at"),
    )


# ---------------------------------------------------------------------------
# 6. nft_ownership_history — история владения NFT
# ---------------------------------------------------------------------------
class NftOwnershipHistory(Base):
    __tablename__ = "nft_ownership_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    nft_address: Mapped[str] = mapped_column(
        String(255), ForeignKey("processed_nfts.nft_address", ondelete="CASCADE"), nullable=False
    )
    owner_ton: Mapped[str] = mapped_column(Text, nullable=False)
    owner_tg_id: Mapped[int | None] = mapped_column(BigInteger)
    owner_username: Mapped[str | None] = mapped_column(Text)
    acquired_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    sold_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    nft: Mapped[ProcessedNft] = relationship(back_populates="ownership_history")

    __table_args__ = (
        Index("ix_ownership_nft_acquired", "nft_address", "acquired_at"),
    )


# ---------------------------------------------------------------------------
# 7. model_sales — продажи по моделям (для recent sales)
# ---------------------------------------------------------------------------
class ModelSale(Base):
    __tablename__ = "model_sales"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    collection_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("collections.id"))
    model: Mapped[str] = mapped_column(Text, nullable=False)
    nft_index: Mapped[int | None] = mapped_column(BigInteger)
    nft_address: Mapped[str | None] = mapped_column(Text)
    price: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    marketplace: Mapped[str] = mapped_column(Text, nullable=False)
    buyer_ton: Mapped[str | None] = mapped_column(Text)
    seller_ton: Mapped[str | None] = mapped_column(Text)
    sold_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_model_sales_coll_model_time", "collection_id", "model", "sold_at"),
    )


# ---------------------------------------------------------------------------
# 8. wallet_tg_map — кэш маппинга TON -> Telegram
# ---------------------------------------------------------------------------
class WalletTgMap(Base):
    __tablename__ = "wallet_tg_map"

    ton_address: Mapped[str] = mapped_column(String(255), primary_key=True)
    tg_user_id: Mapped[int | None] = mapped_column(BigInteger)
    username: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="tx_history",
        comment="ton_dns|getgems|tx_history|telethon|manual",
    )
    confidence: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "source IN ('ton_dns','getgems','tx_history','telethon','manual')",
            name="ck_wallet_tg_map_source",
        ),
        Index("ix_wallet_tg_map_tg_id", "tg_user_id"),
    )


# ---------------------------------------------------------------------------
# 9. listener_state — точки прогресса листенеров (last_index и т.п.)
# ---------------------------------------------------------------------------
class ListenerState(Base):
    __tablename__ = "listener_state"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    last_index: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )



# ---------------------------------------------------------------------------
# 11. floor_snapshots — история floor коллекции (пишется при изменении флора)
# ---------------------------------------------------------------------------
class FloorSnapshot(Base):
    __tablename__ = "floor_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    collection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collections.id", ondelete="CASCADE"), nullable=False
    )
    price: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    ts: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (Index("ix_floor_snapshots_coll_ts", "collection_id", "ts"),)


# ---------------------------------------------------------------------------
# 12. sales — продажи NFT (для ленты продаж витрины)
# ---------------------------------------------------------------------------
class Sale(Base):
    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    collection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collections.id", ondelete="CASCADE"), nullable=False
    )
    item_address: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    ts: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    buyer_tg_id: Mapped[int | None] = mapped_column(BigInteger)

    __table_args__ = (
        UniqueConstraint("item_address", "ts", name="uq_sales_item_ts"),  # dedupe повторов
        Index("ix_sales_coll_ts", "collection_id", "ts"),
    )


# ---------------------------------------------------------------------------
# 13. favorites — избранное пользователя (коллекция ИЛИ конкретный NFT)
# ---------------------------------------------------------------------------
class Favorite(Base):
    __tablename__ = "favorites"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.tg_id", ondelete="CASCADE"), nullable=False
    )
    collection_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collections.id", ondelete="CASCADE")
    )
    item_address: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "(collection_id IS NOT NULL) <> (item_address IS NOT NULL)",
            name="ck_favorites_exactly_one_target",
        ),
        Index(
            "uq_favorites_user_collection", "user_id", "collection_id", unique=True,
            postgresql_where=text("collection_id IS NOT NULL"),
            sqlite_where=text("collection_id IS NOT NULL"),
        ),
        Index(
            "uq_favorites_user_item", "user_id", "item_address", unique=True,
            postgresql_where=text("item_address IS NOT NULL"),
            sqlite_where=text("item_address IS NOT NULL"),
        ),
    )


# ---------------------------------------------------------------------------
# 14. payment_invoices — обработанные инвойсы CryptoBot (идемпотентность вебхука)
# ---------------------------------------------------------------------------
class PaymentInvoice(Base):
    """invoice_id — PK: повторная доставка вебхука не продлевает подписку второй раз."""

    __tablename__ = "payment_invoices"

    invoice_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tg_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    days: Mapped[int] = mapped_column(Integer, nullable=False)
    paid_asset: Mapped[str | None] = mapped_column(Text)
    paid_amount: Mapped[Decimal | None] = mapped_column(Numeric(30, 9))
    processed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
