# /workspace/services/ton/state.py — работа с listener_state и processed_nfts
"""Сохранение состояния листенера и NFT в БД (SQLite/PostgreSQL совместимо)."""
from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import orjson
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ListenerState, NftOwnershipHistory, ProcessedNft
from services.ton.events import MintEvent, TransferEvent


class TonState:
    """Управление состоянием TON-листенера: last_timestamp + NFT в БД."""

    STATE_KEY = "ton_listener"

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def load_last_timestamp(self, key: str | None = None) -> int:
        """Загрузить последний обработанный timestamp для ключа (по умолчанию общий)."""
        state_key = key or self.STATE_KEY
        stmt = select(ListenerState.last_index).where(ListenerState.key == state_key)
        row = (await self.session.execute(stmt)).first()
        return int(row[0]) if row else 0

    async def save_last_timestamp(self, timestamp: int, key: str | None = None) -> None:
        """
        Сохранить последний обработанный timestamp для ключа (upsert через Core).

        Использует SQLAlchemy Core (не ORM) для надёжного upsert: если запись
        существует — UPDATE, иначе INSERT. Это обходит проблему identity map,
        когда ORM не видит объект и пытается вставить дубль.
        """
        from sqlalchemy import insert, update

        state_key = key or self.STATE_KEY
        tbl = ListenerState.__table__

        # Проверяем существование через Core (быстрее ORM и не зависит от identity map)
        stmt = select(tbl.c.key).where(tbl.c.key == state_key)
        row = (await self.session.execute(stmt)).first()

        if row is None:
            # INSERT новой записи
            insert_stmt = insert(tbl).values(
                key=state_key,
                last_index=timestamp,
                updated_at=dt.datetime.now(dt.timezone.utc),
            )
            await self.session.execute(insert_stmt)
        else:
            # UPDATE существующей записи
            update_stmt = (
                update(tbl)
                .where(tbl.c.key == state_key)
                .values(
                    last_index=timestamp,
                    updated_at=dt.datetime.now(dt.timezone.utc),
                )
            )
            await self.session.execute(update_stmt)

        await self.session.flush()



        if state is None:
            new_state = ListenerState(key=self.STATE_KEY, last_index=timestamp)
            self.session.add(new_state)
        else:
            state.last_index = timestamp
            state.updated_at = dt.datetime.now(dt.timezone.utc)

        await self.session.flush()

    async def upsert_mint(self, event: MintEvent, metadata: dict[str, Any] | None = None) -> None:
        """
        Вставить или обновить NFT в processed_nfts.

        metadata_json передаётся dict как есть: колонка JSONB имеет
        bind/result-процессоры SQLAlchemy на обоих диалектах (PG и SQLite),
        сериализация/десериализация выполняются автоматически.
        """
        stmt = select(ProcessedNft).where(ProcessedNft.nft_address == event.nft_address)
        nft = (await self.session.execute(stmt)).scalar_one_or_none()

        if nft is None:
            self.session.add(
                ProcessedNft(
                    nft_address=event.nft_address,
                    nft_index=event.index,
                    current_owner_ton=event.owner_ton,
                    metadata_json=metadata,
                )
            )
            logger.debug(f"Mint: создан NFT {event.nft_address} (index={event.index})")
        else:
            nft.current_owner_ton = event.owner_ton
            if metadata is not None:
                nft.metadata_json = metadata
            logger.debug(f"Mint: обновлён NFT {event.nft_address}")

        await self.session.flush()


    async def record_transfer(self, event: TransferEvent) -> None:
        """
        Записать transfer в processed_nfts (обновить владельца) и nft_ownership_history.

        Если NFT не найден в processed_nfts — создаём запись с текущим владельцем.
        """
        stmt = select(ProcessedNft).where(ProcessedNft.nft_address == event.nft_address)
        nft = (await self.session.execute(stmt)).scalar_one_or_none()

        prev_owner = nft.current_owner_ton if nft else None

        if nft is None:
            # NFT не был замечен ранее — создаём запись
            new_nft = ProcessedNft(
                nft_address=event.nft_address,
                nft_index=0,  # неизвестен
                current_owner_ton=event.new_owner_ton,
            )
            self.session.add(new_nft)
            await self.session.flush()
            nft = new_nft
        else:
            # Обновляем текущего владельца
            nft.current_owner_ton = event.new_owner_ton
            await self.session.flush()

        # Записываем в историю владения
        history_entry = NftOwnershipHistory(
            nft_address=event.nft_address,
            owner_ton=event.new_owner_ton,
            acquired_at=event.timestamp,
        )
        self.session.add(history_entry)

        logger.debug(
            f"Transfer: {event.nft_address} от {prev_owner or 'unknown'} -> {event.new_owner_ton}"
        )
