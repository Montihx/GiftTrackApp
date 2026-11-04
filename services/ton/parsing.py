# /workspace/services/ton/parsing.py — парсинг ответов Tonviewer API
"""Парсинг транзакций из Tonviewer API в MintEvent/TransferEvent."""
from __future__ import annotations

import datetime as dt
from typing import Any

from loguru import logger

from services.ton.events import BlockEvents, MintEvent, TransferEvent


def parse_tonviewer_transactions(
    transactions: list[dict[str, Any]],
    collection_address: str,
    seqno: int,
) -> BlockEvents:
    """
    Парсит список транзакций из Tonviewer API в BlockEvents.

    Пример транзакции от Tonviewer (упрощённо):
    {
        "hash": "abc123...",
        "time": 1699999999,
        "in_msg": {
            "source": "EQ...",
            "destination": "EQ...nft_contract",
            "value": "1000000000",
            "op": 1,  # или другой op_code
            "decoded_body": {
                "type": "nft_transfer",
                "query_id": 123,
                "new_owner": "EQ...new_owner",
                "response_destination": "EQ...old_owner",
                "forward_amount": "0",
                "forward_payload": null
            }
        },
        "out_msgs": [...]
    }

    Для mint событий Tonviewer обычно отдаёт:
    {
        "hash": "...",
        "time": 1699999999,
        "in_msg": {
            "source": "EQ...collection_contract",
            "destination": "EQ...new_nft",
            "op": 0x1  # nft_itemDeploy или аналог
        }
    }
    """
    events = BlockEvents(seqno=seqno)

    for tx in transactions:
        try:
            tx_hash = tx.get("hash", "")
            time_unix = tx.get("time", 0)
            timestamp = dt.datetime.fromtimestamp(time_unix, tz=dt.timezone.utc)

            in_msg = tx.get("in_msg") or {}
            decoded = in_msg.get("decoded_body") or {}
            msg_type = decoded.get("type", "")

            # Определяем тип события
            if msg_type == "nft_transfer" or _is_transfer_op(in_msg.get("op")):
                transfer = _parse_transfer(tx_hash, timestamp, in_msg, decoded)
                if transfer:
                    events.add_transfer(transfer)

            elif _is_mint_event(tx, collection_address):
                mint = _parse_mint(tx_hash, timestamp, tx, collection_address)
                if mint:
                    events.add_mint(mint)

        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Ошибка парсинга транзакции {tx.get('hash', 'unknown')}: {exc!r}")
            continue

    return events


def _is_transfer_op(op: int | None) -> bool:
    """Проверка op_code для transfer (0x5fcc3d14 — стандарт NFT transfer)."""
    if op is None:
        return False
    return op == 0x5FCC3D14 or op == 1607361556  # оба формата


def _is_mint_event(tx: dict[str, Any], collection_address: str) -> bool:
    """
    Определяет, является ли транзакция mint-событием.

    Признаки mint:
    - in_msg.source == collection_address
    - in_msg.destination — это новый NFT контракт
    - op_code == 0x1 (nft_itemDeploy) или аналог
    """
    in_msg = tx.get("in_msg") or {}
    source = in_msg.get("source", "")
    op = in_msg.get("op")

    # Mint: коллекция создаёт новый NFT
    if source and _normalize_addr(source) == _normalize_addr(collection_address):
        # Проверяем op_code или отсутствие decoded_body (raw mint)
        if op in (0x1, 1) or not in_msg.get("decoded_body"):
            return True

    return False


def _parse_mint(
    tx_hash: str,
    timestamp: dt.datetime,
    tx: dict[str, Any],
    collection_address: str,
) -> MintEvent | None:
    """Извлекает данные mint-события из транзакции."""
    in_msg = tx.get("in_msg") or {}
    decoded = in_msg.get("decoded_body") or {}

    nft_address = in_msg.get("destination", "")
    index = decoded.get("item_index", 0)
    owner = decoded.get("owner", "")

    if not nft_address:
        return None

    # Если owner не в decoded, пытаемся извлечь из других полей
    if not owner:
        owner = decoded.get("owner_address", "")

    # Metadata URI (может быть в decoded или отсутствовать)
    metadata_uri = decoded.get("content_url", "") or decoded.get("content", "")

    return MintEvent(
        nft_address=nft_address,
        collection_address=collection_address,
        index=int(index) if index else 0,
        owner_ton=owner,
        metadata_uri=metadata_uri,
        tx_hash=tx_hash,
        timestamp=timestamp,
    )


def _parse_transfer(
    tx_hash: str,
    timestamp: dt.datetime,
    in_msg: dict[str, Any],
    decoded: dict[str, Any],
) -> TransferEvent | None:
    """Извлекает данные transfer-события из транзакции."""
    nft_address = in_msg.get("destination", "")
    new_owner = decoded.get("new_owner", "")
    old_owner = in_msg.get("source", "")

    if not nft_address or not new_owner:
        return None

    return TransferEvent(
        nft_address=nft_address,
        old_owner_ton=old_owner or "",
        new_owner_ton=new_owner,
        tx_hash=tx_hash,
        timestamp=timestamp,
    )


def _normalize_addr(addr: str) -> str:
    """Нормализация адреса для сравнения (убираем пробелы, приводим к верхнему регистру)."""
    return addr.strip().upper()
