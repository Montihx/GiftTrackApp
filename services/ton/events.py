# /workspace/services/ton/events.py — DTO событий mint/transfer
"""Data Transfer Objects для событий блокчейна TON."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MintEvent:
    """Событие создания нового NFT (mint)."""

    nft_address: str
    collection_address: str
    index: int
    owner_ton: str
    metadata_uri: str
    tx_hash: str
    timestamp: dt.datetime


@dataclass(frozen=True, slots=True)
class TransferEvent:
    """Событие перевода NFT между владельцами."""

    nft_address: str
    old_owner_ton: str
    new_owner_ton: str
    tx_hash: str
    timestamp: dt.datetime


@dataclass(slots=True)
class BlockEvents:
    """Все события из одного блока (или батча транзакций от Tonviewer)."""

    seqno: int  # номер блока или timestamp последнего запроса
    mints: list[MintEvent]
    transfers: list[TransferEvent]

    def __init__(self, seqno: int) -> None:
        self.seqno = seqno
        self.mints = []
        self.transfers = []

    def add_mint(self, event: MintEvent) -> None:
        self.mints.append(event)

    def add_transfer(self, event: TransferEvent) -> None:
        self.transfers.append(event)

    def is_empty(self) -> bool:
        return not self.mints and not self.transfers


def mint_to_stream_payload(event: MintEvent, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    """Преобразование MintEvent в payload для Redis Stream STREAM_NFT_UPGRADES."""
    payload: dict[str, Any] = {
        "type": "mint",
        "nft_address": event.nft_address,
        "collection_address": event.collection_address,
        "index": event.index,
        "owner_ton": event.owner_ton,
        "metadata_uri": event.metadata_uri,
        "tx_hash": event.tx_hash,
        "timestamp": event.timestamp.isoformat(),
    }
    if metadata:
        payload["metadata"] = metadata
    return payload


def transfer_to_stream_payload(event: TransferEvent) -> dict[str, Any]:
    """Преобразование TransferEvent в payload для Redis Stream STREAM_NFT_UPGRADES."""
    return {
        "type": "transfer",
        "nft_address": event.nft_address,
        "old_owner_ton": event.old_owner_ton,
        "new_owner_ton": event.new_owner_ton,
        "tx_hash": event.tx_hash,
        "timestamp": event.timestamp.isoformat(),
    }
