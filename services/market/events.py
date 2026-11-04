# /workspace/services/market/events.py — DTO событий листингов
"""Data Transfer Objects для событий листингов на маркетплейсах."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ListingEvent:
    """Событие нового листинга NFT на маркетплейсе."""

    nft_address: str
    nft_index: int
    collection_address: str
    marketplace: str  # getgems | mrkt | tonnel | portals
    price_ton: float
    seller_ton: str
    attributes: dict[str, str]  # {"model": "Obelisk", "background": "Silver Blue", ...}
    listed_at: dt.datetime
    tx_hash: str | None = None


def listing_to_stream_payload(event: ListingEvent) -> dict[str, Any]:
    """Преобразование ListingEvent в payload для Redis Stream STREAM_NFT_LISTINGS."""
    return {
        "type": "listing",
        "nft_address": event.nft_address,
        "nft_index": event.nft_index,
        "collection_address": event.collection_address,
        "marketplace": event.marketplace,
        "price_ton": event.price_ton,
        "seller_ton": event.seller_ton,
        "attributes": event.attributes,
        "listed_at": event.listed_at.isoformat(),
        "tx_hash": event.tx_hash,
    }
