# /workspace/tests/test_ton_listener.py — тесты TON Listener
"""Unit-тесты для services/ton/listener.py и parsing.py."""
from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest

from services.ton.events import MintEvent, TransferEvent
from services.ton.parsing import parse_tonviewer_transactions
from services.ton.state import TonState


# ---------------------------------------------------------------------------
# Тесты parsing.py
# ---------------------------------------------------------------------------

def test_parse_transfer_event():
    """Парсинг transfer-транзакции из Tonviewer."""
    transactions = [
        {
            "hash": "abc123",
            "time": 1699999999,
            "in_msg": {
                "source": "EQold_owner",
                "destination": "EQnft_address",
                "op": 1607361556,  # 0x5FCC3D14
                "decoded_body": {
                    "type": "nft_transfer",
                    "new_owner": "EQnew_owner",
                },
            },
        }
    ]

    events = parse_tonviewer_transactions(transactions, "EQcollection", seqno=0)

    assert len(events.transfers) == 1
    transfer = events.transfers[0]
    assert transfer.nft_address == "EQnft_address"
    assert transfer.old_owner_ton == "EQold_owner"
    assert transfer.new_owner_ton == "EQnew_owner"
    assert transfer.tx_hash == "abc123"


def test_parse_mint_event():
    """Парсинг mint-транзакции из Tonviewer."""
    transactions = [
        {
            "hash": "def456",
            "time": 1699999999,
            "in_msg": {
                "source": "EQcollection",
                "destination": "EQnew_nft",
                "op": 1,
                "decoded_body": {
                    "item_index": 42,
                    "owner": "EQowner",
                    "content_url": "ipfs://Qm123",
                },
            },
        }
    ]

    events = parse_tonviewer_transactions(transactions, "EQcollection", seqno=0)

    assert len(events.mints) == 1
    mint = events.mints[0]
    assert mint.nft_address == "EQnew_nft"
    assert mint.collection_address == "EQcollection"
    assert mint.index == 42
    assert mint.owner_ton == "EQowner"
    assert mint.metadata_uri == "ipfs://Qm123"


def test_parse_empty_transactions():
    """Пустой список транзакций."""
    events = parse_tonviewer_transactions([], "EQcollection", seqno=0)
    assert events.is_empty()


def test_parse_invalid_transaction():
    """Транзакция с некорректными данными не вызывает ошибку."""
    transactions = [
        {"hash": "invalid", "time": 0, "in_msg": None},
        {"no_hash": True},
    ]

    events = parse_tonviewer_transactions(transactions, "EQcollection", seqno=0)
    assert events.is_empty()


# ---------------------------------------------------------------------------
# Тесты state.py
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_state_save_and_load_timestamp(session):
    """Сохранение и загрузка timestamp."""
    state = TonState(session)

    # Изначально 0
    ts = await state.load_last_timestamp()
    assert ts == 0

    # Сохраняем
    await state.save_last_timestamp(1699999999)
    await session.commit()

    # Загружаем
    ts = await state.load_last_timestamp()
    assert ts == 1699999999


@pytest.mark.asyncio
async def test_state_upsert_mint_new(session):
    """Вставка нового NFT через mint."""
    state = TonState(session)

    mint = MintEvent(
        nft_address="EQtest_nft",
        collection_address="EQcollection",
        index=42,
        owner_ton="EQowner",
        metadata_uri="https://example.com/metadata.json",
        tx_hash="tx123",
        timestamp=dt.datetime.now(dt.timezone.utc),
    )

    metadata = {"attributes": [{"trait_type": "Model", "value": "Panda"}]}
    await state.upsert_mint(mint, metadata)
    await session.commit()

    # Проверяем что NFT создан
    from db.models import ProcessedNft
    from sqlalchemy import select

    stmt = select(ProcessedNft).where(ProcessedNft.nft_address == "EQtest_nft")
    nft = (await session.execute(stmt)).scalar_one()
    assert nft is not None
    assert nft.nft_index == 42
    assert nft.current_owner_ton == "EQowner"


@pytest.mark.asyncio
async def test_state_upsert_mint_update(session):
    """Обновление существующего NFT через повторный mint."""
    state = TonState(session)

    # Первый mint
    mint1 = MintEvent(
        nft_address="EQtest_nft",
        collection_address="EQcollection",
        index=42,
        owner_ton="EQowner1",
        metadata_uri="",
        tx_hash="tx1",
        timestamp=dt.datetime.now(dt.timezone.utc),
    )
    await state.upsert_mint(mint1)
    await session.commit()

    # Второй mint (обновление)
    mint2 = MintEvent(
        nft_address="EQtest_nft",
        collection_address="EQcollection",
        index=42,
        owner_ton="EQowner2",
        metadata_uri="",
        tx_hash="tx2",
        timestamp=dt.datetime.now(dt.timezone.utc),
    )
    await state.upsert_mint(mint2)
    await session.commit()

    # Проверяем что владелец обновлён
    from db.models import ProcessedNft
    from sqlalchemy import select

    stmt = select(ProcessedNft).where(ProcessedNft.nft_address == "EQtest_nft")
    nft = (await session.execute(stmt)).scalar_one()
    assert nft.current_owner_ton == "EQowner2"


@pytest.mark.asyncio
async def test_state_record_transfer(session):
    """Запись transfer в processed_nfts и nft_ownership_history."""
    state = TonState(session)

    # Сначала создаём NFT
    mint = MintEvent(
        nft_address="EQtest_nft",
        collection_address="EQcollection",
        index=42,
        owner_ton="EQowner1",
        metadata_uri="",
        tx_hash="tx1",
        timestamp=dt.datetime.now(dt.timezone.utc),
    )
    await state.upsert_mint(mint)
    await session.commit()

    # Transfer
    transfer = TransferEvent(
        nft_address="EQtest_nft",
        old_owner_ton="EQowner1",
        new_owner_ton="EQowner2",
        tx_hash="tx2",
        timestamp=dt.datetime.now(dt.timezone.utc),
    )
    await state.record_transfer(transfer)
    await session.commit()

    # Проверяем обновление владельца
    from db.models import ProcessedNft, NftOwnershipHistory
    from sqlalchemy import select

    stmt = select(ProcessedNft).where(ProcessedNft.nft_address == "EQtest_nft")
    nft = (await session.execute(stmt)).scalar_one()
    assert nft.current_owner_ton == "EQowner2"

    # Проверяем запись в истории
    stmt = select(NftOwnershipHistory).where(NftOwnershipHistory.nft_address == "EQtest_nft")
    history = (await session.execute(stmt)).scalar_one()
    assert history.owner_ton == "EQowner2"


# ---------------------------------------------------------------------------
# Интеграционный тест (с моками)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_listener_process_collection(session):
    """Интеграционный тест: обработка одной коллекции с моками HTTP."""
    from services.ton.listener import TonListener
    from db.models import Collection

    # Создаём тестовую коллекцию
    coll = Collection(
        name="Test Collection",
        contract_address="EQtest_collection",
        source="getgems",
    )
    session.add(coll)
    await session.commit()

    # Мокаем HTTP запросы
    mock_transactions = [
        {
            "hash": "tx1",
            "time": 1699999999,
            "in_msg": {
                "source": "EQold",
                "destination": "EQnft1",
                "op": 1607361556,
                "decoded_body": {"type": "nft_transfer", "new_owner": "EQnew"},
            },
        }
    ]

    # Создаём listener
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value=b"123-0")

    listener = TonListener(session=session, redis=redis_mock)

    # Мокаем _fetch_transactions
    listener._fetch_transactions = AsyncMock(return_value=mock_transactions)

    # Обрабатываем коллекцию
    await listener._process_collection(str(coll.id), "Test Collection", "EQtest_collection")

    # Проверяем что transfer обработан
    assert listener.stats["transfers"] == 1
    assert redis_mock.xadd.called  # Опубликовали в Stream
@pytest.mark.asyncio
async def test_listener_dedupe_no_repeat_processing(session):
    """Регрессия: повторный поллинг тех же транзакций не создаёт дублей и спама."""
    from sqlalchemy import select

    from db.models import Collection, NftOwnershipHistory
    from services.ton.listener import TonListener

    coll = Collection(name="Dedupe Coll", contract_address="EQdedupe", source="getgems")
    session.add(coll)
    await session.commit()

    tx = {
        "hash": "tx_dup",
        "time": 1699999999,
        "in_msg": {
            "source": "EQold",
            "destination": "EQnft_dup",
            "op": 1607361556,
            "decoded_body": {"type": "nft_transfer", "new_owner": "EQnew"},
        },
    }

    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value=b"1-0")

    listener = TonListener(session=session, redis=redis_mock)
    listener._fetch_transactions = AsyncMock(return_value=[tx])

    # Первый цикл: событие обработано
    await listener._process_collection(str(coll.id), "Dedupe Coll", "EQdedupe")
    assert listener.stats["transfers"] == 1
    assert redis_mock.xadd.call_count == 1

    # Второй цикл с теми же транзакциями: ничего не обрабатывается повторно
    await listener._process_collection(str(coll.id), "Dedupe Coll", "EQdedupe")
    assert listener.stats["transfers"] == 1
    assert redis_mock.xadd.call_count == 1  # нет спама в Stream

    # В истории владения ровно одна запись
    stmt = select(NftOwnershipHistory).where(NftOwnershipHistory.nft_address == "EQnft_dup")
    history = (await session.execute(stmt)).scalars().all()
    assert len(history) == 1
