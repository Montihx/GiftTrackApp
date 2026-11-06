# bot/msg_utils.py — общие помощники для обработчиков сообщений
from __future__ import annotations

from aiogram.types import Message, User as TgUser


def sender(msg: Message) -> TgUser:
    """Отправитель сообщения (в личных сообщениях from_user всегда есть)."""
    assert msg.from_user is not None
    return msg.from_user


def uid(msg: Message) -> int:
    return sender(msg).id
