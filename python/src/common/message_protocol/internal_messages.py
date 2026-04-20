from dataclasses import dataclass
from enum import Enum
from typing import ClassVar


class InternalMessageType(str, Enum):
    DATA = "data"
    EOF = "eof"
    RESULT = "result"


@dataclass
class InternalMessage:
    message_type: ClassVar[InternalMessageType]

    def to_dict(self):
        raise NotImplementedError


@dataclass
class DataMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.DATA

    client_id: str
    fruit: str
    amount: int

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "fruit": self.fruit,
            "amount": self.amount,
        }


@dataclass
class EOFMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.EOF

    client_id: str

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
        }


@dataclass
class ResultMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.RESULT

    client_id: str
    fruit_top: list[tuple[str, int]]

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "fruit_top": self.fruit_top,
        }


MESSAGE_TYPES = {
    DataMessage.message_type: DataMessage,
    EOFMessage.message_type: EOFMessage,
    ResultMessage.message_type: ResultMessage,
}


def parse_message(payload):
    if not isinstance(payload, dict):
        raise TypeError("Internal message payload must be a dict")

    msg_type_value = payload.get("type")
    if msg_type_value is None:
        raise ValueError("Internal message is missing 'type' field")

    try:
        msg_type = InternalMessageType(msg_type_value)
    except ValueError as exc:
        raise ValueError(f"Unknown internal message type: {msg_type_value}") from exc

    message_class = MESSAGE_TYPES.get(msg_type)
    if message_class is None:
        raise ValueError(f"Unknown internal message type: {msg_type}")

    try:
        if message_class is DataMessage:
            return DataMessage(
                client_id=payload["client_id"],
                fruit=payload["fruit"],
                amount=payload["amount"],
            )

        if message_class is EOFMessage:
            return EOFMessage(
                client_id=payload["client_id"],
            )

        return ResultMessage(
            client_id=payload["client_id"],
            fruit_top=payload["fruit_top"],
        )
    except KeyError as exc:
        raise ValueError(
            f"Missing field for internal message type '{msg_type}': {exc}"
        ) from exc
