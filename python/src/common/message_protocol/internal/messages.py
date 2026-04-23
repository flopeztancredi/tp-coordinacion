from dataclasses import dataclass
from enum import Enum
from typing import ClassVar


class InternalMessageType(str, Enum):
    DATA = "data"
    EOF = "eof"
    RESULT = "result"
    SUM_EOF_NOTICE = "sum_eof_notice"
    SUM_COUNT_UPDATE = "sum_count_update"
    SUM_FLUSH = "sum_flush"


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
    total_records: int

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "total_records": self.total_records,
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


@dataclass
class SumEOFNoticeMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.SUM_EOF_NOTICE

    client_id: str
    total_records: int
    coordinator_id: int

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "total_records": self.total_records,
            "coordinator_id": self.coordinator_id,
        }


@dataclass
class SumCountUpdateMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.SUM_COUNT_UPDATE

    client_id: str
    sum_id: int
    processed_count: int

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "sum_id": self.sum_id,
            "processed_count": self.processed_count,
        }


@dataclass
class SumFlushMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.SUM_FLUSH

    client_id: str
    total_records: int

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "total_records": self.total_records,
        }
