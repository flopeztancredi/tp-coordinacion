import json

from .messages import (
    DataMessage,
    EOFMessage,
    InternalMessage,
    InternalMessageType,
    ResultMessage,
    SumCountUpdateMessage,
    SumEOFNoticeMessage,
    SumFlushMessage,
)
from .parser import MESSAGE_TYPES, parse_message


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
