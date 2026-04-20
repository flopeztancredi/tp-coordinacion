import json
from .internal_messages import DataMessage, EOFMessage, InternalMessageType, parse_message, ResultMessage


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
