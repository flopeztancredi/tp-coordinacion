from .messages import (
    DataMessage,
    EOFMessage,
    InternalMessageType,
    ResultMessage,
    SumCountUpdateMessage,
    SumEOFNoticeMessage,
    SumFlushMessage,
)


MESSAGE_TYPES = {
    DataMessage.message_type: DataMessage,
    EOFMessage.message_type: EOFMessage,
    ResultMessage.message_type: ResultMessage,
    SumEOFNoticeMessage.message_type: SumEOFNoticeMessage,
    SumCountUpdateMessage.message_type: SumCountUpdateMessage,
    SumFlushMessage.message_type: SumFlushMessage,
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
                total_records=payload["total_records"],
            )

        if message_class is SumEOFNoticeMessage:
            return SumEOFNoticeMessage(
                client_id=payload["client_id"],
                total_records=payload["total_records"],
                coordinator_id=payload["coordinator_id"],
            )

        if message_class is SumCountUpdateMessage:
            return SumCountUpdateMessage(
                client_id=payload["client_id"],
                sum_id=payload["sum_id"],
                processed_count=payload["processed_count"],
            )

        if message_class is SumFlushMessage:
            return SumFlushMessage(
                client_id=payload["client_id"],
                total_records=payload["total_records"],
            )

        return ResultMessage(
            client_id=payload["client_id"],
            fruit_top=payload["fruit_top"],
        )
    except KeyError as exc:
        raise ValueError(
            f"Missing field for internal message type '{msg_type}': {exc}"
        ) from exc
