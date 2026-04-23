import logging

from common import message_protocol, middleware
import config
from coordination.publishers import ControlPublisher


class SumDataPlane:
    def __init__(self, state):
        self._state = state
        self._stopped = False
        self._closed = False
        self._input_queue = middleware.MessageMiddlewareQueueRabbitMQ(config.MOM_HOST, config.INPUT_QUEUE)
        self._control_publisher = ControlPublisher()

    def start(self):
        self._input_queue.start_consuming(self.process_message)

    def stop(self):
        if self._stopped:
            return
        self._stopped = True

        try:
            self._input_queue.stop_consuming()
        except Exception:
            logging.exception("Failed to stop sum data consumer")

    def close(self):
        if self._closed:
            return

        self._closed = True
        try:
            self._input_queue.close()
        except Exception:
            logging.exception("Failed to close sum data input queue")

        try:
            self._control_publisher.close()
        except Exception:
            logging.exception("Failed to close sum data control publisher")

    def process_message(self, message, ack, nack):
        try:
            msg = self._parse_message(message)
            self._handle_message(msg)
            ack()
        except Exception:
            logging.exception("Failed to process sum data message")
            nack()

    def _parse_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return message_protocol.internal.parse_message(fields)

    def _handle_message(self, msg):
        if msg.message_type == message_protocol.internal.InternalMessageType.DATA:
            self._process_data(msg.client_id, msg.fruit, msg.amount)
        elif msg.message_type == message_protocol.internal.InternalMessageType.EOF:
            self._process_eof(msg.client_id, msg.total_records)
        else:
            raise ValueError(f"Unknown input message type: {msg.message_type}")

    def _process_data(self, client_id, fruit, amount):
        count_to_report = self._state.add_data(client_id, fruit, amount)
        if count_to_report is not None:
            processed_count, coordinator_id = count_to_report
            self._send_count_update(client_id, coordinator_id, processed_count)

    def _process_eof(self, client_id, total_records):
        self._state.start_closing(client_id, total_records, config.ID)
        eof_notice = message_protocol.internal.SumEOFNoticeMessage(client_id=client_id, total_records=total_records, coordinator_id=config.ID)
        self._control_publisher.broadcast(eof_notice)

    def _send_count_update(self, client_id, coordinator_id, processed_count):
        count_update = message_protocol.internal.SumCountUpdateMessage(client_id=client_id, sum_id=config.ID, processed_count=processed_count)
        self._control_publisher.send(coordinator_id, count_update)
