import logging
import threading

from common import message_protocol, middleware
import config
from coordination import topology
from coordination.publishers import AggregationPublisher, ControlPublisher


class SumControlPlane:
    def __init__(self, state):
        self._state = state
        self._ready = threading.Event()
        self._startup_error = None
        self._stopped = False
        self._closed = False
        self._control_publisher = None
        self._aggregation_publisher = None
        self._control_input_exchange = None

    def start(self):
        try:
            self._control_publisher = ControlPublisher()
            self._aggregation_publisher = AggregationPublisher()
            self._control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(config.MOM_HOST, config.SUM_CONTROL_EXCHANGE, [topology.sum_routing_key(config.ID)])
            self._ready.set()
            self._control_input_exchange.start_consuming(self.process_message)
        except Exception as exc:
            self._startup_error = exc
            self._ready.set()
            raise

    def wait_until_ready(self):
        self._ready.wait()
        if self._startup_error is not None:
            raise self._startup_error

    def stop(self):
        if self._stopped:
            return

        if self._control_input_exchange is None:
            return

        try:
            self._control_input_exchange.stop_consuming()
        except Exception:
            logging.exception("Failed to stop sum control consumer")
        self._stopped = True

    def close(self):
        if self._closed:
            return

        self._closed = True
        if self._control_input_exchange is not None:
            try:
                self._control_input_exchange.close()
            except Exception:
                logging.exception("Failed to close sum control input exchange")

        if self._control_publisher is not None:
            try:
                self._control_publisher.close()
            except Exception:
                logging.exception("Failed to close sum control publisher")

        if self._aggregation_publisher is not None:
            try:
                self._aggregation_publisher.close()
            except Exception:
                logging.exception("Failed to close sum aggregation publisher")

    def process_message(self, message, ack, nack):
        try:
            msg = self._parse_message(message)
            self._handle_message(msg)
            ack()
        except Exception:
            logging.exception("Failed to process sum control message")
            nack()

    def _parse_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return message_protocol.internal.parse_message(fields)

    def _handle_message(self, msg):
        if msg.message_type == message_protocol.internal.InternalMessageType.SUM_EOF_NOTICE:
            self._handle_eof_notice(msg.client_id, msg.total_records, msg.coordinator_id)
        elif msg.message_type == message_protocol.internal.InternalMessageType.SUM_COUNT_UPDATE:
            self._handle_count_update(msg.client_id, msg.sum_id, msg.processed_count)
        elif msg.message_type == message_protocol.internal.InternalMessageType.SUM_FLUSH:
            self._handle_flush(msg.client_id, msg.total_records)
        else:
            raise ValueError(f"Unknown control message type: {msg.message_type}")

    def _handle_eof_notice(self, client_id, total_records, coordinator_id):
        processed_count = self._state.start_closing(client_id, total_records, coordinator_id)
        self._send_count_update(client_id, coordinator_id, processed_count)

    def _handle_count_update(self, client_id, sender_sum_id, processed_count):
        assert self._control_publisher is not None
        total_records_to_flush = self._state.add_worker_count(client_id, sender_sum_id, processed_count)
        if total_records_to_flush is not None:
            flush_msg = message_protocol.internal.SumFlushMessage(client_id, total_records_to_flush)
            self._control_publisher.broadcast(flush_msg)

    def _handle_flush(self, client_id, total_records):
        assert self._aggregation_publisher is not None
        fruit_items = self._state.take_flush_items(client_id)
        logging.info("Flushing local sum state for client %s", client_id)
        self._aggregation_publisher.send_partials(client_id, fruit_items)
        self._aggregation_publisher.send_eofs(client_id, total_records)

    def _send_count_update(self, client_id, coordinator_id, processed_count):
        assert self._control_publisher is not None
        count_update = message_protocol.internal.SumCountUpdateMessage(client_id=client_id, sum_id=config.ID, processed_count=processed_count)
        self._control_publisher.send(coordinator_id, count_update)
