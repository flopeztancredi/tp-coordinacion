import os
import logging
import signal
import bisect

from common import middleware, message_protocol, fruit_item


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self._stopped = False
        self._closed = False
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"])
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.partials_by_client = {}
        self.eof_counts = {}

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def _handle_sigterm(self, *_):
        logging.info("SIGTERM received, shutting down aggregation gracefully")
        self.stop()

    def stop(self):
        if self._stopped:
            return

        try:
            self.input_exchange.stop_consuming()
        except Exception:
            logging.exception("Failed to stop aggregation consumer")
        self._stopped = True

    def close(self):
        if self._closed:
            return

        self._closed = True
        try:
            self.input_exchange.close()
        except Exception:
            logging.exception("Failed to close aggregation input exchange")

        try:
            self.output_queue.close()
        except Exception:
            logging.exception("Failed to close aggregation output queue")

    def process_messsage(self, message, ack, nack):
        try:
            msg = self._parse_message(message)
            self._handle_message(msg)
            ack()
        except Exception:
            logging.exception("Failed to process aggregation message")
            nack()

    def _parse_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return message_protocol.internal.parse_message(fields)

    def _handle_message(self, msg):
        if msg.message_type == message_protocol.internal.InternalMessageType.DATA:
            self._process_data(msg.client_id, msg.fruit, msg.amount)
        elif msg.message_type == message_protocol.internal.InternalMessageType.EOF:
            self._process_eof(msg.client_id)
        else:
            raise ValueError(f"Unknown message type: {msg.message_type}")

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        partials = self.partials_by_client.setdefault(client_id, [])

        for i in range(len(partials)):
            if partials[i].fruit == fruit:
                partials[i] = partials[i] + fruit_item.FruitItem(fruit, amount)
                return
        bisect.insort(partials, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        self.eof_counts[client_id] = self.eof_counts.get(client_id, 0) + 1
        eof_count = self.eof_counts[client_id]
        logging.info(f"Received EOF for client {client_id} ({eof_count}/{SUM_AMOUNT})")

        if eof_count < SUM_AMOUNT:
            return

        partials = self.partials_by_client.get(client_id, [])
        top = self._calculate_top(partials)
        self._send_result(client_id, top)
        self.partials_by_client.pop(client_id, None)
        self.eof_counts.pop(client_id, None)

    def _send_result(self, client_id, top):
        result_msg = message_protocol.internal.ResultMessage(client_id, top)
        serialized_result = message_protocol.internal.serialize(result_msg.to_dict())
        self.output_queue.send(serialized_result)

    def _calculate_top(self, fruit_items):
        top_items = list(fruit_items[-TOP_SIZE:])
        top_items.reverse()
        return [(item.fruit, item.amount) for item in top_items]


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, aggregation_filter._handle_sigterm)

    try:
        aggregation_filter.start()
    except Exception:
        logging.exception("Aggregation error")
    finally:
        aggregation_filter.stop()
        aggregation_filter.close()
        logging.info("Aggregation filter shut down")

    return 0


if __name__ == "__main__":
    main()
