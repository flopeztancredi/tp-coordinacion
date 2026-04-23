import os
import logging
import heapq

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.partial_tops_by_client = {}
        self.result_counts = {}

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def process_messsage(self, message, ack, nack):
        try:
            msg = self._parse_message(message)
            self._handle_message(msg)
            ack()
        except Exception:
            logging.exception("Failed to process join message")
            nack()

    def _parse_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return message_protocol.internal.parse_message(fields)

    def _handle_message(self, msg):
        if msg.message_type == message_protocol.internal.InternalMessageType.RESULT:
            self._process_result(msg.client_id, msg.fruit_top)
        else:
            raise ValueError(f"Unknown message type: {msg.message_type}")

    def _process_result(self, client_id, partial_top):
        logging.info(f"Processing result for client {client_id}")

        if client_id not in self.partial_tops_by_client:
            self.partial_tops_by_client[client_id] = []

        heap = self.partial_tops_by_client[client_id]
        for fruit, amount in partial_top:
            if len(heap) < TOP_SIZE:
                heapq.heappush(heap, (amount, fruit))
            elif amount > heap[0][0]:
                heapq.heapreplace(heap, (amount, fruit))

        self.result_counts[client_id] = self.result_counts.get(client_id, 0) + 1
        result_count = self.result_counts[client_id]
        logging.info(f"Received result {result_count}/{AGGREGATION_AMOUNT} for client {client_id}")

        if result_count == AGGREGATION_AMOUNT:
            final_top = sorted([(f, a) for a, f in heap], key=lambda x: x[1], reverse=True)
            self._send_result(client_id, final_top)
            del self.partial_tops_by_client[client_id]
            del self.result_counts[client_id]

    def _send_result(self, client_id, final_top):
        result_msg = message_protocol.internal.ResultMessage(client_id, final_top)
        serialized_result = message_protocol.internal.serialize(result_msg.to_dict())
        self.output_queue.send(serialized_result)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    return 0


if __name__ == "__main__":
    main()
