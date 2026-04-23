from common import message_protocol, middleware
import config
from coordination import topology


class AggregationPublisher:
    def __init__(self):
        aggregation_routing_keys = [topology.aggregation_routing_key(aggregation_id) for aggregation_id in range(config.AGGREGATION_AMOUNT)]
        self._exchange = middleware.MessageMiddlewareExchangeRabbitMQ(config.MOM_HOST, config.AGGREGATION_PREFIX, aggregation_routing_keys)

    def send_partials(self, client_id, fruit_items):
        for item in fruit_items:
            data_msg = message_protocol.internal.DataMessage(client_id, item.fruit, item.amount)
            serialized_data = message_protocol.internal.serialize(data_msg.to_dict())
            aggregation_id = topology.aggregation_id_for(item.fruit)
            self._exchange.send_to(serialized_data, topology.aggregation_routing_key(aggregation_id))

    def send_eofs(self, client_id, total_records):
        eof_msg = message_protocol.internal.EOFMessage(client_id, total_records)
        serialized_eof = message_protocol.internal.serialize(eof_msg.to_dict())
        self._exchange.send(serialized_eof)

    def close(self):
        self._exchange.close()
