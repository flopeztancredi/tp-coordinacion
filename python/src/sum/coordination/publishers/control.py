from common import message_protocol, middleware
import config
from coordination import topology


class ControlPublisher:
    def __init__(self):
        self._exchange = middleware.MessageMiddlewareExchangeRabbitMQ(config.MOM_HOST, config.SUM_CONTROL_EXCHANGE, topology.sum_routing_keys())

    def broadcast(self, message):
        serialized_msg = message_protocol.internal.serialize(message.to_dict())
        self._exchange.send(serialized_msg)

    def send(self, sum_id, message):
        serialized_msg = message_protocol.internal.serialize(message.to_dict())
        self._exchange.send_to(serialized_msg, topology.sum_routing_key(sum_id))

    def close(self):
        self._exchange.close()
