import pika
from pika.exceptions import AMQPConnectionError, AMQPError
from .middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=queue_name, durable=True)
            self._queue_name = queue_name
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def start_consuming(self, on_message_callback):
        def _callback(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)

        try:
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(queue=self._queue_name, on_message_callback=_callback)
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self._connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError() from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self._channel = self._connection.channel()
            self._exchange_name = exchange_name
            self._routing_keys = routing_keys

            self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue

            for routing_key in routing_keys:
                self._channel.queue_bind(exchange=exchange_name, queue=self._queue_name, routing_key=routing_key)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def start_consuming(self, on_message_callback):
        def _callback(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)

        try:
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(queue=self._queue_name, on_message_callback=_callback)
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
                )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self._connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError() from e
