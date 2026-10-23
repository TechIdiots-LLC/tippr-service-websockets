import json
import logging
import socket

import amqp
import gevent


LOG = logging.getLogger(__name__)

# how long one wait for incoming frames lasts before the loop checks the
# connection again (the wait itself yields to other greenlets)
DRAIN_TIMEOUT = 1


class MessageSource(object):
    """An AMQP based message source.

    This will monitor a fanout exchange on AMQP and signal on receipt of any
    messages, as well as allow messages to be sent to a topic exchange on
    status changes within the system.

    It uses py-amqp, whose sockets gevent's monkey-patching makes
    cooperative: pika's BlockingConnection runs its own poll loop, which
    never finished connecting under gevent, so no messages arrived.

    """

    def __init__(self, config):
        assert config.endpoint.family == socket.AF_INET

        self.host = config.endpoint.address.host
        self.port = config.endpoint.address.port
        self.vhost = config.vhost
        self.username = config.username
        self.password = config.password
        self.broadcast_exchange = config.exchange.broadcast
        self.status_exchange = config.exchange.status
        self.send_status_messages = config.send_status_messages
        self.message_handler = None

        self.connection = None
        self.channel = None
        self.publish_channel = None

    def _connect(self):
        self.connection = amqp.Connection(
            host="%s:%d" % (self.host, self.port),
            userid=self.username,
            password=self.password,
            virtual_host=self.vhost,
        )
        self.connection.connect()

        # Publisher channel (use a dedicated channel for publishing)
        self.publish_channel = self.connection.channel()

        # Consumer channel: a queue of our own on the fanout exchange (as
        # r2 declares it: not durable, not auto-deleted)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.broadcast_exchange, type="fanout",
            durable=False, auto_delete=False)
        queue_name, _, _ = self.channel.queue_declare(
            queue="", exclusive=True, auto_delete=True)
        self.channel.queue_bind(queue=queue_name,
                                exchange=self.broadcast_exchange)
        self.channel.basic_consume(queue=queue_name,
                                   callback=self._on_message, no_ack=True)

    @property
    def connected(self):
        return bool(self.connection)

    def _on_message(self, message):
        if self.message_handler:
            body = message.body
            decoded = (body.decode("utf-8")
                       if isinstance(body, (bytes, bytearray)) else body)
            namespace = message.delivery_info["routing_key"]
            self.message_handler(namespace=namespace, message=decoded)

    def _on_close(self):
        LOG.warning("lost connection")
        try:
            if self.connection:
                self.connection.collect()
        except Exception:
            pass
        self.connection = None
        self.channel = None
        self.publish_channel = None

    def send_message(self, key, payload):
        """Publish a status update to the status exchange."""
        if self.send_status_messages and self.publish_channel:
            serialized_payload = json.dumps(payload).encode("utf-8")
            try:
                self.publish_channel.basic_publish(
                    amqp.Message(serialized_payload),
                    exchange=self.status_exchange,
                    routing_key=key,
                )
            except Exception:
                LOG.exception("failed to publish message")

    def pump_messages(self):
        """Maintain a connection to the broker and handle incoming frames.

        This will never return, so it should be run from a separate greenlet.

        """
        while True:
            try:
                self._connect()
                LOG.info("connected")

                while self.connected:
                    try:
                        self.connection.drain_events(timeout=DRAIN_TIMEOUT)
                    except socket.timeout:
                        pass
                    gevent.sleep()
            except (socket.error, OSError, amqp.exceptions.AMQPError) as exc:
                LOG.warning("connection failed: %s", exc)
            self._on_close()
            gevent.sleep(1)
