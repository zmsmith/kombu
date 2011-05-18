"""
kombu.transport.pyhaigha
========================

Haigha transport.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import socket

from haigha.channel import Channel as _Channel
from haigha.connection import Connection as _Connection
from haigha.message import Message as _Message


from kombu.transport import base

DEFAULT_PORT = 5672


class Message(base.Message):

    def __init__(self, channel, amqp_message, **kwargs):
        dinfo = amqp_message.delivery_info
        props = amqp_message.properties

        kwargs.update({"body": amqp_message.body,
                       "delivery_tag": dinfo.delivery_tag,
                       "content_type": props.content_type,
                       "content_encoding": props.content_encoding,
                       "headers": props.headers,
                       "delivery_info": dict(
                            consumer_tag=getattr(dinfo, "consumer_tag", None),
                            routing_key=dinfo.routing_key,
                            delivery_tag=dinfo.delivery_tag,
                            redelivered=dinfo.redelivered,
                            exchange=dinfo.exchange)})

        super(Message, self).__init__(channel, **kwargs)


class Channel(_Channel):
    Message = Message

    _proxy = {"basic": ("qos", "consume", "cancel", "publish", "get", "ack",
                        "reject", "recover"),
              "queue": ("declare", "bind", "unbind", "purge", "delete"),
              "exchange": ("declare", "delete"),
              "txn": ("select", "commit", "rollback")}

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)

        for cls_name, methods in self._proxy.iteritems():
            cls = getattr(self, cls_name)
            for method in methods:

                orig = getattr(cls, method)
                @wraps(orig)
                def _proxied(self, *args, **kwargs):
                    return orig(*args, **kwargs)
                setattr(self, "%s_%s" % (cls_name, method), _proxied)

    def prepare_message(self, body, priority=None,
            content_type=None, content_encoding=None, headers=None,
            properties=None):
        return _Message(body, priority=priority, content_type=content_type,
                              content_encoding=content_encoding,
                              application_headers=headers, **properties)

    def message_to_python(self, raw_message):
        return self.Message(channel=self, amqp_message=raw_message)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class Connection(_Connection):

    def drain_events(self):
        return event.dispatch()


class Transport(base.Transport):
    default_port = DEFAULT_PORT

    connection_errors = ()
    channel_errors = ()

    Message = Message
    Connection = Connection

    def __init__(self, client, **kwargs):
        self.client = client
        self.default_port = kwargs.get("default_port", self.default_port)

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return connection.drain_events(**kwargs)

    def establish_connection(self):
        """Establish connection to the AMQP broker."""
        conninfo = self.client
        if not conninfo.hostname:
            raise KeyError("Missing hostname for AMQP connection.")
        if conninfo.userid is None:
            conninfo.userid = "guest"
        if conninfo.password is None:
            conninfo.password = "guest"
        if not conninfo.port:
            conninfo.port = self.default_port

        return self.Connection(host=conninfo.hostname,
                               port=conninfo.port,
                               vhost=conninfo.virtual_host,
                               user=conninfo.userid,
                               password=conninfo.password,
                               heartbeat=None)

    def close_connection(self, connection):
        """Close the AMQP broker connection."""
        connection.close()
