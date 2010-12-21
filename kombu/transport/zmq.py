"""
A zeromq transport for kombu. This is incredibly bare-bones for starters, and
doesn't support ANY features beyond message pushing. No queues, no routing, no
priorities, nada, zilch.

To use, do something like:

    conn = BrokerConnection(transport='zeromq',
                            hostname='tcp://localhost')

    conn = BrokerConnection(transport='zeromq',
                            hostname='tcp://localhost:10112')

    conn = BrokerConnection(transport='pyzmq',
                            hostname='ipc:///tmp/zeromqtest')
"""

from __future__ import absolute_import


import zmq
from anyjson import serialize, deserialize
from kombu.transport import virtual

__author__ = 'storborg@mit.edu'

DEFAULT_PORT = 10111


class Connection(object):

    def __init__(self, connectstring, port=None):
        """
        To have the flexibility to use e.g. sockets and the like, we're using
        the BrokerConnection ``host`` parameter as a generic connection string.
        But, to use with tcp, we need to parse it and grab the right fields.
        """
        transport, address = connectstring.split('://')
        if transport == 'tcp':
            if ':' in address:
                host, port = address.split(':')
            else:
                host, port = address, (port or DEFAULT_PORT)
            self.push_endpoint = 'tcp://*:%d' % port
            self.pull_endpoint = 'tcp://%s:%d' % (host, port)
        elif transport in ('pgm', 'epgm'):
            raise NotImplementedError("no multicast yet, sorry")
        else:
            self.push_endpoint = self.pull_endpoint = connectstring

    def connect(self):
        context = zmq.Context()

        # Start one PUSH socket listening.
        self.push_socket = context.socket(zmq.PUSH)
        self.push_socket.bind(self.push_endpoint)

        # Start one PULL socket, and connect it. Eventually this would probably
        # be connecting to several other PUSH sockets to get data from multiple
        # sources.
        self.pull_socket = context.socket(zmq.PULL)
        self.pull_socket.connect(self.pull_endpoint)

    def put(self, buf):
        self.push_socket.send(buf)

    def get(self):
        return self.pull_socket.recv()


class Channel(virtual.Channel):
    _client = None

    def _put(self, queue, message, **kwargs):
        self.client.put(serialize(message))

    def _get(self, queue):
        return deserialize(self.client.get())

    def _purge(self, queue):
        raise NotImplementedError("does not implement _purge")

    def _size(self, queue):
        return 0

    def _open(self):
        conninfo = self.connection.client
        conn = Connection(conninfo.hostname, conninfo.port)
        conn.connect()
        return conn

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(virtual.Transport):
    Channel = Channel
