#!/usr/bin/env python

import argparse
import socket
import sys

from tornado import gen, iostream
from tornado.ioloop import IOLoop
from tornado.netutil import TCPServer

from memcrashed.handlers.binary import BinaryProtocolHandler
from memcrashed.handlers.text import TextProtocolHandler


class Server(TCPServer):
    def __init__(self, io_loop=None, ssl_options=None):
        super(Server, self).__init__(io_loop, ssl_options)
        self.handler = BinaryProtocolHandler(self.io_loop)
        self.backend = None

    def handle_stream(self, stream, address):
        self.ensure_backend()
        self._start_interaction(stream)

    @gen.engine
    def _start_interaction(self, stream):
        while not stream.closed():
            yield gen.Task(self.handler.process, stream, self.backend)

    def set_handler(self, handler_type):
        if handler_type == 'text':
            self.handler = TextProtocolHandler(self.io_loop)
        else:
            self.handler = BinaryProtocolHandler(self.io_loop)

    def create_backend(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend = iostream.IOStream(s, io_loop=self.io_loop)
        backend.connect(("127.0.0.1", 11211))
        return backend

    def ensure_backend(self):
        if self.backend is None:
            self.backend = self.create_backend()


def create_options_from_arguments(args):
    default_port = 22322
    default_address = 'localhost'
    parser = argparse.ArgumentParser(description="A Memcached sharding and failover proxy")
    parser.add_argument('-p', '--port', action='store', dest='port', default=22322, type=int,
                        help='Port in which the proxy will run. "{}" by default.'.format(default_port))
    parser.add_argument('-a', '--address', action='store', dest='address', default='localhost',
                        help='Address to which the proxy will be bound. "{}" by default.'.format(default_address))
    parser.add_argument('-t', '--text-protocol', action='store_true', dest='is_text_protocol', default=False,
                        help='If provided, will run over Memcache text protocol; Otherwise, runs over binary protocol (faster and more robust).')
    options = parser.parse_args(args)
    return options


def start_server(options):
    io_loop = IOLoop.instance()
    server = Server(io_loop=io_loop)
    if options.is_text_protocol:
        server.set_handler('text')
    server.listen(options.port, options.address)
    io_loop.start()


def main():
    options = create_options_from_arguments(sys.argv[1:])
    start_server(options)


if __name__ == '__main__':  # pragma: no cover
    main()
