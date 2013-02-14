#!/usr/bin/env python

import socket

from tornado import gen, iostream
from tornado.ioloop import IOLoop
from tornado.netutil import TCPServer

from memcrashed import parser


class Server(TCPServer):
    def __init__(self, io_loop=None, ssl_options=None):
        super(Server, self).__init__(io_loop, ssl_options)
        self.handler = BinaryProtocolHandler(self.io_loop)

    def handle_stream(self, stream, address):
        self.handler.process(stream)

    def set_handler(self, handler_type):
        self.handler = TextProtocolHandler(self.io_loop)


class BinaryProtocolHandler(object):
    HEADER_BYTES = 24

    def __init__(self, io_loop):
        self.io_loop = io_loop

    @gen.engine
    def process(self, stream):
        header_bytes = yield gen.Task(stream.read_bytes, self.HEADER_BYTES)

        headers = parser.unpack_request_header(header_bytes)
        body_bytes = yield gen.Task(stream.read_bytes, headers.total_body_length)

        all_bytes = header_bytes + body_bytes

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend = iostream.IOStream(s, io_loop=self.io_loop)
        yield gen.Task(backend.connect, ("127.0.0.1", 11211))

        yield gen.Task(backend.write, all_bytes)

        header_bytes = yield gen.Task(backend.read_bytes, self.HEADER_BYTES)

        headers = parser.unpack_response_header(header_bytes)
        body_bytes = yield gen.Task(backend.read_bytes, headers.total_body_length)

        all_bytes = header_bytes + body_bytes

        yield gen.Task(stream.write, all_bytes)


class TextProtocolHandler(object):
    END = '\r\n'

    def __init__(self, io_loop):
        self.io_loop = io_loop

    @gen.engine
    def process(self, stream):
        header_bytes = yield gen.Task(stream.read_until, self.END)

        body_bytes = yield gen.Task(stream.read_until, self.END)

        all_bytes = header_bytes + body_bytes

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend = iostream.IOStream(s, io_loop=self.io_loop)
        yield gen.Task(backend.connect, ("127.0.0.1", 11211))

        yield gen.Task(backend.write, all_bytes)

        print 'writing:', backend.writing()
        print 'reading:', backend.reading()

        header_bytes = yield gen.Task(backend.read_until, self.END)

        yield gen.Task(stream.write, header_bytes)


if __name__ == '__main__':  # pragma: no cover
    io_loop = IOLoop.instance()
    server = Server(io_loop=io_loop)
    server.listen(22322)
    io_loop.start()
