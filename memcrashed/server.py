#!/usr/bin/env python

import socket

from tornado import gen, iostream
from tornado.ioloop import IOLoop
from tornado.netutil import TCPServer

from memcrashed import parser


HEADER_BYTES = 24


class Server(TCPServer):
    @gen.engine
    def handle_stream(self, stream, address):
        header_bytes = yield gen.Task(stream.read_bytes, HEADER_BYTES)

        headers = parser.unpack_request_header(header_bytes)
        body_bytes = yield gen.Task(stream.read_bytes, headers.total_body_length)

        all_bytes = header_bytes + body_bytes

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend = iostream.IOStream(s, io_loop=self.io_loop)
        yield gen.Task(backend.connect, ("127.0.0.1", 11211))

        yield gen.Task(backend.write, all_bytes)

        header_bytes = yield gen.Task(backend.read_bytes, HEADER_BYTES)

        headers = parser.unpack_response_header(header_bytes)
        body_bytes = yield gen.Task(backend.read_bytes, headers.total_body_length)

        all_bytes = header_bytes + body_bytes

        yield gen.Task(stream.write, all_bytes)


if __name__ == '__main__':  # pragma: no cover
    io_loop = IOLoop.instance()
    server = Server(io_loop=io_loop)
    server.listen(22322)
    io_loop.start()
