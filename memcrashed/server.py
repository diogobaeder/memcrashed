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
        print('SERVER: handle_stream called')

        header_bytes = yield gen.Task(stream.read_bytes, HEADER_BYTES)

        print('SERVER: request header_bytes received')

        headers = parser.unpack_request_header(header_bytes)
        body_bytes = yield gen.Task(stream.read_bytes, headers.total_body_length)

        print('SERVER: request body_bytes received')

        all_bytes = header_bytes + body_bytes

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend = iostream.IOStream(s, io_loop=self.io_loop)
        yield gen.Task(backend.connect, ("127.0.0.1", 11211))

        print('SERVER: connected to the backend')

        yield gen.Task(backend.write, all_bytes)

        print('SERVER: wrote all_bytes to the backend')

        header_bytes = yield gen.Task(backend.read_bytes, HEADER_BYTES)

        print('SERVER: read header_bytes from the backend')

        headers = parser.unpack_response_header(header_bytes)
        body_bytes = yield gen.Task(backend.read_bytes, headers.total_body_length)

        print('SERVER: read body_bytes from the backend')

        all_bytes = header_bytes + body_bytes

        yield gen.Task(stream.write, all_bytes)

        print('SERVER: wrote all_bytes back to the stream')


if __name__ == '__main__':  # pragma: no cover
    server = Server()
    server.listen(8888)
    IOLoop.instance().start()
