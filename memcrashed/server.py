#!/usr/bin/env python

import argparse
from io import BytesIO
import socket
import sys

from tornado import gen, iostream
from tornado.ioloop import IOLoop
from tornado.netutil import TCPServer

from memcrashed.parser import BinaryParser


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


class BinaryProtocolHandler(object):
    HEADER_BYTES = 24
    QUIET_OPS = (
        0x09,  # GetQ
        0x0d,  # GetKQ
        0x11,  # SetQ
        0x12,  # AddQ
        0x13,  # ReplaceQ
        0x14,  # DeleteQ
        0x15,  # IncrementQ
        0x16,  # DecrementQ
        0x17,  # QuitQ
        0x18,  # FlushQ
        0x19,  # AppendQ
        0x1a,  # PrependQ
        0x1e,  # GATQ
        0x32,  # RSetQ
        0x34,  # RAppendQ
        0x36,  # RPrependQ
        0x38,  # RDeleteQ
        0x3a,  # RIncrQ
        0x3c,  # RDecrQ
    )
    NO_OP = 0x0a

    def __init__(self, io_loop):
        self.io_loop = io_loop
        self.parser = BinaryParser()

    @gen.engine
    def process(self, stream, backend, callback):
        stream_buffer = BytesIO()

        header_bytes = yield gen.Task(stream.read_bytes, self.HEADER_BYTES)
        stream_buffer.write(header_bytes)
        headers = self.parser.unpack_request_header(header_bytes)
        if headers.total_body_length > 0:
            body_bytes = yield gen.Task(stream.read_bytes, headers.total_body_length)
            stream_buffer.write(body_bytes)

        if headers.opcode in self.QUIET_OPS:
            while headers.opcode is not self.NO_OP:
                header_bytes = yield gen.Task(stream.read_bytes, self.HEADER_BYTES)
                stream_buffer.write(header_bytes)
                headers = self.parser.unpack_request_header(header_bytes)
                if headers.total_body_length > 0:
                    body_bytes = yield gen.Task(stream.read_bytes, headers.total_body_length)
                    stream_buffer.write(body_bytes)

        yield gen.Task(backend.write, stream_buffer.getvalue())
        stream_buffer = BytesIO()

        header_bytes = yield gen.Task(backend.read_bytes, self.HEADER_BYTES)
        stream_buffer.write(header_bytes)
        headers = self.parser.unpack_response_header(header_bytes)
        if headers.total_body_length > 0:
            body_bytes = yield gen.Task(backend.read_bytes, headers.total_body_length)
            stream_buffer.write(body_bytes)

        if headers.opcode in self.QUIET_OPS:
            while headers.opcode is not self.NO_OP:
                header_bytes = yield gen.Task(backend.read_bytes, self.HEADER_BYTES)
                stream_buffer.write(header_bytes)
                headers = self.parser.unpack_response_header(header_bytes)
                if headers.total_body_length > 0:
                    body_bytes = yield gen.Task(backend.read_bytes, headers.total_body_length)
                    stream_buffer.write(body_bytes)

        yield gen.Task(stream.write, stream_buffer.getvalue())
        callback()


class TextProtocolHandler(object):
    END = b'\r\n'

    def __init__(self, io_loop):
        self.io_loop = io_loop

    @gen.engine
    def process(self, stream, backend, callback):
        header_bytes = yield gen.Task(stream.read_until, self.END)

        body_bytes = yield gen.Task(stream.read_until, self.END)

        all_bytes = header_bytes + body_bytes

        yield gen.Task(backend.write, all_bytes)

        header_bytes = yield gen.Task(backend.read_until, self.END)

        yield gen.Task(stream.write, header_bytes)
        callback()


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
