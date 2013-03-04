#!/usr/bin/env python

from io import BytesIO

from tornado import gen

from memcrashed.parser import BinaryParser
from memcrashed.proxy import ProxyRepository


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
        self.pool_repository = ProxyRepository()

    @gen.engine
    def process(self, client_stream, backend_stream, callback):
        with BytesIO() as stream_data:
            yield gen.Task(self._read_full_chunk, self.parser.unpack_request_header, stream_data, client_stream)
            yield gen.Task(backend_stream.write, stream_data.getvalue())

        with BytesIO() as stream_data:
            yield gen.Task(self._read_full_chunk, self.parser.unpack_response_header, stream_data, backend_stream)
            yield gen.Task(client_stream.write, stream_data.getvalue())

        callback()

    @gen.engine
    def _read_full_chunk(self, unpack, stream_data, stream, callback):
        while True:
            headers = yield gen.Task(self._read_chunk, stream, stream_data, unpack)
            if headers.opcode not in self.QUIET_OPS:
                break
        callback()

    @gen.engine
    def _read_chunk(self, stream, stream_data, unpack, callback):
        header_bytes = yield gen.Task(stream.read_bytes, self.HEADER_BYTES)
        stream_data.write(header_bytes)
        headers = unpack(header_bytes)
        if headers.total_body_length > 0:
            body_bytes = yield gen.Task(stream.read_bytes, headers.total_body_length)
            stream_data.write(body_bytes)
        callback(headers)
