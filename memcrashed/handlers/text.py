#!/usr/bin/env python

from io import BytesIO

from tornado import gen

from memcrashed.parser import TextParser
from memcrashed.proxy import ProxyRepository


class TextProtocolHandler(object):
    EOL = b'\r\n'
    END = b'END' + EOL

    def __init__(self, io_loop):
        self.io_loop = io_loop
        self.parser = TextParser()
        self.pool_repository = ProxyRepository()

    @gen.engine
    def process(self, client_stream, backend_stream, callback):
        with BytesIO() as stream_data:
            header = yield gen.Task(self._process_request, stream_data, client_stream, backend_stream)

        with BytesIO() as stream_data:
            yield gen.Task(self._process_response, header, stream_data, backend_stream, client_stream)

        callback()

    @gen.engine
    def _process_request(self, stream_data, client_stream, backend_stream, callback):
        header_bytes = yield gen.Task(self._read_chunk_until_eol, client_stream, stream_data)
        header = self.parser.unpack_request_header(header_bytes)

        if self.parser.is_storage_command(header.command):
            bytes_to_read = self._extract_bytes_quantity(header_bytes, bytes_index=4)
            yield gen.Task(self._read_chunk_bytes, client_stream, stream_data, bytes_to_read)

        yield gen.Task(backend_stream.write, stream_data.getvalue())
        callback(header)

    @gen.engine
    def _process_response(self, header, stream_data, backend_stream, client_stream, callback):
        if self.parser.is_retrieval_command(header.command):
            yield gen.Task(self._read_retrieval_values, backend_stream, stream_data)
        else:
            yield gen.Task(self._read_chunk_until_eol, backend_stream, stream_data)

        yield gen.Task(client_stream.write, stream_data.getvalue())

        callback()

    @gen.engine
    def _read_retrieval_values(self, backend_stream, stream_data, callback):
        while True:
            header_bytes = yield gen.Task(self._read_chunk_until_eol, backend_stream, stream_data)
            if header_bytes != self.END:
                bytes_to_read = self._extract_bytes_quantity(header_bytes, bytes_index=3)
                yield gen.Task(self._read_chunk_bytes, backend_stream, stream_data, bytes_to_read)
            else:
                break

        callback()

    @gen.engine
    def _read_chunk_until_eol(self, stream, stream_data, callback):
        bytes_ = yield gen.Task(stream.read_until, self.EOL)
        stream_data.write(bytes_)
        callback(bytes_)

    @gen.engine
    def _read_chunk_bytes(self, stream, stream_data, bytes_to_read, callback):
        bytes_ = yield gen.Task(stream.read_bytes, bytes_to_read)
        stream_data.write(bytes_)
        callback(bytes_)

    def _extract_bytes_quantity(self, header_bytes, bytes_index):
        tokens = header_bytes[:-2].split(b' ')
        bytes_to_read = int(tokens[bytes_index]) + len(self.EOL)
        return bytes_to_read
