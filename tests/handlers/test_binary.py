import binascii
from unittest import skipUnless

from mock import MagicMock
from nose.tools import istest
from tornado import iostream

from memcrashed.handlers.binary import BinaryProtocolHandler
from ..utils import pylibmc, PYLIBMC_EXISTS, PYLIBMC_SKIP_REASON, server_running, ServerTestCase


class BinaryProtocolHandlerTest(ServerTestCase):
    @istest
    @skipUnless(PYLIBMC_EXISTS, PYLIBMC_SKIP_REASON)
    def stores_a_value_successfully(self):
        host = 'localhost'
        port = 22322

        with server_running(host, port):
            server = '{}:{}'.format(host, port)
            client = pylibmc.Client([server], binary=True)

            self.assertTrue(client.set('foo', 'bar'))

    @istest
    @skipUnless(PYLIBMC_EXISTS, PYLIBMC_SKIP_REASON)
    def stores_a_value_twice_without_error(self):
        host = 'localhost'
        port = 22322

        with server_running(host, port):
            server = '{}:{}'.format(host, port)
            client = pylibmc.Client([server], binary=True)

            client.set('foo', 'bar')
            self.assertTrue(client.set('foo', 'bar'))

    @istest
    @skipUnless(PYLIBMC_EXISTS, PYLIBMC_SKIP_REASON)
    def gets_a_value_successfully(self):
        host = 'localhost'
        port = 22322

        with server_running(host, port):
            server = '{}:{}'.format(host, port)
            client = pylibmc.Client([server], binary=True)

            client.set('foo', 'bar')
            self.assertEqual(client.get('foo'), 'bar')

    @istest
    @skipUnless(PYLIBMC_EXISTS, PYLIBMC_SKIP_REASON)
    def gets_a_mapping_successfully(self):
        host = 'localhost'
        port = 22322

        with server_running(host, port):
            server = '{}:{}'.format(host, port)
            client = pylibmc.Client([server], binary=True)

            client.set('foo', 'bar')
            client.set('foo2', 'bar2')
            self.assertEqual(client.get_multi(['foo', 'foo2']), {
                'foo': 'bar',
                'foo2': 'bar2',
            })

    @istest
    def gets_multiple_values(self):
        self.maxDiff = None
        protocol = BinaryProtocolHandler('some ioloop')

        overall_calls = []
        client_stream = MockStream(overall_calls, 'client_stream')
        backend_stream = MockStream(overall_calls, 'backend_stream')
        client_requests_hex = [
            b'800d00030000000000000003000000000000000000000000',
            b'666f6f',
            b'800d00040000000000000004000000000000000000000000',
            b'666f6f32',
            b'800a00000000000000000000000000000000000000000000',
        ]
        backend_responses_hex = [
            b'810d0003040000000000000a000000000000000000000006',
            b'00000000666f6f626172',
            b'810d0004040000000000000c000000000000000000000007',
            b'00000000666f6f3262617232',
            b'810a00000000000000000000000000000000000000000000',
        ]
        client_requests = [binascii.unhexlify(bytes_) for bytes_ in client_requests_hex]
        backend_responses = [binascii.unhexlify(bytes_) for bytes_ in backend_responses_hex]

        client_stream.mock_stream.read_bytes.side_effect = client_requests
        backend_stream.mock_stream.read_bytes.side_effect = backend_responses

        expected_overall_calls = [
            (client_stream, 'read_bytes', client_requests_hex[0]),
            (client_stream, 'read_bytes', client_requests_hex[1]),
            (client_stream, 'read_bytes', client_requests_hex[2]),
            (client_stream, 'read_bytes', client_requests_hex[3]),
            (client_stream, 'read_bytes', client_requests_hex[4]),

            (backend_stream, 'write', b''.join(client_requests_hex[0:5])),

            (backend_stream, 'read_bytes', backend_responses_hex[0]),
            (backend_stream, 'read_bytes', backend_responses_hex[1]),
            (backend_stream, 'read_bytes', backend_responses_hex[2]),
            (backend_stream, 'read_bytes', backend_responses_hex[3]),
            (backend_stream, 'read_bytes', backend_responses_hex[4]),

            (client_stream, 'write', b''.join(backend_responses_hex[0:5])),
        ]

        def finish_test():
            try:
                self.assertEqual(overall_calls, expected_overall_calls)
            finally:
                self.stop()

        protocol.process(client_stream, backend_stream, finish_test)
        self.wait(timeout=1)

    @istest
    def gets_noop_for_noop(self):
        self.maxDiff = None
        protocol = BinaryProtocolHandler('some ioloop')

        overall_calls = []
        client_stream = MockStream(overall_calls, 'client_stream')
        backend_stream = MockStream(overall_calls, 'backend_stream')
        client_request_hex = b'800a00000000000000000000000000000000000000000000'
        backend_response_hex = b'810a00000000000000000000000000000000000000000000'
        client_request = binascii.unhexlify(client_request_hex)
        backend_response = binascii.unhexlify(backend_response_hex)

        client_stream.mock_stream.read_bytes.return_value = client_request
        backend_stream.mock_stream.read_bytes.return_value = backend_response

        expected_overall_calls = [
            (client_stream, 'read_bytes', client_request_hex),
            (backend_stream, 'write', client_request_hex),
            (backend_stream, 'read_bytes', backend_response_hex),
            (client_stream, 'write', backend_response_hex),
        ]

        def finish_test():
            try:
                self.assertEqual(overall_calls, expected_overall_calls)
            finally:
                self.stop()

        protocol.process(client_stream, backend_stream, finish_test)
        self.wait(timeout=1)


class MockStream(object):
    def __init__(self, overall_calls, name):
        self.mock_stream = MagicMock(iostream.IOStream)
        self.overall_calls = overall_calls
        self.name = name

    def read_bytes(self, byte_quantity, callback):
        bytes_ = self.mock_stream.read_bytes(byte_quantity)
        self.overall_calls.append((self, 'read_bytes', binascii.hexlify(bytes_)))
        callback(bytes_)

    def write(self, bytes_, callback):
        self.overall_calls.append((self, 'write', binascii.hexlify(bytes_)))
        callback(self.mock_stream.write(bytes_))

    def __repr__(self):
        return self.name
