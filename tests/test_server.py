import binascii
from contextlib import contextmanager
import os
import socket
import subprocess
import sys
from unittest import TestCase, skipUnless
import time

import memcache
from mock import patch, MagicMock, ANY
from nose.tools import istest
from tornado import iostream
from tornado.testing import AsyncTestCase

from memcrashed.server import Server, BinaryProtocolHandler, TextProtocolHandler, create_options_from_arguments, start_server, main

try:
    import pylibmc
except ImportError:
    PYLIBMC_EXISTS = False
else:
    PYLIBMC_EXISTS = True
PYLIBMC_SKIP_REASON = "Can't run in Python 3 because pylibmc is not yet ported."


PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))


@contextmanager
def server_running(host, port, args=[]):
    server_path = os.path.join(PROJECT_ROOT, 'memcrashed', 'server.py')
    command_args = [
        sys.executable,
        server_path,
        '-p', str(port),
        '-a', host,
    ]
    command_args.extend(args)
    env = {
        'PYTHONPATH': PROJECT_ROOT,
    }
    proc = subprocess.Popen(command_args, env=env)
    time.sleep(0.2)
    try:
        yield
    finally:
        proc.kill()


@contextmanager
def proxy_memcached(client):
    recv_results = []
    sent_messages = []
    host = client.buckets[0]
    orig_recv = host.recv
    orig_readline = host.readline
    orig_send_cmd = host.send_cmd

    def recv(*args, **kwargs):
        result = orig_recv(*args, **kwargs)
        recv_results.append(result.strip())
        return result

    def readline():
        result = orig_readline()
        recv_results.append(result.strip())
        return result

    def send_cmd(cmd):
        sent_messages.append(cmd)
        return orig_send_cmd(cmd)

    with patch.object(host, 'recv', recv), patch.object(host, 'send_cmd', send_cmd), patch.object(host, 'readline', readline):
        yield (recv_results, sent_messages)


class ServerTestCase(AsyncTestCase):
    def setUp(self):
        super(ServerTestCase, self).setUp()
        host = '127.0.0.1'
        port = 11211
        self.memcached_client = memcache.Client(['%s:%s' % (host, port)])
        self.memcached_client.flush_all()
        self.memcached_client.disconnect_all()


class SmokeTest(AsyncTestCase):
    @istest
    def checks_for_backend_memcached(self):
        host = '127.0.0.1'
        port = 11211

        client = memcache.Client(['%s:%s' % (host, port)])
        data = client.get_stats()
        client.disconnect_all()

        self.assertIsNotNone(data)


class ServerTest(ServerTestCase):
    def response_without_cas(self, data):
        return data[:-8]

    @istest
    def reads_back_a_written_value(self):
        '''
        This test uses low-level sockets to check if the Server is respecting the protocol when setting a value from memcached.
        '''
        host = '127.0.0.1'
        port = 22322

        request_bytes = binascii.unhexlify(b'80010003080000000000000e0000000000000000000000000000000000000000666f6f626172')
        response_bytes = binascii.unhexlify(b'81010000000000000000000000000000000000000000012b')

        server = Server(io_loop=self.io_loop)
        server.listen(port, address=host)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stream = iostream.IOStream(s, io_loop=self.io_loop)

        def start_test():
            stream.connect((host, port), send_request)

        def send_request():
            stream.write(request_bytes, write_finished)

        def write_finished(*args, **kwargs):
            stream.read_bytes(len(response_bytes), receive_response)

        def receive_response(data):
            self.assertEqual(self.response_without_cas(data), self.response_without_cas(response_bytes))
            stream.close()
            self.stop()

        self.io_loop.add_callback(start_test)

        self.wait()

    @istest
    def starts_with_binary_protocol_handler_by_default(self):
        server = Server(io_loop=self.io_loop)
        self.assertIsInstance(server.handler, BinaryProtocolHandler)

    @istest
    def passes_io_loop_to_protocol(self):
        server = Server(io_loop=self.io_loop)
        self.assertIs(server.io_loop, server.handler.io_loop)

    @istest
    def passes_request_to_handler(self):
        server = Server(io_loop=self.io_loop)
        handler = MagicMock(spec=BinaryProtocolHandler)
        stream = MagicMock(iostream.IOStream)

        stream.closed.side_effect = [False, True]

        with patch.object(server, 'handler', handler):
            server.backend = 'some backend'
            server.handle_stream(stream, 'some address')

            handler.process.assert_called_with(stream, 'some backend', callback=ANY)

    @istest
    def sets_a_text_handler(self):
        server = Server(io_loop=self.io_loop)
        server.set_handler('text')
        self.assertIsInstance(server.handler, TextProtocolHandler)

    @istest
    def sets_a_binary_handler(self):
        server = Server(io_loop=self.io_loop)
        server.set_handler('binary')
        self.assertIsInstance(server.handler, BinaryProtocolHandler)

    @istest
    def passes_io_loop_to_new_handler(self):
        server = Server(io_loop=self.io_loop)
        server.set_handler('text')
        self.assertIs(server.io_loop, server.handler.io_loop)


class TextProtocolHandlerTest(ServerTestCase):
    def command_for_lines(self, lines):
        return b''.join(line + b'\r\n' for line in lines)

    def assert_response_matches_request(self, request_bytes, response_bytes):
        host = '127.0.0.1'
        port = 22322

        server = Server(io_loop=self.io_loop)
        server.set_handler('text')
        server.listen(port, address=host)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stream = iostream.IOStream(s, io_loop=self.io_loop)

        def start_test():
            stream.connect((host, port), send_request)

        def send_request():
            stream.write(request_bytes, write_finished)

        def write_finished(*args, **kwargs):
            stream.read_bytes(len(response_bytes), receive_response)

        def receive_response(data):
            self.assertEqual(data, response_bytes)
            stream.close()
            self.stop()

        self.io_loop.add_callback(start_test)

        self.wait()

    @istest
    def stores_a_value(self):
        request_bytes = self.command_for_lines([
            b'set foo 0 0 3',
            b'bar',
        ])
        response_bytes = self.command_for_lines([
            b'STORED',
        ])

        self.assert_response_matches_request(request_bytes, response_bytes)

    @istest
    def stores_a_value_with_eol(self):
        value = b'bar\r\nbaz'

        with proxy_memcached(self.memcached_client) as (recv_results, sent_messages):
            self.memcached_client.set('foo', value)
        self.memcached_client.flush_all()

        request_bytes = self.command_for_lines(sent_messages)
        response_bytes = self.command_for_lines(recv_results)

        self.assert_response_matches_request(request_bytes, response_bytes)
        self.assertTrue(self.memcached_client.get('foo'), value)

    @istest
    def reads_a_value(self):
        self.memcached_client.set('foo', 'bar')

        request_bytes = self.command_for_lines([
            b'get foo',
        ])
        response_bytes = self.command_for_lines([
            b'VALUE foo 0 3',
            b'bar',
            b'END',
        ])

        self.assert_response_matches_request(request_bytes, response_bytes)

    @istest
    def reads_a_value_with_eol(self):
        value = b'bar\r\nbaz'
        self.memcached_client.set('foo', value)

        with proxy_memcached(self.memcached_client) as (recv_results, sent_messages):
            self.memcached_client.get('foo')
        self.memcached_client.flush_all()

        self.memcached_client.set('foo', value)

        request_bytes = self.command_for_lines(sent_messages)
        response_bytes = self.command_for_lines(recv_results)

        self.assert_response_matches_request(request_bytes, response_bytes)

    @istest
    def reads_multiple_values(self):
        self.memcached_client.set('foo', 'bar')
        self.memcached_client.set('foo2', 'bar2')

        request_bytes = self.command_for_lines([
            b'get foo foo2',
        ])
        response_bytes = self.command_for_lines([
            b'VALUE foo 0 3',
            b'bar',
            b'VALUE foo2 0 4',
            b'bar2',
            b'END',
        ])

        self.assert_response_matches_request(request_bytes, response_bytes)

    @istest
    def stores_a_value_successfully(self):
        host = '127.0.0.1'
        port = 12345

        with server_running(host, port, args=['-t']):
            server = '{}:{}'.format(host, port)
            client = memcache.Client([server])
            self.assertTrue(client.set('foo', 'bar'))

    @istest
    def gets_a_value_successfully(self):
        host = '127.0.0.1'
        port = 12345

        with server_running(host, port, args=['-t']):
            server = '{}:{}'.format(host, port)
            client = memcache.Client([server])
            client.set('foo', 'bar')
            self.assertEqual(client.get('foo'), 'bar')

    @istest
    def gets_multiple_values_successfully(self):
        host = '127.0.0.1'
        port = 12345

        with server_running(host, port, args=['-t']):
            server = '{}:{}'.format(host, port)
            client = memcache.Client([server])
            client.set('foo', 'bar')
            client.set('foo2', 'bar2')
            self.assertEqual(client.get_multi(['foo', 'foo2']), {
                'foo': 'bar',
                'foo2': 'bar2',
            })

    @istest
    def deletes_a_value(self):
        self.memcached_client.set('foo', 'bar')

        request_bytes = self.command_for_lines([
            b'delete foo',
        ])
        response_bytes = self.command_for_lines([
            b'DELETED',
        ])

        self.assert_response_matches_request(request_bytes, response_bytes)

    @istest
    def fails_to_delete_a_value(self):
        request_bytes = self.command_for_lines([
            b'delete baz',
        ])
        response_bytes = self.command_for_lines([
            b'NOT_FOUND',
        ])

        self.assert_response_matches_request(request_bytes, response_bytes)


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


class ArgumentParserTest(TestCase):
    @istest
    def parses_without_args(self):
        options = create_options_from_arguments([])
        self.assertEqual(options.port, 22322)
        self.assertEqual(options.address, 'localhost')
        self.assertFalse(options.is_text_protocol)

    @istest
    def parses_with_short_args(self):
        options = create_options_from_arguments([
            '-p', '1234',
            '-a', 'other.server',
            '-t'
        ])
        self.assertEqual(options.port, 1234)
        self.assertEqual(options.address, 'other.server')
        self.assertTrue(options.is_text_protocol)

    @istest
    def parses_with_long_args(self):
        options = create_options_from_arguments([
            '--port=1234',
            '--address=other.server',
            '--text-protocol'
        ])
        self.assertEqual(options.port, 1234)
        self.assertEqual(options.address, 'other.server')
        self.assertTrue(options.is_text_protocol)


class InitializationTest(TestCase):
    @istest
    @patch('memcrashed.server.Server')
    @patch('tornado.ioloop.IOLoop.instance')
    def starts_the_server_with_provided_options(self, io_loop_instance, MockServer):
        io_loop = io_loop_instance.return_value

        class options(object):
            is_text_protocol = False
            port = 'some port'
            address = 'some address'

        start_server(options)

        server_instance = MockServer.return_value
        MockServer.assert_called_with(io_loop=io_loop)
        self.assertFalse(server_instance.set_handler.called)
        server_instance.listen.assert_called_with(options.port, options.address)
        io_loop.start.assert_called_with()

    @istest
    @patch('memcrashed.server.Server')
    @patch('tornado.ioloop.IOLoop.instance')
    def starts_the_server_with_text_protocol(self, io_loop_instance, MockServer):
        io_loop = io_loop_instance.return_value

        class options(object):
            is_text_protocol = True
            port = 'some port'
            address = 'some address'

        start_server(options)

        server_instance = MockServer.return_value
        MockServer.assert_called_with(io_loop=io_loop)
        server_instance.set_handler.assert_called_with('text')
        server_instance.listen.assert_called_with(options.port, options.address)
        io_loop.start.assert_called_with()

    @istest
    @patch('memcrashed.server.create_options_from_arguments')
    @patch('memcrashed.server.start_server')
    def runs_main_function(self, start_server, create_options_from_arguments):
        main()

        create_options_from_arguments.assert_called_with(sys.argv[1:])
        start_server.assert_called_with(create_options_from_arguments.return_value)
