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


class ServerTestCase(AsyncTestCase):
    def setUp(self):
        super(ServerTestCase, self).setUp()
        host = '127.0.0.1'
        port = 11211
        client = memcache.Client(['%s:%s' % (host, port)])
        client.flush_all()
        client.disconnect_all()


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

    @istest
    def stores_a_value_successfully(self):
        host = '127.0.0.1'
        port = 12345

        with server_running(host, port, args=['-t']):
            server = '{}:{}'.format(host, port)
            client = memcache.Client([server])
            self.assertTrue(client.set('foo', 'bar'))


class TextProtocolHandlerTest(ServerTestCase):
    def command_for_lines(self, lines):
        return b''.join(line + b'\r\n' for line in lines)

    @istest
    def reads_back_a_written_value(self):
        host = '127.0.0.1'
        port = 22322

        request_bytes = self.command_for_lines([
            b'set foo 0 0 3',
            b'bar',
        ])
        response_bytes = self.command_for_lines([
            b'STORED',
        ])

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


class BinaryProtocolHandlerTest(ServerTestCase):
    @istest
    @skipUnless(PYLIBMC_EXISTS, PYLIBMC_SKIP_REASON)
    def stores_a_value_successfully(self):
        host = '127.0.0.1'
        port = 12345

        with server_running(host, port):
            server = '{}:{}'.format(host, port)
            client = pylibmc.Client([server], binary=True)
            self.assertTrue(client.set('foo', 'bar'))

    @istest
    @skipUnless(PYLIBMC_EXISTS, PYLIBMC_SKIP_REASON)
    def stores_a_value_twice_without_error(self):
        host = '127.0.0.1'
        port = 12345

        with server_running(host, port):
            server = '{}:{}'.format(host, port)
            client = pylibmc.Client([server], binary=True)
            client.set('foo', 'bar')
            self.assertTrue(client.set('foo', 'bar'))


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
