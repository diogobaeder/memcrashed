import os
import socket
import subprocess
import time

import memcache
from mock import patch, MagicMock
from nose.tools import istest
from tornado import iostream
from tornado.testing import AsyncTestCase

from memcrashed.server import Server, BinaryProtocolHandler, TextProtocolHandler


PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))


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
    @istest
    def reads_back_a_written_value(self):
        '''
        This test uses low-level sockets to check if the Server is respecting the protocol when getting a value from memcached.
        '''
        host = '127.0.0.1'
        port = 22322

        request_header_bytes = b'\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        request_body_bytes = b'foo'
        request_bytes = request_header_bytes + request_body_bytes

        response_header_bytes = b'\x81\x0c\x00\x03\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        response_body_bytes = b'foo'
        response_bytes = response_header_bytes + response_body_bytes

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
            self.assertEqual(data, response_bytes)
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

        with patch.object(server, 'handler', handler):
            server.backend = 'some backend'
            server.handle_stream('some stream', 'some address')

            handler.process.assert_called_with('some stream', 'some backend')

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
    def starts_the_server_in_a_specific_port(self):
        host = '127.0.0.1'
        port = 12345

        server_path = os.path.join(PROJECT_ROOT, 'memcrashed', 'server.py')
        args = [
            'python',
            server_path,
            '-p', str(port),
            '-a', host,
            '-t'
        ]
        env = {
            'PYTHONPATH': PROJECT_ROOT,
        }
        proc = subprocess.Popen(args, env=env)
        time.sleep(0.1)
        try:
            server = '{}:{}'.format(host, port)
            client = memcache.Client([server])
            self.assertTrue(client.set('foo', 'bar'))
        finally:
            proc.kill()


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
