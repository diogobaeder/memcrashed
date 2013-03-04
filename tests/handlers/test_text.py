import socket

import memcache
from nose.tools import istest
from tornado import iostream

from memcrashed.pool import PoolRepository
from memcrashed.server import Server, TextProtocolHandler
from ..utils import proxy_memcached, server_running, ServerTestCase


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

    @istest
    def increases_a_value(self):
        self.memcached_client.set('foo', 2)

        request_bytes = self.command_for_lines([
            b'incr foo 3',
        ])
        response_bytes = self.command_for_lines([
            b'5',
        ])

        self.assert_response_matches_request(request_bytes, response_bytes)

    @istest
    def fails_to_increase_a_value(self):
        request_bytes = self.command_for_lines([
            b'incr baz 3',
        ])
        response_bytes = self.command_for_lines([
            b'NOT_FOUND',
        ])

        self.assert_response_matches_request(request_bytes, response_bytes)

    @istest
    def starts_with_pool_repository(self):
        handler = TextProtocolHandler(self.io_loop)

        self.assertIsInstance(handler.pool_repository, PoolRepository)
