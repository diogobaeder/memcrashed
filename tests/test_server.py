import socket

import memcache
from nose.tools import istest
from tornado import iostream
from tornado.testing import AsyncTestCase

from memcrashed.server import Server


class SmokeTest(AsyncTestCase):
    @istest
    def checks_for_backend_memcached(self):
        host = '127.0.0.1'
        port = 11211

        client = memcache.Client(['%s:%s' % (host, port)])
        data = client.get_stats()

        self.assertIsNotNone(data)


class ServerTest(AsyncTestCase):
    def setUp(self):
        super(ServerTest, self).setUp()
        host = '127.0.0.1'
        port = 11211
        client = memcache.Client(['%s:%s' % (host, port)])
        client.flush_all()

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
    def can_be_started_with_a_specified_port(self):
        pass
