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
    @istest
    def reads_back_a_written_value(self):
        '''
        This test uses low-level sockets to check if the Server is respecting the protocol when setting a value to memcached.

        The operation being done is setting "foo" key as "bar" value.
        '''
        host = '127.0.0.1'
        port = 22322

        request_header_bytes = b'\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        request_body_bytes = b'foo'
        request_bytes = request_header_bytes + request_body_bytes

        response_header_bytes = b'\x81\x0c\x00\x03\x04\x00\x00\x00\x00\x00\x00\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01'
        response_body_bytes = b'\x00\x00\x00\x00foobar'
        response_bytes = response_header_bytes + response_body_bytes

        server = Server(io_loop=self.io_loop)
        server.listen(port, address=host)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stream = iostream.IOStream(s, io_loop=self.io_loop)

        def start_test():
            print('test started')
            stream.connect((host, port), send_request)

        def send_request():
            print('sending request...')
            stream.write(request_bytes)
            print('wrote request to stream')
            stream.read_bytes(len(response_bytes), receive_response)
            print('read bytes')

        def receive_response(data):
            print('receiving response...')
            self.assertEqual(data, response_bytes)
            stream.close()
            print('closed the stream')
            self.stop()
            print('stopped the io_loop')

        self.io_loop.add_callback(start_test)

        self.wait(timeout=30)

    @istest
    def can_be_started_with_a_specified_port(self):
        pass
