from unittest import TestCase

from nose.tools import istest

from memcrashed import parser
from memcrashed.parser import TextParser


class ParserTest(TestCase):
    @istest
    def unpacks_request_header_with_raw_field(self):
        request_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)

    @istest
    def unpacks_request_header_with_additional_fields(self):
        request_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.magic, 0x80)
        self.assertEqual(header.opcode, 0x01)
        self.assertEqual(header.key_length, 0x0003)
        self.assertEqual(header.extra_length, 0x08)
        self.assertEqual(header.data_type, 0x00)
        self.assertEqual(header.vbucket_id, 0x0000)
        self.assertEqual(header.total_body_length, 0x0000000e)
        self.assertEqual(header.opaque, 0x00000000)
        self.assertEqual(header.cas, 0x0000000000000000)

    @istest
    def unpacks_response_header_with_raw_field(self):
        response_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        header = parser.unpack_response_header(response_bytes)

        self.assertEqual(header.raw, response_bytes)

    @istest
    def unpacks_response_header_with_additional_fields(self):
        response_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        header = parser.unpack_response_header(response_bytes)

        self.assertEqual(header.magic, 0x80)
        self.assertEqual(header.opcode, 0x01)
        self.assertEqual(header.key_length, 0x0003)
        self.assertEqual(header.extra_length, 0x08)
        self.assertEqual(header.data_type, 0x00)
        self.assertEqual(header.status, 0x0000)
        self.assertEqual(header.total_body_length, 0x0000000e)
        self.assertEqual(header.opaque, 0x00000000)
        self.assertEqual(header.cas, 0x0000000000000000)


class TextParserTest(TestCase):
    @istest
    def unpacks_set_header_with_reply(self):
        parser = TextParser()
        request_bytes = b'set foo 0 1 2\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'set')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.bytes, 2)
        self.assertEqual(header.noreply, False)

    @istest
    def unpacks_set_header_without_reply(self):
        parser = TextParser()
        request_bytes = b'set foo 0 1 2 noreply\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'set')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.bytes, 2)
        self.assertEqual(header.noreply, True)
