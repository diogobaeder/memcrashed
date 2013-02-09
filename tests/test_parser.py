from unittest import TestCase

from nose.tools import istest

from memcrashed import parser


class ParserTest(TestCase):
    @istest
    def unpacks_request_header_with_raw_field(self):
        request_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        request_header = parser.unpack_header(request_bytes)

        self.assertEqual(request_header.raw, request_bytes)

    @istest
    def unpacks_request_header_with_additional_fields(self):
        request_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        header = parser.unpack_header(request_bytes)

        self.assertEqual(header.magic, 0x80)
        self.assertEqual(header.opcode, 0x01)
        self.assertEqual(header.key_length, 0x0003)
        self.assertEqual(header.extra_length, 0x08)
        self.assertEqual(header.data_type, 0x00)
        self.assertEqual(header.reserved, 0x0000)
        self.assertEqual(header.total_body_length, 0x0000000e)
        self.assertEqual(header.opaque, 0x00000000)
        self.assertEqual(header.cas, 0x0000000000000000)
