from unittest import TestCase

from nose.tools import istest

from memcrashed.parser import BinaryParser, TextParser


class BinaryParserTest(TestCase):
    @istest
    def unpacks_request_header_with_raw_field(self):
        parser = BinaryParser()
        request_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)

    @istest
    def unpacks_request_header_with_additional_fields(self):
        parser = BinaryParser()
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
        parser = BinaryParser()
        response_bytes = b'\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        header = parser.unpack_response_header(response_bytes)

        self.assertEqual(header.raw, response_bytes)

    @istest
    def unpacks_response_header_with_additional_fields(self):
        parser = BinaryParser()
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
        self.assertFalse(header.noreply)
        self.assertTrue(parser.is_storage_command(header.command))

    @istest
    def unpacks_set_header_without_reply(self):
        parser = TextParser()
        request_bytes = b'set foo 0 1 2 noreply\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'set')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.bytes, 2)
        self.assertTrue(header.noreply)
        self.assertTrue(parser.is_storage_command(header.command))

    @istest
    def unpacks_cas_header_with_reply(self):
        parser = TextParser()
        request_bytes = b'cas foo 0 1 2 3\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'cas')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.bytes, 2)
        self.assertFalse(header.noreply)
        self.assertTrue(parser.is_storage_command(header.command))

    @istest
    def unpacks_cas_header_without_reply(self):
        parser = TextParser()
        request_bytes = b'cas foo 0 1 2 3 noreply\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'cas')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.bytes, 2)
        self.assertTrue(header.noreply)
        self.assertTrue(parser.is_storage_command(header.command))

    @istest
    def unpacks_get_header(self):
        parser = TextParser()
        request_bytes = b'get foo\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'get')
        self.assertEqual(header.keys, [b'foo'])
        self.assertTrue(parser.is_retrieval_command(header.command))

    @istest
    def unpacks_get_header_with_multiple_items(self):
        parser = TextParser()
        request_bytes = b'get foo bar\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'get')
        self.assertEqual(header.keys, [b'foo', b'bar'])
        self.assertTrue(parser.is_retrieval_command(header.command))

    @istest
    def unpacks_gets_header(self):
        parser = TextParser()
        request_bytes = b'gets foo\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'gets')
        self.assertEqual(header.keys, [b'foo'])
        self.assertTrue(parser.is_retrieval_command(header.command))

    @istest
    def unpacks_gets_header_with_multiple_items(self):
        parser = TextParser()
        request_bytes = b'gets foo bar\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'gets')
        self.assertEqual(header.keys, [b'foo', b'bar'])
        self.assertTrue(parser.is_retrieval_command(header.command))

    @istest
    def unpacks_delete_header_with_reply(self):
        parser = TextParser()
        request_bytes = b'delete foo\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'delete')
        self.assertEqual(header.key, b'foo')
        self.assertFalse(header.noreply)
        self.assertTrue(parser.is_delete_touch_command(header.command))

    @istest
    def unpacks_delete_header_without_reply(self):
        parser = TextParser()
        request_bytes = b'delete foo noreply\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'delete')
        self.assertEqual(header.key, b'foo')
        self.assertTrue(header.noreply)
        self.assertTrue(parser.is_delete_touch_command(header.command))

    @istest
    def unpacks_incr_header_with_reply(self):
        parser = TextParser()
        request_bytes = b'incr foo 123\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'incr')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.value, 123)
        self.assertFalse(header.noreply)
        self.assertTrue(parser.is_increase_decrease_command(header.command))

    @istest
    def unpacks_incr_header_without_reply(self):
        parser = TextParser()
        request_bytes = b'incr foo 123 noreply\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'incr')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.value, 123)
        self.assertTrue(header.noreply)
        self.assertTrue(parser.is_increase_decrease_command(header.command))

    @istest
    def unpacks_decr_header_with_reply(self):
        parser = TextParser()
        request_bytes = b'decr foo 123\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'decr')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.value, 123)
        self.assertFalse(header.noreply)
        self.assertTrue(parser.is_increase_decrease_command(header.command))

    @istest
    def unpacks_decr_header_without_reply(self):
        parser = TextParser()
        request_bytes = b'decr foo 123 noreply\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'decr')
        self.assertEqual(header.key, b'foo')
        self.assertEqual(header.value, 123)
        self.assertTrue(header.noreply)
        self.assertTrue(parser.is_increase_decrease_command(header.command))

    @istest
    def unpacks_touch_header_with_reply(self):
        parser = TextParser()
        request_bytes = b'touch foo\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'touch')
        self.assertEqual(header.key, b'foo')
        self.assertFalse(header.noreply)
        self.assertTrue(parser.is_delete_touch_command(header.command))

    @istest
    def unpacks_touch_header_without_reply(self):
        parser = TextParser()
        request_bytes = b'touch foo noreply\r\n'

        header = parser.unpack_request_header(request_bytes)

        self.assertEqual(header.raw, request_bytes)
        self.assertEqual(header.command, b'touch')
        self.assertEqual(header.key, b'foo')
        self.assertTrue(header.noreply)
        self.assertTrue(parser.is_delete_touch_command(header.command))
