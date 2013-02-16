from collections import namedtuple
from struct import Struct

COMMON_BLOCKS = 'magic opcode key_length extra_length data_type %s total_body_length opaque cas'
REQUEST_BLOCKS = COMMON_BLOCKS % 'vbucket_id'
RESPONSE_BLOCKS = COMMON_BLOCKS % 'status'
HEADER_FORMAT = '! B B H B B H I I Q'


RequestHeader = namedtuple('RequestHeader', 'raw %s' % REQUEST_BLOCKS)
ResponseHeader = namedtuple('ResponseHeader', 'raw %s' % RESPONSE_BLOCKS)
header_struct = Struct(HEADER_FORMAT)


def unpack_request_header(header_bytes):
    fields = extract_fields_for_header(header_bytes)
    return RequestHeader(*fields)


def unpack_response_header(header_bytes):
    fields = extract_fields_for_header(header_bytes)
    return ResponseHeader(*fields)


def extract_fields_for_header(header_bytes):
    tokens = header_struct.unpack(header_bytes)
    fields = (header_bytes, ) + tokens
    return fields


class TextParser(object):
    STORAGE_FIELDS = 'command key flags exptime bytes noreply'
    StorageRequestHeader = namedtuple('RequestHeader', 'raw %s' % STORAGE_FIELDS)

    def unpack_request_header(self, header_bytes):
        fields = self._fields_from_header(header_bytes)
        request_header = self.StorageRequestHeader(*fields)
        return request_header

    def _fields_from_header(self, header_bytes):
        statement = header_bytes.strip()
        header_fields = statement.split(' ')
        for i in (2, 3, 4):
            header_fields[i] = int(header_fields[i])
        fields = [header_bytes]
        fields.extend(header_fields)
        if header_fields[-1] == 'noreply':
            fields.pop()
            fields.append(True)
        else:
            fields.append(False)
        return fields
