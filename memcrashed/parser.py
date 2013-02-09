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
