from collections import namedtuple
from struct import Struct

REQUEST_BLOCKS = 'magic opcode key_length extra_length data_type reserved total_body_length opaque cas'
HEADER_FORMAT = '! B B H B B H I I Q'


RequestHeader = namedtuple('RequestHeader', 'raw %s' % REQUEST_BLOCKS)
header_struct = Struct(HEADER_FORMAT)


def unpack_header(header_bytes):
    tokens = header_struct.unpack(header_bytes)
    fields = (header_bytes, ) + tokens
    header = RequestHeader(*fields)
    return header
