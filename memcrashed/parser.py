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
    STORAGE_FIELDS = 'command key bytes noreply'
    DELETE_FIELDS = 'command key noreply'
    INCRDECR_FIELDS = 'command key value noreply'
    RETRIEVAL_FIELDS = 'command key'

    StorageRequestHeader = namedtuple('RequestHeader', 'raw %s' % STORAGE_FIELDS)
    DeleteRequestHeader = namedtuple('GenericRequestHeader', 'raw %s' % DELETE_FIELDS)
    IncreaseDecreaseRequestHeader = namedtuple('IncreaseDecreaseRequestHeader', 'raw %s' % INCRDECR_FIELDS)
    RetrievalRequestHeader = namedtuple('RetrievalRequestHeader', 'raw %s' % RETRIEVAL_FIELDS)

    def unpack_request_header(self, header_bytes):
        fields = self._fields_from_header(header_bytes)
        command = fields[1]
        if self._is_storage_command(command):
            request_header = self.StorageRequestHeader(*fields)
        elif self._is_incrdecr_command(command):
            request_header = self.IncreaseDecreaseRequestHeader(*fields)
        elif self._is_delete_command(command):
            request_header = self.DeleteRequestHeader(*fields)
        else:
            request_header = self.RetrievalRequestHeader(*fields)
        return request_header

    def _fields_from_header(self, header_bytes):
        statement = header_bytes.strip()
        header_fields = statement.split(b' ')
        command = header_fields[0]
        key = header_fields[1]
        fields = [
            header_bytes,
            command,
            key,
        ]
        if self._is_storage_command(command):
            bytes_ = int(header_fields[4])
            fields.append(bytes_)
        if self._is_incrdecr_command(command):
            value = int(header_fields[2])
            fields.append(value)
        if not self._is_retrieval_command(command):
            noreply = statement.endswith(b'noreply')
            fields.append(noreply)
        return fields

    def _is_storage_command(self, command):
        return command in (b'set', b'cas')

    def _is_retrieval_command(self, command):
        return command in (b'get', )

    def _is_delete_command(self, command):
        return command in (b'delete', )

    def _is_incrdecr_command(self, command):
        return command in (b'incr', b'decr')
