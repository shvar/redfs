#!/usr/bin/python
"""EXECUTE_ON_HOST message implementation.

EXECUTE_ON_HOST message contains a command which must be executed on the Host
upon the request from the Node.

Currently only "delete_chunks" command is supported,
so the internals are simple.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from contrib.dbc import consists_of

from common.abstractions import AbstractMessage

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class ExecuteOnHostMessage(AbstractMessage):
    """EXECUTE_ON_HOST message."""

    name = 'EXECUTE_ON_HOST'
    version = 0

    __slots__ = ('chunk_uuids_to_delete',
                 'ack_deleted_chunk_uuids')


    def __init__(self, *args, **kwargs):
        super(ExecuteOnHostMessage, self).__init__(*args, **kwargs)
        self.chunk_uuids_to_delete = []


    @bzip2
    @json_dumps
    def _get_body(self):
        # N2H
        assert consists_of(self.chunk_uuids_to_delete, UUID)
        return {'delete_chunks': [u.hex for u in self.chunk_uuids_to_delete]}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # N2H
        assert 'delete_chunks' in body, \
               repr(body)
        self.chunk_uuids_to_delete = map(UUID, body['delete_chunks'])


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # H2N
        assert (isinstance(self.ack_deleted_chunk_uuids, list) and
                consists_of(self.ack_deleted_chunk_uuids, UUID)), \
               repr(self.ack_deleted_chunk_uuids)
        return {'delete_chunks': [u.hex for u in self.ack_deleted_chunk_uuids]}


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # H2N
        assert 'delete_chunks' in body, repr(body)
        self.ack_deleted_chunk_uuids = map(UUID, body['delete_chunks'])
