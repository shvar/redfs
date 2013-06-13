#!/usr/bin/python
"""EXECUTE_ON_NODE message implementation.

EXECUTE_ON_NODE message contains a command which must be executed on the Node
upon the request from the Host.
"""

import collections as col
from uuid import UUID

from contrib.dbc import consists_of

from common.abstractions import AbstractMessage

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class ExecuteOnNodeMessage(AbstractMessage):
    """EXECUTE_ON_NODE message.

    Execute a command on the Node.

    Currently only "delete_datasets" command is supported,
    so the internals are simple.
    """

    name = 'EXECUTE_ON_NODE'
    version = 0

    __slots__ = ('ds_uuids_to_delete',
                 'ack_deleted_ds_uuids')


    def __init__(self, *args, **kwargs):
        super(ExecuteOnNodeMessage, self).__init__(*args, **kwargs)

        self.ds_uuids_to_delete = None
        self.ack_deleted_ds_uuids = None


    @bzip2
    @json_dumps
    def _get_body(self):
        # H2N
        assert (isinstance(self.ds_uuids_to_delete, list) and
                consists_of(self.ds_uuids_to_delete, UUID)), \
               repr(self.ds_uuids_to_delete)
        return {'delete_datasets': [u.hex for u in self.ds_uuids_to_delete]}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # H2N
        assert 'delete_datasets' in body, repr(body)
        self.ds_uuids_to_delete = map(UUID, body['delete_datasets'])


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # N2H
        assert isinstance(self.ack_deleted_ds_uuids, col.Iterable), \
               repr(self.ack_deleted_ds_uuids)
        return {'delete_datasets': [u.hex for u in self.ack_deleted_ds_uuids]}


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # N2H
        assert 'delete_datasets' in body, repr(body)
        self.ack_deleted_ds_uuids = map(UUID, body['delete_datasets'])
