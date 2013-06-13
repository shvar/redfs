#!/usr/bin/python
"""
NOTIFY_HOST transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from contrib.dbc import consists_of

from common.abstractions import pauses

from protocol import transactions

from trusted.docstore.models.transaction_states.notify_host import \
    NotifyHostTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Classes
#

class NotifyHostTransaction_Node(transactions.NotifyHostTransaction,
                                 AbstractNodeTransaction):
    """NOTIFY_HOST transaction on the Node."""

    __slots__ = ()

    State = NotifyHostTransactionState_Node


    @pauses
    def on_begin(self):
        """
        Received the NOTIFY_HOST request from a host.
        """
        assert self.is_outgoing()

        with self.open_state() as state:
            assert (state.chunk_uuids_to_replicate is None or
                    consists_of(state.chunk_uuids_to_replicate, UUID)), \
                   repr(self.chunk_uuids_to_replicate)
            assert (state.chunk_uuids_to_restore is None or
                    consists_of(state.chunk_uuids_to_restore, UUID)), \
                   repr(self.chunk_uuids_to_restore)

            self.message.expect_replication = state.chunk_uuids_to_replicate
            self.message.expect_restore = state.chunk_uuids_to_restore

        self.manager.post_message(self.message)


    def on_end(self):
        assert self.is_outgoing()
