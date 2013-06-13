#!/usr/bin/python
"""
RECEIVE_CHUNKS transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import pauses

from protocol import transactions

from trusted.docstore.models.transaction_states.receive_chunks import \
    ReceiveChunksTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Classes
#

class ReceiveChunksTransaction_Node(transactions.ReceiveChunksTransaction,
                                    AbstractNodeTransaction):
    """RECEIVE_CHUNKS transaction on the Node."""

    __slots__ = ()

    State = ReceiveChunksTransactionState_Node


    @pauses
    def on_begin(self):
        """Send the RECEIVE_CHUNKS request to the host."""
        assert self.is_outgoing()

        with self.open_state() as state:
            self.message.expect_replication = state.chunks_to_replicate
            self.message.expect_restore = state.chunks_to_restore

        self.manager.post_message(self.message)


    def on_end(self):
        """Receive the RECEIVE_CHUNKS response from the host."""
        assert self.is_outgoing()
