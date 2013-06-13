#!/usr/bin/python
"""
SEND_CHUNKS transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import pauses

from protocol import transactions

from trusted.docstore.models.transaction_states.send_chunks import \
    SendChunksTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Classes
#

class SendChunksTransaction_Node(transactions.SendChunksTransaction,
                                 AbstractNodeTransaction):
    """SEND_CHUNKS transaction on the Node."""

    __slots__ = ()

    State = SendChunksTransactionState_Node


    @pauses
    def on_begin(self):
        with self.open_state() as state:
            self.message.chunks_map = state.chunks_map

        self.manager.post_message(self.message)


    def on_end(self):
        pass
