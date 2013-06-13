#!/usr/bin/python
"""
READY_FOR_CHUNKS transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import pauses

from protocol import transactions

from trusted.docstore.models.transaction_states.ready_for_chunks import\
    ReadyForChunksTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Classes
#

class ReadyForChunksTransaction_Node(transactions.ReadyForChunksTransaction,
                                     AbstractNodeTransaction):
    """READY_FOR_CHUNKS transaction on the Node."""

    __slots__ = ()

    State = ReadyForChunksTransactionState_Node


    @pauses
    def on_begin(self):
        self.manager.post_message(self.message)


    def on_end(self):
        pass
