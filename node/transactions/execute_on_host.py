#!/usr/bin/python
"""
EXECUTE_ON_HOST transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import pauses

from trusted.docstore.models.transaction_states.execute_on_host import \
    ExecuteOnHostTransactionState_Node

from protocol import transactions

from ._node import AbstractNodeTransaction



#
# Classes
#

class ExecuteOnHostTransaction_Node(transactions.ExecuteOnHostTransaction,
                                    AbstractNodeTransaction):
    """EXECUTE_ON_HOST transaction on the Node."""

    __slots__ = ()

    State = ExecuteOnHostTransactionState_Node


    @pauses
    def on_begin(self):
        with self.open_state() as state:
            self.message.chunk_uuids_to_delete = state.chunk_uuids_to_delete

        self.manager.post_message(self.message)


    def on_end(self):
        pass
