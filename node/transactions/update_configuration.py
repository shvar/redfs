#!/usr/bin/python
"""
UPDATE_CONFIGURATION transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col

from common.abstractions import pauses

from protocol import transactions

from trusted.docstore.models.transaction_states.update_configuration \
    import UpdateConfigurationTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Classes
#

class UpdateConfigurationTransaction_Node(
          transactions.UpdateConfigurationTransaction,
          AbstractNodeTransaction):
    """UPDATE_CONFIGURATION transaction on the Node."""

    __slots__ = ()

    State = UpdateConfigurationTransactionState_Node


    @pauses
    def on_begin(self):
        with self.open_state() as state:
            assert isinstance(state.settings, col.Mapping), \
                   repr(state.settings)
            # As this transaction is created,
            # we already have a message template in self.message.
            self.message.settings = state.settings

        self.manager.post_message(self.message)


    def on_end(self):
        pass
