#!/usr/bin/python
"""
READY_FOR_CHUNKS transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import pauses

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Classes
#

class ReadyForChunksTransaction_Host(transactions.ReadyForChunksTransaction,
                                     AbstractHostTransaction):
    """READY_FOR_CHUNKS transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the READY_FOR_CHUNKS transaction on the Host."""

        __slots__ = ()

        name = 'READY_FOR_CHUNKS'



    @pauses
    def on_begin(self):
        self.manager.post_message(self.message)


    def on_end(self):
        pass
