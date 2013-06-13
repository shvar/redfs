#!/usr/bin/python
"""
LOGOUT transaction implementation on the Host.
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

class LogoutTransaction_Host(transactions.LogoutTransaction,
                             AbstractHostTransaction):
    """LOGOUT transaction on the Host."""

    __slots__ = tuple()



    class State(AbstractHostTransaction.State):
        """The state for the LOGOUT transaction on the Host."""

        __slots__ = ()

        name = 'LOGOUT'



    @pauses
    def on_begin(self):
        self.manager.post_message(self.message)


    def on_end(self):
        pass
