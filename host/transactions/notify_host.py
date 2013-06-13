#!/usr/bin/python
"""
NOTIFY_HOST transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Classes
#

class NotifyHostTransaction_Host(transactions.NotifyHostTransaction,
                                 AbstractHostTransaction):
    """NOTIFY_HOST transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the NOTIFY_HOST transaction on the Host."""

        __slots__ = ()

        name = 'NOTIFY_HOST'



    def on_begin(self):
        assert self.is_incoming()


    def on_end(self):
        assert self.is_incoming()
        self.message_ack = self.message.reply()
        self.manager.post_message(self.message_ack)
