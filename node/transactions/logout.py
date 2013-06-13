#!/usr/bin/python
"""
LOGOUT transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from protocol import transactions

from trusted.docstore.models.transaction_states.logout import \
    LogoutTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Constants/variables
#

logger = logging.getLogger(__name__)



#
# Classes
#

class LogoutTransaction_Node(transactions.LogoutTransaction,
                             AbstractNodeTransaction):
    """LOGOUT transaction on the Node."""

    __slots__ = ()

    State = LogoutTransactionState_Node


    def on_begin(self):
        src_uuid = self.message.src.uuid
        logger.debug('Marking %s as logged out', src_uuid)
        self.manager.app.known_hosts.mark_as_just_logged_out(src_uuid)


    def on_end(self):
        self.message_ack = self.message.reply()
        self.manager.post_message(self.message_ack)
