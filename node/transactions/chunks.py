#!/usr/bin/python
"""
CHUNKS transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from trusted.docstore.models.transaction_states.chunks import \
    ChunksTransactionState_Node

from protocol import transactions

from ._node import AbstractNodeTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ChunksTransaction_Node(transactions.ChunksTransaction,
                             AbstractNodeTransaction):
    """CHUNKS transaction on the Node."""

    __slots__ = ()

    State = ChunksTransactionState_Node


    def on_begin(self):
        """
        Received the CHUNKS request from a host.
        """
        logger.error('The node does not support the CHUNKS transaction! '
                         '(on_begin)')


    def on_end(self):
        logger.error('The node does not support the CHUNKS transaction! '
                         '(on_end)')
