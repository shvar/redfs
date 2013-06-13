#!/usr/bin/python
"""
NEED_INFO_FROM_HOST transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from common.abstractions import pauses

from trusted.docstore.models.transaction_states.need_info_from_host \
    import NeedInfoFromHostTransactionState_Node

from protocol import transactions

from ._node import AbstractNodeTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class NeedInfoFromHostTransaction_Node(
          transactions.NeedInfoFromHostTransaction,
          AbstractNodeTransaction):
    """NEED_INFO_FROM_HOST transaction on the Node."""

    __slots__ = ()

    State = NeedInfoFromHostTransactionState_Node


    @pauses
    def on_begin(self):
        """Send NEED_INFO_FROM_HOST request to the Host."""
        assert self.is_outgoing()

        with self.open_state() as state:
            self.message.query = state.query
        logger.debug('Want info from %r: %r',
                     self.message.dst, self.message.query)

        self.manager.post_message(self.message)


    def on_end(self):
        """Receive NEED_INFO_FROM_HOST response from the Host."""
        assert self.is_outgoing()

        if self.message_ack is None:
            logger.warning('%r failed', self)
        else:
            with self.open_state(for_update=True) as state:
                state.ack_result = self.message_ack.ack_result
