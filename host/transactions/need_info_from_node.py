#!/usr/bin/python
"""
NEED_INFO_FROM_NODE transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging

from contrib.dbc import contract_epydoc

from common.abstractions import pauses

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class NeedInfoFromNodeTransaction_Host(
          transactions.NeedInfoFromNodeTransaction,
          AbstractHostTransaction):
    """NEED_INFO_FROM_NODE transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the NEED_INFO_FROM_NODE transaction on the Host."""

        __slots__ = ('query', 'ack_result')

        name = 'NEED_INFO_FROM_NODE'


        @contract_epydoc
        def __init__(self, query, *args, **kwargs):
            """Constructor.

            @type query: col.Mapping
            """
            super(NeedInfoFromNodeTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.query = dict(query)


        def __str__(self):
            # Do not log "query" - it may be huge to generate.
            return u'query={!r}'.format(self.query)



    @pauses
    def on_begin(self):
        """Send NEED_INFO_FROM_NODE request to the Node."""
        assert self.is_outgoing()

        with self.open_state() as state:
            self.message.query = state.query
        logger.verbose('NEED_INFO_FROM_NODE: %r', self.message.query)

        self.manager.post_message(self.message)


    def on_end(self):
        """Receive NEED_INFO_FROM_NODE response from the Node."""
        assert self.is_outgoing()
        if self.message_ack is None:
            logger.warning('Cannot perform %r', self)
        else:
            logger.debug('NEED_INFO_FROM_NODE result received!')
            with self.open_state(for_update=True) as state:
                state.ack_result = self.message_ack.ack_result
