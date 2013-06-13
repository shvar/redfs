#!/usr/bin/python
"""
NOTIFY_NODE transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import pauses

from protocol import transactions
from protocol.messages import NotifyNodeMessage

from ._host import AbstractHostTransaction



#
# Classes
#

class NotifyNodeTransaction_Host(transactions.NotifyNodeTransaction,
                                 AbstractHostTransaction):
    """NOTIFY_NODE transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the NOTIFY_NODE transaction on the Host."""

        __slots__ = ('logs',)

        name = 'NOTIFY_NODE'


        def __init__(self, logs, *args, **kwargs):
            super(NotifyNodeTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.logs = logs


        def __str__(self):
            return u'logs={self.logs!r}'.format(self=self)



    @pauses
    def on_begin(self):
        with self.open_state() as state:
            if state.logs is not None:
                self.message.payload.append(NotifyNodeMessage.ErrorLogsPayload(
                                                state.logs))

        self.manager.post_message(self.message)


    def on_end(self):
        pass
