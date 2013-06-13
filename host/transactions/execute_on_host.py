#!/usr/bin/python
"""
EXECUTE_ON_HOST transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from common.abstractions import pauses, unpauses_incoming
from common.utils import exceptions_logged

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ExecuteOnHostTransaction_Host(transactions.ExecuteOnHostTransaction,
                                    AbstractHostTransaction):
    """EXECUTE_ON_HOST transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the EXECUTE_ON_HOST transaction on the Host."""

        __slots__ = ('deleted_chunk_uuids',)

        name = 'EXECUTE_ON_HOST'


        def __init__(self, *args, **kwargs):
            super(ExecuteOnHostTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.deleted_chunk_uuids = []



    @pauses
    def on_begin(self):
        self.message.body_receiver.on_finish.addCallback(
            self._eoh_body_received)
        self.message.body_receiver.on_finish.addErrback(
            lambda f: self.complete_transaction())


    @exceptions_logged(logger)
    def _eoh_body_received(self, body_data):
        with self.open_state(for_update=True) as state:
            _deleted = state.deleted_chunk_uuids \
                     = list(self.manager.app.chunk_storage.delete_chunks(
                                self.message.chunk_uuids_to_delete))
        if _deleted:
            logger.debug('After deleting %i chunks, recalculating the storage',
                         len(_deleted))
            self.manager.app.chunk_storage.update_dummy_chunks_size()

        self.complete_transaction()


    @unpauses_incoming
    def complete_transaction(self):
        """Just complete the transaction, no matter of success or failure."""
        pass


    def on_end(self):
        self.message_ack = self.message.reply()
        with self.open_state() as state:
            self.message_ack.ack_deleted_chunk_uuids = \
                state.deleted_chunk_uuids

        self.manager.post_message(self.message_ack)
