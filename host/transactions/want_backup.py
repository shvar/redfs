#!/usr/bin/python
"""
WANT_BACKUP transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.abstractions import pauses
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

class WantBackupTransaction_Host(transactions.WantBackupTransaction,
                                 AbstractHostTransaction):
    """WANT_BACKUP transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the WANT_BACKUP transaction on the Host."""

        __slots__ = ('ds_uuid', 'ack_result_code')

        name = 'WANT_BACKUP'


        @contract_epydoc
        def __init__(self, ds_uuid, *args, **kwargs):
            """
            @type ds_uuid: UUID
            """
            super(WantBackupTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.ds_uuid = ds_uuid
            self.ack_result_code = None


        def __str__(self):
            return u'ds_uuid={self.ds_uuid!r}'.format(self=self)



    @pauses
    def on_begin(self):
        with self.open_state() as state:
            self.message.dataset_uuid = state.ds_uuid

        self.manager.post_message(self.message)


    def on_end(self):
        if self.message_ack is None:
            logger.warning('%r failed', self)
        else:
            logger.debug('WANT_BACKUP result received')
            with self.open_state(for_update=True) as state:
                state.ack_result_code = self.message_ack.ack_result_code
