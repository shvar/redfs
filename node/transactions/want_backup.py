#!/usr/bin/python
"""
WANT_BACKUP transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from contrib.dbc import contract_epydoc

from common.abstractions import unpauses, unpauses_incoming
from common.utils import exceptions_logged

from protocol import transactions
from protocol.messages.want_backup import WantBackupMessage as WB_Msg

from trusted import db
from trusted.db import TrustedQueries
from trusted.docstore.models.transaction_states.want_backup import \
    WantBackupTransactionState_Node

from ._node import AbstractNodeTransaction
from .backup import BackupTransaction_Node



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class WantBackupTransaction_Node(transactions.WantBackupTransaction,
                                 AbstractNodeTransaction):
    """WANT_BACKUP transaction on the Node.

    The code mostly similar to WANT_RESTORE.

    @todo: Implement the real analysis
           whether we indeed want to offer backup to the host.
    """

    __slots__ = ()

    State = WantBackupTransactionState_Node


    @unpauses
    def on_begin(self):
        """
        Received the WANT_BACKUP request from a host.
        """
        with db.RDB() as rdbw:
            # Do we need to proceed backup or not.
            suspended = \
                TrustedQueries.TrustedUsers.is_user_suspended_by_username(
                    self.message.src.user.name, rdbw)
        if not suspended:
            self.__do_nested_backup()
        else:
            with self.open_state(for_update=True) as state:
                state.ack_result_code = WB_Msg.ResultCodes.USER_IS_SUSPENDED


    def __do_nested_backup(self):
        _message = self.message
        dataset_uuid = _message.dataset_uuid

        # Start the nested BACKUP transaction; currently - unconditionally.
        b_tr = self.manager.create_new_transaction(name='BACKUP',
                                                   src=_message.dst,
                                                   dst=_message.src,
                                                   parent=self,
                                                   # BACKUP-specific
                                                   dataset_uuid=dataset_uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def _on_backup_success(b_state):
            """
            @type b_state: BackupTransaction_Node.State
            """
            logger.debug('Inner backup successful')

            with self.open_state(for_update=True) as state:
                state.ack_result_code = \
                    WB_Msg.ResultCodes.from_backup_result_code(
                        b_state.ack_result_code)

                logger.debug('Inner backup result: %r',
                             b_state.ack_result_code)


        @exceptions_logged(logger)
        def _on_backup_error(result):
            logger.debug('Inner backup unsuccessful')
            self.__done_with_want_backup()


        logger.debug('Offered BACKUP for DS %r (host %r)!',
                     dataset_uuid, _message.src)
        b_tr.completed.addCallbacks(_on_backup_success,
                                    _on_backup_error)


    @unpauses_incoming
    def __done_with_want_backup(self):
        logger.debug('Done with WANT_BACKUP')


    def on_end(self):
        assert self.is_incoming()

        self.message_ack = self.message.reply()
        with self.open_state() as state:
            self.message_ack.ack_result_code = state.ack_result_code

        self.manager.post_message(self.message_ack)
