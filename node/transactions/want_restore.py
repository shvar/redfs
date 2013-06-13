#!/usr/bin/python
"""
WANT_RESTORE transaction implementation on the Node.
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
from protocol.messages import WantRestoreMessage as WR_Msg

from trusted.docstore.models.transaction_states.want_restore import \
    WantRestoreTransactionState_Node

from ._node import AbstractNodeTransaction
from .restore import RestoreTransaction_Node



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class WantRestoreTransaction_Node(transactions.WantRestoreTransaction,
                                  AbstractNodeTransaction):
    """WANT_RESTORE transaction on the Node.

    The code is mostly similar to WANT_BACKUP.
    """

    __slots__ = ()

    State = WantRestoreTransactionState_Node


    @unpauses
    def on_begin(self):
        """
        Received the WANT_RESTORE request from a host.

        @todo: add "if user is suspended" here check when we are ready
            to implement the manual restore request.
        """
        assert self.is_incoming()

        # TODO: Add "if user is suspended" here
        raise NotImplementedError()

        self.__do_nested_restore()


    def __do_nested_restore(self):
        _message = self.message
        dataset_uuid = _message.dataset_uuid

        # TODO: file_paths -> file_paths_for_basedirs
        raise NotImplementedError()

        r_tr = \
            self.manager.create_new_transaction(name='RESTORE',
                                                src=_message.dst,
                                                dst=_message.src,
                                                parent=self,
                                                # RESTORE-specific
                                                ds_uuid=dataset_uuid,
                                                file_paths=_message.file_paths,
                                                wr_uuid=self.uuid)


        @exceptions_logged(logger)
        @contract_epydoc
        def _on_restore_success(r_state):
            """
            @type r_state: RestoreTransaction_Node.State
            """
            logger.debug('Inner restore successful')

            with self.open_state(for_update=True) as state:
                state.ack_result_code = \
                    WR_Msg.ResultCodes.from_restore_result_code(
                        r_state.ack_result_code)

                logger.debug('Inner restore result: %r',
                             r_state.ack_result_code)


        @exceptions_logged(logger)
        def _on_restore_error(result):
            logger.debug('Inner restore unsuccessful')
            self.__done_with_want_restore()


        logger.debug('Offered RESTORE for %r!', dataset_uuid)
        r_tr.completed.addCallbacks(_on_restore_success,
                                    _on_restore_error)


    @unpauses_incoming
    def __done_with_want_restore(self):
        logger.debug('Done with restore ops')


    def on_end(self):
        assert self.is_incoming()

        self.message_ack = self.message.reply()
        with self.open_state() as state:
            self.message_ack.ack_result_code = state.ack_result_code

        self.manager.post_message(self.message_ack)
