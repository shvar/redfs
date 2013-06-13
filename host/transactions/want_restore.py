#!/usr/bin/python
"""
WANT_RESTORE transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.abstractions import pauses

from protocol import transactions

from ._host import AbstractHostTransaction
from .restore import RestoreTransaction_Host



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class WantRestoreTransaction_Host(transactions.WantRestoreTransaction,
                                  AbstractHostTransaction):
    """WANT_RESTORE transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the WANT_RESTORE transaction on the Host."""

        __slots__ = ('dataset_uuid', 'file_paths', 'target_dir',
                     'ack_result_code')

        name = 'WANT_RESTORE'


        @contract_epydoc
        def __init__(self,
                     dataset_uuid, file_paths, target_dir,
                     *args, **kwargs):
            """Constructor.

            @type dataset_uuid: UUID
            @type file_paths: col.Iterable
            @type target_dir: basestring
            """
            super(WantRestoreTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.dataset_uuid = dataset_uuid
            self.file_paths = file_paths
            self.target_dir = target_dir


        def __str__(self):
            return (u'dataset_uuid={self.dataset_uuid!r}'
                     'file_paths={self.file_paths!r}'
                     'target_dir={self.target_dir!r}'
                         .format(self=self))



    @pauses
    def on_begin(self):
        """
        @todo: if we ever want WantRestore to work again,
            we need to fix (get rid of)
            C{RestoreTransaction_Host.restore_targets_by_wr_uuid}.
        """
        _message = self.message

        with self.open_state() as state:
            _message.dataset_uuid = state.dataset_uuid
            _message.file_paths = state.file_paths

            logger.debug('%r: will restore to %r', self, state.target_dir)
            RestoreTransaction_Host.restore_targets_by_wr_uuid[self.uuid] = \
                state.target_dir

        self.manager.post_message(self.message)


    def on_end(self):
        logger.debug('WANT_RESTORE result received')
        with self.open_state(for_update=True) as state:
            state.ack_result_code = self.message_ack.ack_result_code
