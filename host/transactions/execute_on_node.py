#!/usr/bin/python
"""
EXECUTE_ON_NODE transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging

from common.abstractions import pauses

from contrib.dbc import contract_epydoc

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ExecuteOnNodeTransaction_Host(transactions.ExecuteOnNodeTransaction,
                                    AbstractHostTransaction):
    """EXECUTE_ON_NODE transaction on the Host."""

    __slots__ = ()

    __hb_count = 0



    class State(AbstractHostTransaction.State):
        """The state for the EXECUTE_ON_NODE transaction on the Host."""

        __slots__ = ('ds_uuids_to_delete',
                     'ack_deleted_ds_uuids')

        name = 'EXECUTE_ON_NODE'


        @contract_epydoc
        def __init__(self, ds_uuids_to_delete, *args, **kwargs):
            """
            @type ds_uuids_to_delete: col.Iterable
            """
            super(ExecuteOnNodeTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.ds_uuids_to_delete = list(ds_uuids_to_delete)
            self.ack_deleted_ds_uuids = None


        def __str__(self):
            return u'ds_uuids_to_delete={self.ds_uuids_to_delete!r}' \
                       .format(self=self)



    @pauses
    def on_begin(self):
        with self.open_state() as state:
            self.message.ds_uuids_to_delete = state.ds_uuids_to_delete

        self.manager.post_message(self.message)


    def on_end(self):
        if self.failure is None:
            # Success!
            logger.info('Deleted datasets from the node: %r',
                        self.message_ack.ack_deleted_ds_uuids)

            with self.open_state(for_update=True) as state:
                state.ack_deleted_ds_uuids = \
                    self.message_ack.ack_deleted_ds_uuids

                _to_delete = frozenset(state.ds_uuids_to_delete)
                _deleted = frozenset(self.message_ack.ack_deleted_ds_uuids)

                if _to_delete == _deleted:
                    logger.info('All requested datasets deleted successfully!')
                else:
                    logger.info('Some datasets were not deleted: %r',
                                sorted(_to_delete - _deleted))

        else:
            # Failure!
            logger.error('Could not delete datasets on Node')
