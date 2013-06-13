#!/usr/bin/python
"""
EXECUTE_ON_NODE transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from common.abstractions import unpauses_incoming
from common.db import Queries

from trusted import db
from trusted.docstore.models.transaction_states.execute_on_node import \
    ExecuteOnNodeTransactionState_Node

from protocol import transactions

from ._node import AbstractNodeTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ExecuteOnNodeTransaction_Node(transactions.ExecuteOnNodeTransaction,
                                    AbstractNodeTransaction):
    """EXECUTE_ON_NODE transaction on the Node."""

    __slots__ = ()

    State = ExecuteOnNodeTransactionState_Node


    @unpauses_incoming
    def on_begin(self):
        """
        This method is called when the request together with its body
        is received completely.
        """
        _message = self.message

        datasets_to_delete = _message.ds_uuids_to_delete

        __deleted_successfully = set
        for ds_uuid in datasets_to_delete:
            # Note: we are not using delete_datasets() here,
            # cause it is possible some datasets will fail to delete
            try:
                with db.RDB() as rdbw:
                    Queries.Datasets.delete_dataset(
                        _message.src.uuid, ds_uuid, rdbw)
            except:
                # This is indeed a warning, as we cannot guarantee that
                # the host sent proper UUIDs that indeed may be deleted.
                logger.warning('Could not delete dataset %s from host %s',
                               ds_uuid, _message.src.uuid)
            else:
                __deleted_successfully.add(ds_uuid)

        with self.open_state(for_update=True) as state:
            state.deleted_successfully = __deleted_successfully


    def on_end(self):

        # We completed the transaction processing,
        # so let's prepare the reply message.
        self.message_ack = self.message.reply()

        with self.open_state() as state:
            self.message_ack.ack_deleted_ds_uuids = state.deleted_successfully
        logger.debug('Deleted: %r',
                     self.message_ack.ack_deleted_ds_uuids)

        self.manager.post_message(self.message_ack)
