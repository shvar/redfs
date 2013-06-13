#!/usr/bin/python
"""
NOTIFY_NODE transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import os
from datetime import datetime

from common.abstractions import unpauses_incoming

from protocol import transactions

from trusted.docstore.models.transaction_states.notify_node import \
    NotifyNodeTransactionState_Node

from node import settings

from ._node import AbstractNodeTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class NotifyNodeTransaction_Node(transactions.NotifyNodeTransaction,
                                 AbstractNodeTransaction):
    """NOTIFY_NODE transaction on the Node."""

    __slots__ = ()

    State = NotifyNodeTransactionState_Node


    @unpauses_incoming
    def on_begin(self):
        for item in self.message.payload:
            if item.TYPE == 'error':
                _now = datetime.utcnow()
                _log_dir = settings.get_log_directory()

                logger.debug('Received remote logs, writing them: %r',
                             item.logs.keys())
                for orig_filename, data in item.logs.iteritems():
                    new_filename = 'remote.{0}.{1}.log'.format(orig_filename,
                                                               _now)
                    with open(os.path.join(_log_dir, new_filename),
                              'wb') as fh_out:
                        fh_out.write(data.encode('utf-8'))

            else:
                logger.error('Unsupported payload type %r for %r',
                             item.TYPE, self)


    def on_end(self):
        # We completed NOTIFY_NODE processing, so let's prepare
        # the reply message.
        self.message_ack = self.message.reply()

        self.manager.post_message(self.message_ack)
