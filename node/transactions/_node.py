#!/usr/bin/python
"""The transaction on the Node."""

#
# Imports
#

from __future__ import absolute_import
import logging

from contrib.dbc import contract_epydoc

from common.abstractions import AbstractTransaction
from common.utils import exceptions_logged

from trusted.docstore.models.transactions import \
    Transaction as TransactionModel



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class AbstractNodeTransaction(AbstractTransaction):
    """
    An abstract class for every node-specific transaction implementation.
    """

    # Cannot use __slots__ due to multiple inheritance

    def is_incoming(self, deep_check=True):
        """
        See the definition in AbstractTransaction.
        """
        assert self.message is not None
        assert self.message.dst is not None

        my_nodes = set(self.manager.app.ports_map.itervalues())

        if self.message.dst in my_nodes:
            assert self.message.src not in my_nodes
            result = True
        else:
            result = False

        if deep_check:
            super(AbstractNodeTransaction, self) \
                .is_incoming_deep_check(my_nodes, result)

        return result


    def is_outgoing(self, deep_check=True):
        """
        See the definition in AbstractTransaction.
        """
        assert self.message is not None
        assert self.message.src is not None

        my_nodes = set(self.manager.app.ports_map.itervalues())

        if self.message.src in my_nodes:
            assert self.message.dst not in my_nodes
            result = True
        else:
            result = False

        if deep_check:
            super(AbstractNodeTransaction, self) \
                .is_outgoing_deep_check(my_nodes, result)

        return result


    @exceptions_logged(logger)
    def neither_incoming_nor_outgoing(self):
        """
        Handle the errorneous situation when the transaction
        is neither incoming nor outgoing.
        """
        logger.error('What does this transaction (%r) do here?\n'
                         '\tus:  %r\n'
                         '\tsrc: %r\n'
                         '\tdst: %r',
                     self,
                     self.manager.app.ports_map.values(),
                     self.message.src,
                     self.message.dst)


    @contract_epydoc
    def to_model(self, state):
        """Get a FastDB-model representation from the transaction object.

        @type state: AbstractTransaction.State
        @rtype: TransactionModel
        """
        return TransactionModel(type_=self.type,
                                uuid=self.uuid,
                                src=self.message.src.uuid,
                                dst=self.message.dst.uuid,
                                ts=self.start_time,
                                state=state)
