#!/usr/bin/python

#
# Imports
#

import logging

from common.abstractions import AbstractTransaction
from common.utils import exceptions_logged



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class AbstractHostTransaction(AbstractTransaction):
    """
    An abstract class for every host-specific transaction implementation.
    """

    __slots__ = ()



    class State(AbstractTransaction.State):
        """
        An abstract class for every host-specific
        transaction state payload implementation.

        @cvar name: the name of the transaction. Every subclass implementation
            must contain one.
        """

        __slots__ = ()

        def __init__(self, *args, **kwargs):
            """Constructor."""
            super(AbstractHostTransaction.State, self) \
                .__init__(tr_type=self.name,
                          *args, **kwargs)



    def is_incoming(self, deep_check=True):
        """
        See the definition in AbstractTransaction.
        """
        assert self.message is not None
        assert self.message.dst is not None

        my_host = self.manager.app.host

        if self.message.dst == my_host:
            assert self.message.src != my_host, \
                   (self.message.src, my_host)
            result = True
        else:
            result = False

        if deep_check:
            super(AbstractHostTransaction, self) \
                .is_incoming_deep_check(my_host, result)

        return result


    def is_outgoing(self, deep_check=True):
        """
        See the definition in AbstractTransaction.
        """
        assert self.message is not None
        assert self.message.src is not None

        my_host = self.manager.app.host

        if self.message.src == my_host:
            assert self.message.dst != my_host, \
                   (self.message.dst, my_host)
            result = True
        else:
            result = False

        if deep_check:
            super(AbstractHostTransaction, self) \
                .is_outgoing_deep_check(my_host, result)

        return result


    @exceptions_logged(logger)
    def neither_incoming_nor_outgoing(self):
        """
        Handle the errorneous situation when the transaction
        is neither incoming nor outgoing.
        """
        logger.error('What does this transaction (%r) do here?\n'
                         '\tme:  %r\n'
                         '\tsrc: %r\n'
                         '\tdst: %r',
                     self, self.manager.app.host,
                     self.message.src, self.message.dst)
