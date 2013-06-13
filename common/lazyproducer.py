#!/usr/bin/python
"""The Twisted-style Pull Producer over the iterable."""

#
# Imports
#

from __future__ import absolute_import

import collections as col
import logging

from twisted.internet.defer import Deferred
from twisted.internet.interfaces import IPullProducer

from zope.interface import implements

from contrib.dbc import contract_epydoc

from .utils import exceptions_logged



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class LazyPullProducer(object):
    """
    The Twisted-style Pull Producer over the iterable (maybe non-reiterable)
    which yields the lines of the message body.
    """

    implements(IPullProducer)

    __slots__ = ('completed', 'transport', '__base_iterable')


    @contract_epydoc
    def __init__(self, transport, base_iterable):
        """
        @type base_iterable: col.Iterable
        """
        self.completed = Deferred()
        self.transport = transport
        self.__base_iterable = iter(base_iterable)
        # False stands for PullProducer
        transport.registerProducer(self, False)


    @exceptions_logged(logger)
    def resumeProducing(self):
        try:
            self.transport.write(self.__base_iterable.next())
        except StopIteration:
            self.transport.unregisterProducer()
            self.__on_complete()


    @exceptions_logged(logger)
    def stopProducing(self):
        del self.__base_iterable
        self.__on_complete()


    def __on_complete(self):
        self.completed.callback(None)
