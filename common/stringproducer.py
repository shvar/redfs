#!/usr/bin/python
"""
A special C{StringProducer} class that converts a usual fixed-string storage
to the C{IBodyProducer} interface.
"""

#
# Imports
#

from __future__ import absolute_import

from zope.interface import implements

from twisted.internet.defer import succeed
from twisted.web.iweb import IBodyProducer



#
# Classes
#

class StringProducer(object):
    """
    Simplistic IBodyProducer implementation which just generates
    the body according to the passed prepared contents.
    """
    implements(IBodyProducer)


    def __init__(self, body):
        self.body = body
        self.length = len(body)


    def __len__(self):
        return self.length


    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)


    def pauseProducing(self):
        pass


    def resumeProducing(self):
        pass


    def stopProducing(self):
        pass
