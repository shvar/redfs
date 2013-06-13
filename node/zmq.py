#!/usr/bin/python
"""ZMQ support on the Node."""

#
# Imports
#

from __future__ import absolute_import
import logging

from contrib.dbc import contract_epydoc

from common.utils import exceptions_logged
from common.zmq_ex import ZMQProtocol



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class NodeZMQControlProtocol(ZMQProtocol):

    __slots__ = ('app',)


    def __init__(self, app, *args, **kwargs):
        """
        The constructor is enhanced comparing to the regular ZMQ protocols,
        and contains an "app" argument.
        So, when passing to the reactor.bindZeroMQ(), it should be
        partially evaluated.

        @type app: NodeApp
        """
        super(NodeZMQControlProtocol, self).__init__(*args, **kwargs)
        self.app = app


    @exceptions_logged(logger)
    def messageReceived(self, message):
        logger.debug('ZMQ received %r at %r', message, self.app)
        self.transport.sendMessage(json.dumps({8: 10}))

