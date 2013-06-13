#!/usr/bin/python
"""
Various low-level hacks of the foreign modules/classes which could not
be easily used for the needed purpose otherwise.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from twisted.protocols.basic import LineReceiver



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Functions
#

def bind_TLSMixin_writeSomeData(new_func):
    """
    Substitute a method: twisted.internet.tcp module,
    _TLSMixin.writeSomeData().
    Relevant before Twisted 11.1.

    @param new_func: A new function to substitute the method.
                     The first argument is assumed the previous value
                     of the method, and all the other arguments
                     are assumed the same as in the original method.
    """
    from twisted.internet.tcp import _TLSMixin  # pylint:disable=E0611

    assert not hasattr(_TLSMixin.writeSomeData, '_orig_func'), \
           repr(_TLSMixin.writeSomeData)

    if not hasattr(_TLSMixin, 'writeSomeData'):
        logger.warning("_TLSMixin doesn't have writeSomeData")
    else:
        orig_func = _TLSMixin.writeSomeData

        def new_method(self, data):
            return new_func(orig_func, self, data)

        new_method._orig_func = orig_func

        _TLSMixin.writeSomeData = new_method


def unbind_TLSMixin_writeSomeData():
    """
    Unbind the previously substituted method:
    twisted.internet.tcp module, _TLSMixin.writeSomeData().

    Relevant before Twisted 11.1.
    """
    from twisted.internet.tcp import _TLSMixin  # pylint:disable=E0611

    if not hasattr(_TLSMixin.writeSomeData, '_orig_func'):
        logger.warning("_TLSMixin.writeSomeData doesn't have _orig_func")
    else:
        _TLSMixin.writeSomeData = _TLSMixin.writeSomeData._orig_func


def bind_tcp_Client_writeSomeData(new_func):
    """
    Substitute a method: twisted.internet.tcp module,
    Client.writeSomeData().
    Relevant since Twisted 11.1 (and maybe before).

    @param new_func: A new function to substitute the method.
                     The first argument is assumed the previous value
                     of the method, and all the other arguments
                     are assumed the same as in the original method.
    """
    from twisted.internet.tcp import Client

    assert not hasattr(Client.writeSomeData, '_orig_func'), \
           repr(Client.writeSomeData)

    if not hasattr(Client, 'writeSomeData'):
        logger.warning("Client doesn't have writeSomeData")
    else:
        orig_func = Client.writeSomeData

        def new_method(self, data):
            return new_func(orig_func, self, data)

        new_method._orig_func = orig_func

        Client.writeSomeData = new_method


def unbind_tcp_Client_writeSomeData():
    """
    Unbind the previously substituted method:
    twisted.internet.tcp module, Client.writeSomeData().

    Relevant since Twisted 11.1 (and maybe before).
    """
    from twisted.internet.tcp import Client

    if not hasattr(Client.writeSomeData, '_orig_func'):
        logger.warning("Client.writeSomeData doesn't have _orig_func")
    else:
        Client.writeSomeData = Client.writeSomeData._orig_func


def bind_LineReceiver_dataReceived(new_func):
    """
    Substitute a method: twisted.protocols.basic module,
    LineReceiver.dataReceived().

    @param new_func: A new function to substitute the method.
                     The first argument is assumed the previous value
                     of the method, and all the other arguments
                     are assumed the same as in the original method.

    """
    assert not hasattr(LineReceiver.dataReceived, '_orig_func'), \
           repr(LineReceiver.dataReceived)

    if not hasattr(LineReceiver, 'dataReceived'):
        logger.warning("LineReceiver doesn't have dataReceived")
    else:
        orig_func = LineReceiver.dataReceived

        def new_method(self, data):
            return new_func(orig_func, self, data)

        new_method._orig_func = orig_func

        LineReceiver.dataReceived = new_method


def unbind_LineReceiver_dataReceived():
    """
    Unbind the previously substituted method:
    twisted.protocols.basic module, LineReceiver.dataReceived().
    """
    if not hasattr(LineReceiver.dataReceived, '_orig_func'):
        logger.warning("LineReceiver.dataReceived doesn't have _orig_func")
    else:
        LineReceiver.dataReceived = LineReceiver.dataReceived._orig_func
