#!/usr/bin/python
"""
Various extensions to the PyZMQ and ZMQ-specific reactor.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import numbers
import time
import warnings
from abc import ABCMeta, abstractmethod
from threading import Lock

try:
    import zmq
except ImportError:
    warnings.warn('ZMQ module not found (though not used yet).')
    ZMQ_VERSION_TUPLE = None
else:
    ZMQ_VERSION_TUPLE = tuple(map(int, zmq.zmq_version().split('.')))



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ZMQProtocol(object):
    """
    Base abstract class for a ZMQ messaging protocol under ZMQ reactor.
    """
    __metaclass__ = ABCMeta

    __slots__ = ('transport',)


    @abstractmethod
    def messageReceived(self, message):
        """
        You must implement this method in any subclass!

        @raises NotImplementedError: whenever not implemented in any subclass.
        """
        pass



class ZMQReqConnection(object):
    """
    This class is capable to handle ZMQ connections and reconnections
    in case of errors, using ZMQ Req socket, and performing ZMQ send and
    receive operations with timeouts.
    """

    __slots__ = ('_url', '_socket',
                 '__zmq_lock')


    # Initialized on the first use in .__connect(),
    # so that a simple import of this module
    # won't cause yet another ZMQ context to be created
    # (creating two ZMQ contexts simultaneously shown to be deadly).
    _context = None


    class NoConnection(Exception):
        pass



    def __init__(self, url):
        self._url = url
        self._socket = None
        self.__zmq_lock = Lock()


    def __connect(self):
        """
        @rtype: NoneType, zmq.Socket
        @raises ZMQReqConnection.NoConnection:
            if the connection could not be established.
        """
        assert self._socket is None
        if self._socket is not None:
            raise Exception(repr(self._socket))

        if self._url:
            if ZMQReqConnection._context is None:
                ZMQReqConnection._context = zmq.Context()

            self._socket = ZMQReqConnection._context.socket(zmq.REQ)
            self._socket.connect(self._url)
        else:
            raise ZMQReqConnection.NoConnection()


    def __disconnect(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None


    def recv_json(self, timeout=None):
        """
        Given a socket, try to receive the message (and JSON-decode it),
        spending no more (maybe less!) than C{timeout} seconds (approximately)
        in waiting for socket to be ready.

        @param timeout: how much to wait for socket to be ready.
                        If None, wait infinitely.
        @type timeout: NoneType, numbers.Real

        @returns: the message (already JSON-decoded).
        @raises ZMQReqConnection.NoConnection:
            if the connection could not be established.
        """
        assert timeout is None or timeout

        res = None

        with self.__zmq_lock:
            # No sense to reconnect it if it is None,
            # as we are anyway waiting for incoming messages.
            if self._socket is None:
                pass
            else:
                # Is the socket immediately available for reading?
                sockets = [self._socket]

                # First, see if the socket is readable within a timeout.
                rlist, wlist, xlist = zmq.select(rlist=sockets,
                                                 wlist=sockets,
                                                 xlist=sockets,
                                                 timeout=timeout)

                # There is a known bug in ZMQ 2.0 (fixed in ZMQ 2.1),
                # causing timeout sometime to be ignored.
                # Abide by timeout manually.
                if self._socket not in rlist and \
                   ZMQ_VERSION_TUPLE[0:2] < (2, 1):
                    time.sleep(timeout)
                    # Now, quickly poll to check if it is readable immediately
                    # (we've waited for too long already).
                    rlist, wlist, xlist = zmq.select(rlist=sockets,
                                                     wlist=sockets,
                                                     xlist=sockets,
                                                     timeout=0)

                # After one or two timeouts, is it finally readable?
                if self._socket in rlist:
                    try:
                        res = self._socket.recv_json()
                    except (zmq.ZMQError, ValueError) as e:
                        logger.error('ZMQ error on receive: %r, %s', e, e)
                else:
                    # Timeout, no data to return
                    logger.debug('Timed out on ZMQ receive.')

        if res is None:
            self.__disconnect()
            raise ZMQReqConnection.NoConnection(
                      'No connection to Trusted Host')
        else:
            return res


    def send_json(self, msg):
        """
        Send a JSON-encoded message. If the connection is not established yet,
        establish it before sending.

        @raises ZMQReqConnection.NoConnection:
            if the connection could not be established.
        """
        with self.__zmq_lock:
            if self._socket is None:
                self.__connect()

            if self._socket is not None:
                sockets = [self._socket]
                # Is the socket immediately available for writing?
                rlist, wlist, xlist = zmq.select(rlist=sockets,
                                                 wlist=sockets,
                                                 xlist=sockets,
                                                 timeout=0)

                try:
                    if self._socket in wlist:
                        self._socket.send_json(msg)
                    else:
                        raise ZMQReqConnection.NoConnection()
                except (zmq.ZMQError, ZMQReqConnection.NoConnection) as e:
                    logger.error('ZMQ error on send: %s', e)
                    self.__disconnect()
                    raise ZMQReqConnection.NoConnection(
                              'No connection to Trusted Host')
