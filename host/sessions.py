#!/usr/bin/python
"""
This class incapsulates the features regarding tracking the status of sessions
to various peers.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from abc import ABCMeta, abstractmethod, abstractproperty

from contrib.dbc import contract_epydoc, consists_of

from common import settings as common_settings
from common.abstractions import IOutgoingSession, IIncomingSession
from common.http_transport import HTTPClientTransport



#
# Logging
#

logger = logging.getLogger(__name__)
logger_status_session = logging.getLogger('status.session')



#
# Classes
#

class OutgoingSessionStatus(object):
    """
    @note: contains the status of a session, rather than a connection.
    """
    CONNECTING          = 0
    CONNECTING_VIA_NODE = 1
    CONNECTED           = 2
    CONNECTED_VIA_NODE  = 3

    _to_str = {
        CONNECTING:          'connecting',
        CONNECTING_VIA_NODE: 'connecting via node',
        CONNECTED:           'connected',
        CONNECTED_VIA_NODE:  'connected via node'
    }



class SessionTracker(object):
    """
    The class capable of tracking the status of connection sessions
    established to some peers, whether nodes or hosts.
    It also keeps an eye on each incoming connection, but does not store
    the incoming sessions as it is not needed at the moment.

    The session tracked here are the series of connections
    establishments/losses to any peer, until some connection is lost
    for errorneous reasons.
    """

    __slots__ = ('_app', '__outgoing')



    @contract_epydoc
    def __init__(self, app):
        """
        @type app: HTTPClientTransport
        """
        self._app = app
        self.__outgoing = {}


    def __repr__(self):
        return '<SessionTracker: {0!r}>' \
                   .format({k: OutgoingSessionStatus._to_str.get(
                                   v, 'n/a{0!r}'.format(v))
                                for (k, v) in self.__outgoing})


    def _is_connected_out(self, peer):
        return (peer in self.__outgoing and
                self.__outgoing[peer] \
                    in (OutgoingSessionStatus.CONNECTED,
                        OutgoingSessionStatus.CONNECTED_VIA_NODE))


    def outgoing_connecting(self, peer, host_through_node=False):
        OSS = OutgoingSessionStatus

        if ((peer not in self.__outgoing) or
            (host_through_node and self.__outgoing[peer] == OSS.CONNECTING) or
            (not host_through_node and
             self.__outgoing[peer] == OSS.CONNECTING_VIA_NODE)):
            logger_status_session.info('Connecting to %r: via_node=%r',
                                       peer, host_through_node,
                                       extra={'is_outgoing': True,
                                              'peer': peer,
                                              'status': 'connecting',
                                              'via_node': host_through_node})

        if not self._is_connected_out(peer):
            self.__outgoing[peer] = \
                OSS.CONNECTING_VIA_NODE if host_through_node \
                                        else OSS.CONNECTING


    def outgoing_connected(self, peer, host_through_node=False):

        if not self._is_connected_out(peer):
            logger_status_session.info('Connected to %r: via_node=%r',
                                       peer, host_through_node,
                                       extra={'is_outgoing': True,
                                              'peer': peer,
                                              'status': 'connected',
                                              'via_node': host_through_node})

        _oss = OutgoingSessionStatus
        self.__outgoing[peer] = _oss.CONNECTED_VIA_NODE if host_through_node \
                                                        else _oss.CONNECTED


    def outgoing_connection_completed(self, peer):
        pass


    def outgoing_connection_failed(self, peer):
        if self._is_connected_out(peer):
            logger_status_session.info('Connection to %r failed',
                                       peer,
                                       extra={'is_outgoing': True,
                                              'peer': peer,
                                              'status': 'connection_failed',
                                              'via_node': None})
            del self.__outgoing[peer]


    def incoming_connected(self, peer):
        logger_status_session.info('Incoming from %r',
                                   peer,
                                   extra={'is_outgoing': False,
                                          'peer': peer,
                                          'status': 'connected'})



class BoundOutgoingSession(IOutgoingSession):
    """
    A class defining a session bound to some specific
    outgoing connection attempt, i.e. to a single peer.
    """
    __slots__ = ("__tracker", "__peer")


    def __init__(self, tracker, peer):
        self.__tracker = tracker
        self.__peer = peer


    def __is_host_through_node_connection(self, realm):
        return (self.__peer != self.__tracker._app.primary_node and
                realm == common_settings.HTTP_AUTH_REALM_NODE)


    def connecting(self, realm):
        _host_through_node = self.__is_host_through_node_connection(realm)
        self.__tracker.outgoing_connecting(
            self.__peer, host_through_node=_host_through_node)


    def connected(self, realm):
        _host_through_node = self.__is_host_through_node_connection(realm)
        self.__tracker.outgoing_connected(
            self.__peer, host_through_node=_host_through_node)


    def connection_completed(self):
        self.__tracker.outgoing_connection_completed(self.__peer)


    def connection_failed(self):
        self.__tracker.outgoing_connection_failed(self.__peer)



class BoundIncomingSession(IIncomingSession):
    __slots__ = ('__tracker', '__peer')


    def __init__(self, tracker, peer):
        self.__tracker = tracker
        self.__peer = peer


    def connected(self):
        self.__tracker.incoming_connected(self.__peer)
