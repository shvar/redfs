#!/usr/bin/python
"""
The code to support SSL authentication.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from uuid import UUID

from twisted.cred import error as cred_error
from twisted.cred.checkers import ICredentialsChecker
from twisted.internet import reactor, defer, threads
from twisted.python.failure import Failure

from zope.interface import implements

from contrib.dbc import contract_epydoc

from common import ssl
from common.abstract_peerbook import AbstractPeerBook
from common.ssl_credential_factory import ISSLCertificate, SSLCredentials
from common.utils import exceptions_logged, in_main_thread

from .auth_any import AuthAvatar



#
# Constants
#

logger = logging.getLogger(__name__)




#
# Classes
#

class NodeSSLChecker(object):
    """The verificator of SSL/TLS authentication information on the Node.

    @cvar credentialInterfaces: required for ICredentialsChecker
    """
    __slots__ = ('__known_hosts',)

    implements(ICredentialsChecker)

    credentialInterfaces = (ISSLCertificate, )


    def __init__(self, known_hosts):
        self.__known_hosts = known_hosts


    @exceptions_logged(logger)
    @contract_epydoc
    def requestAvatarId(self, creds):
        """
        @note: required for ICredentialsChecker

        @type creds: SSLCredentials

        @return: as per the required interface,
            the C{Deferred} firing the C{AuthAvatar} object,
            or the C{AuthAvatar} object itself.

        @note: the result of this function goes into
            C{common.server}::C{HTTPAuthRealm.requestAvatar()},
            which in particular invokes C{NodeApp._avatar_converter}.
        """
        # On any error, consider it UnauthorizedLogin
        try:
            _subj = creds.cert.get_subject()
            assert _subj.OU == ssl.CERT_OU_HOST, \
                   repr(_subj.get_components())

            logger.debug('SSL: getting avatar for %r', _subj.CN)
            peer_uuid = UUID(_subj.CN)

            _host = self.__known_hosts.get(peer_uuid)
            _username = None if _host is None \
                             else _host.user.name

            if _username is not None:
                logger.debug('Checking certificate for %r - ok', _username)
                return AuthAvatar(username=_username,
                                  host_uuid_candidates=[peer_uuid],
                                  do_create=False)
            else:
                logger.warning('No username for host UUID %s', peer_uuid)
                return defer.fail(cred_error.UnauthorizedLogin())

        except Exception:
            logger.exception('CRITICAL ERROR!')

            return defer.fail(cred_error.UnauthorizedLogin())
