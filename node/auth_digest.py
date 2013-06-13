#!/usr/bin/python
"""
The code to support DIGEST authentication.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from collections import namedtuple
from uuid import UUID

from twisted.cred import error as cred_error, credentials
from twisted.cred.checkers import ICredentialsChecker
from twisted.internet import reactor, defer, threads
from twisted.python.failure import Failure
from twisted.web.guard import DigestCredentialFactory

from zope.interface import implements

from contrib.dbc import contract_epydoc

from common.abstract_peerbook import AbstractPeerBook
from common.utils import exceptions_logged, in_main_thread

from .auth_any import AuthAvatar



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class AuthDigestChecker(object):
    """The verificator of DIGEST authentication information.

    @cvar credentialInterfaces: required for ICredentialsChecker

    @type __peerbook: AbstractPeerBook

    @note: the result of this function goes into
           C{common.server}::C{HTTPAuthRealm.requestAvatar()},
           which in particular invokes C{NodeApp._avatar_converter}.
    """
    __slots__ = ('__peerbook',)

    GUEST_USERNAME = 'guest'

    ALGO = 'MD5'  # or "SHA". SHA algorithm is not supported by either Curl,
                  # Wget, or browsers, but perfectly works with urllib2

    implements(ICredentialsChecker)

    credentialInterfaces = (credentials.IUsernameDigestHash,)


    @contract_epydoc
    def __init__(self, peerbook):
        """Constructor.

        @type peerbook: AbstractPeerBook
        """
        self.__peerbook = peerbook


    @exceptions_logged(logger)
    @contract_epydoc
    def __cbDigestMatch(self, matched, username, host_uuids, msgtype):
        """
        @returns: Either the tuple of user of the peer being authenticated
            and its possible host candidates; or a C{Failure} object.
        @rtype: AuthAvatar, Failure
        """
        assert in_main_thread()

        logger.debug('Digest for %r: %s',
                     username,
                     'matched' if matched else 'mismatched')
        if matched:
            return AuthAvatar(username=username,
                              host_uuid_candidates=host_uuids,
                              do_create=(msgtype == 'LOGIN'))
        else:
            return Failure(cred_error.UnauthorizedLogin())


    @exceptions_logged(logger)
    @contract_epydoc
    def requestAvatarId(self, creds):
        """
        @note: required for ICredentialsChecker

        @type creds: credentials.DigestedCredentials

        @returns: As per the required interface,
            the C{Deferred} firing the C{AuthAvatar} object,
            or the C{AuthAvatar} object itself.
        """
        assert in_main_thread()

        _username = creds.username

        logger.debug('Digest: getting avatar for %r', _username)

        if _username == AuthDigestChecker.GUEST_USERNAME:
            return ANONYMOUS
        else:
            authorized_user = self.__peerbook.get_user_by_name(_username)

            if authorized_user is not None:
                assert _username.upper() == authorized_user.name.upper(), \
                       (_username, authorized_user)
                logger.debug('Checking digest for %r against '
                             'username=%r/method=%r/realm=%r/fields=%r',
                             authorized_user,
                             creds.username, creds.method,
                             creds.realm, creds.fields)
                return defer.maybeDeferred(creds.checkHash,
                                           authorized_user.digest) \
                            .addCallback(self.__cbDigestMatch,
                                         authorized_user.name,
                                         creds._host_uuid_candidates,
                                         creds._msgtype)
            else:
                return defer.fail(cred_error.UnauthorizedLogin())




class DigestWithCandidatesCredentialFactory(DigestCredentialFactory):
    """
    The implementation of DigestCredentialFactory which not just
    attempts to verify the digest and confirm that the user is valid,
    but also examine the available hosts for this user, examine
    the host candidates which were passed in the incoming HTTP message,
    and suggest the proper host UUID.
    """
    __slots__ = ()


    @exceptions_logged(logger)
    def decode(self, response, request):
        """
        Overrides the C{DigestCredentialFactory.decode};
        the resulting C{t.cred.credentials.DigestedCredentials} object
        will have an extra C{_host_uuid_candidates} field if it was present
        in the message (that is, it was a LOGIN message).
        """
        result = super(DigestWithCandidatesCredentialFactory, self) \
                     .decode(response, request)

        # Wild intrusion!
        _candidate_strs = \
            request.requestHeaders.getRawHeaders('x-host-uuid-candidates', [])
        try:
            _msgtype = dict(i.split('=')
                                for i in request.requestHeaders
                                                .getRawHeaders('pragma')[0]
                                                .split(','))['msgtype']
        except:
            logger.exception('Received bad headers: %r',
                             request.requestHeaders)
            _msgtype = None

        result._host_uuid_candidates = \
            None if _candidate_strs is None \
                 else frozenset(UUID(c) for c in _candidate_strs)
        result._msgtype = _msgtype

        return result
