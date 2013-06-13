#!/usr/bin/python
"""
Based on
http://twistedmatrix.com/pipermail/twisted-python/attachments/20041001/04d76272/attachment.obj
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import random
import time
import urllib2
from abc import ABCMeta, abstractmethod
from collections import deque
from hashlib import md5  # pylint:disable=E0611
from types import NoneType

from OpenSSL import SSL, crypto

from contrib.dbc import contract_epydoc, typed, consists_of

from twisted.cred import _digest
from twisted.cred.checkers import ANONYMOUS
from twisted.cred._digest import calcResponse, calcHA1, calcHA2
from twisted.internet import reactor, error, defer
from twisted.python.failure import Failure
#from twisted.web.http import HTTPClient
from twisted.web.client import (HTTPClientFactory, _makeGetterFactory,
                                HTTPPageGetter)

from .abstractions import IOutgoingSession
from .lazyproducer import LazyPullProducer
from .utils import exceptions_logged, in_main_thread



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class AbstractAuthentication(object):
    """
    Abstract implementation of any HTTP Authentication.
    """
    __slots__ = ('realm',)

    securityLevel = 0


    def __init__(self, challenge):
        self.realm = challenge.get('realm')


    def __cmp__(self, right):
        return -1 if right is None \
                  else cmp(self.securityLevel, right.securityLevel)



class DigestAuthentication(AbstractAuthentication):
    """
    DIGEST HTTP Authentication.
    """
    __slots__ = ('domain', 'nonce', 'opaque', 'stale',
                 'algorithmName', 'qop', 'nonce_count', 'supported')

    securityLevel = 2

    @exceptions_logged(logger)
    def __init__(self, challenge):
        AbstractAuthentication.__init__(self, challenge)
        self.domain = challenge.get('domain')
        self.nonce = challenge.get('nonce')
        self.opaque = challenge.get('opaque')
        self.stale = challenge.get('stale', 'false')
        self.algorithmName = challenge.get('algorithm', 'md5').lower()
        self.qop = challenge.get('qop')

        self.nonce_count = 0
        self.supported = (self.algorithmName in _digest.algorithms) and \
                         (self.qop in 'auth', None)


    @staticmethod
    @exceptions_logged(logger)
    def calculateCnonce():
        """
        The cnonce-value is an opaque quoted string value provided by the
        client and used by both client and server to avoid chosen plaintext
        attacks, to provide mutual authentication, and to provide some
        message integrity protection.  (RFC 2617, sec. 3.2.2)
        """
        return md5('{}{}'.format(time.ctime(), random.random())).hexdigest()


    @exceptions_logged(logger)
    def getAuthorization(self, user, pwd_digest, method, uri):
        algo = self.algorithmName
        cnonce = DigestAuthentication.calculateCnonce()
        _ha1 = calcHA1(algo, None, None, None, self.nonce, cnonce,
                       preHA1=pwd_digest)
        _ha2 = calcHA2(algo, method, uri, self.qop, None)
        resp = calcResponse(_ha1, _ha2, algo,
                            self.nonce, '{:08x}'.format(self.nonce_count),
                            cnonce, self.qop)

        header = 'Digest username="{0}", realm="{1}", ' \
                     'nonce="{2}", uri="{3}", ' \
                     'response="{4}", algorithm="{5}"' \
                     .format(user,
                             self.realm,
                             self.nonce,
                             uri,
                             resp,
                             self.algorithmName)
        if self.opaque is not None:
            header += ', opaque="{}"'.format(self.opaque)
        if self.qop is not None:
            header += ', qop="{0}", nc="{1:08x}", cnonce="{2}"' \
                          .format(self.qop, self.nonce_count, cnonce)
        return header



class HTTPPageGetter_wDigest(HTTPPageGetter):
    """
    @todo: serverAuth must be initialized in constructor, when the
        HTTPPageGetter becomes a new-style class and super() is working on it.
    """

    serverAuth = None


    @exceptions_logged(logger)
    def __connectionMade(self):
        """
        @note: Cloned from t.w.c.HTTPPageGetter.connectionMade()
        """
        _factory = self.factory  # pylint:disable=E1101
        method = getattr(_factory, 'method', 'GET')
        _factory.session.connected(_factory.realm)
        self.sendCommand(method, _factory.path)
        self.sendHeader('Host',
                        _factory.headers.get('host', _factory.host))
        self.sendHeader('User-Agent', _factory.agent)

        # If we are on the no-digested-yet ("Authorization" header is absent)
        # phase of the LOGIN transaction ("x-host-uuid-candidates" header is
        # present), we don't evaluate the body.
        # It will make it easier to avoid the "Content-Length" header
        # completely in the future, when switching to HTTP/1.1 and chunked
        # content type encoding.
        if _factory.headers.has_key('x-host-uuid-candidates') and \
           not _factory.headers.has_key('Authorization'):
            data = None
        else:
            data = getattr(_factory, 'postdata', None)

        if data is not None:
            self.sendHeader('Content-Length', str(len(data)))

        cookieData = []
        for (key, value) in _factory.headers.items():
            logger.debug('Sending headers: %r: %r', key, value)
            if key.lower() not in self._specialHeaders:
                # we calculated it on our own
                if isinstance(value, basestring):
                    self.sendHeader(key, value)
                else:
                    assert consists_of(value, basestring), repr(value)
                    for l in value:
                        self.sendHeader(key, l)
            if key.lower() == 'cookie':
                cookieData.append(value)
        for cookie, cookval in _factory.cookies.items():
            cookieData.append('{}={}'.format(cookie, cookval))
        if cookieData:
            self.sendHeader('Cookie', '; '.join(cookieData))
        self.endHeaders()
        self.headers = {}

        if data is not None:
            # self.transport is t.i.tcp.TLSConnection
            # (see also t.i.tcp.Connection)

            if isinstance(data, basestring):
                # The body is the string with the contents.
                self.transport.write(data)
            elif isinstance(data, col.Iterable):
                # The body is the generator yielding the contents.
                producer = LazyPullProducer(self.transport, iter(data))
            else:
                raise NotImplementedError("{!r}".format(data))


    @exceptions_logged(logger)
    def handleStatus_200(self):
        """
        Connection was successful; also, at this stage we've received
        the peer certificate.
        """
        # Since Twisted 11.1, the certificate should be received differently.
        # Though likely, it should work the same in pre-11.1.
        peer_cert = self.transport.getPeerCertificate()

        assert isinstance(peer_cert, crypto.X509), \
               repr(peer_cert)

        # Call back the special method from the caller.
        _factory = self.factory  # pylint:disable=E1101
        _factory.goodSSLCertHandler(peer_cert)


    # @exceptions_logged(logger)
    # def sendCommand(self, command, path):
    #     """
    #     Enhances t.w.h.HTTPClient.sendCommand()
    #     """
    #     # May be used for traffic measurement
    #     #text = "%s %s HTTP/1.0\r\n" % (command, path)
    #     HTTPClient.sendCommand(self, command, path)


    # @exceptions_logged(logger)
    # def sendHeader(self, name, value):
    #     """
    #     Enhances t.w.h.HTTPClient.sendHeader()
    #     """
    #     # May be used for traffic measurement
    #     text = "{}: {}\r\n".format(name, value)
    #     HTTPClient.sendHeader(self, name, value)


    @exceptions_logged(logger)
    def connectionMade(self):
        """
        Enhanced the connectionMade() method from t.w.c.HTTPPageGetter
        """
        _factory = self.factory

        if _factory.authMechanism is not None:
            realm = _factory.digestMgr.find_user_digest(_factory.realm,
                                                        _factory.url)
            if realm is not ANONYMOUS:
                try:
                    user, pwd_digest = realm
                except Exception:
                    raise ValueError('Could not parse {!r} for realm {!r}, '
                                         'url {!r} (factory {!r})'
                                         .format(realm, _factory.realm,
                                                 _factory.url, _factory))
                header = _factory.authMechanism \
                                 .getAuthorization(user,
                                                   pwd_digest,
                                                   _factory.method,
                                                   _factory.path)
                _factory.headers.update({'Authorization': header})

        # We are using our own implementation of connectionMade,
        # which is able to measure the postdata-based traffic
        # outcoming from the client;
        # in future, it is easily possible to perform the throughput control
        # (traffic shaping) as well.
        #HTTPPageGetter.connectionMade(self)
        self.__connectionMade()


    # @exceptions_logged(logger)
    # def connectionLost(self, reason):
    #     HTTPPageGetter.connectionLost(self, reason)


    @staticmethod
    @exceptions_logged(logger)
    def createAuthObject(authHeader):
        """
        Returns the authentication mechanism, or None if not implemented.
        """
        authType, challenge = authHeader.split(' ', 1)
        _authType_lower = authType.lower()

        challenge = urllib2.parse_keqv_list(urllib2.parse_http_list(challenge))
        assert _authType_lower in ('digest', 'ssl'), \
               repr(_authType_lower)

        # "basic" authentication is not supported
        return DigestAuthentication(challenge) if _authType_lower == 'digest' \
                                               else None


    @exceptions_logged(logger)
    def handleStatus_401(self):
        _factory = self.factory  # pylint:disable=E1101

        authFound = False
        for _serverAuth in self.headers.get('www-authenticate', []):
            # This may include "ssl" and "digest" methods.
            # Ignore the "ssl".

            if _factory.authMechanism is None and _serverAuth:
                authCandidate = \
                    HTTPPageGetter_wDigest.createAuthObject(_serverAuth)
                if authCandidate is not None:
                    # Let's retry with DIGEST
                    authFound = True
                    _factory.authMechanism = authCandidate

                    self.serverAuth = _serverAuth
                    logger.debug('On 401: doing reconnect')
                    self.reconnect()
                    logger.debug('On 401: reconnected')
                    self.quietLoss = True
                    logger.debug('On 401: loseConnection')
                    self._completelyDone = False
                    self.transport.loseConnection()

        if not authFound:
            # Indeed an authentication error
            return self.handleStatusDefault()


    @exceptions_logged(logger)
    def reconnect(self):
        _factory = self.factory  # pylint:disable=E1101

        if _factory.scheme == 'https':
            assert _factory.contextFactory is not None, \
                   repr(_factory.contextFactory)
            _connectSSL = reactor.connectSSL  # pylint:disable=E1101,C0103
            _connectSSL(_factory.host, _factory.port,
                        _factory, _factory.contextFactory)
            logger.debug('Connected over HTTPS')
        else:
            # Everything should go under SSL nowadays.
            assert False
            _connectTCP = reactor.connectTCP  # pylint:disable=E1101,C0103
            _connectTCP(_factory.host, _factory.port, _factory)
            logger.debug('Connected over HTTP')



class AbstractHTTPDigestMgr(object):
    """
    Abstract class defining the interface to get the username and digest
    for a realm.
    """
    __metaclass__ = ABCMeta


    @abstractmethod
    def find_user_digest(self, realm, authuri):
        """
        Abstract method that must be implemented in any child class.

        @returns: a tuple of (username, digest).
        @rtype: tuple
        """
        return None



class HTTPClientFactory_wDigest(HTTPClientFactory):
    """
    @ivar digestMgr: In the original implementation, this should have been
                     an instance of urllib2.HTTPPasswordMgr,
                     having a function find_user_password(realm, authuri)
                     that returns a (username, passwd) sequence.
                     But in this, implementation, we never store
                     a plain password, but its digest instead, thus it is
                     an AbstractHTTPDigestMgr instance.
    @type digestMgr: AbstractHTTPDigestMgr;
    """
    __slots__ = ('authMechanism', 'contextFactory', 'digestMgr', 'realm',
                 'goodSSLCertHandler', 'session')

    protocol = HTTPPageGetter_wDigest

    @exceptions_logged(logger)
    def __init__(self, *args, **kwargs):
        self.authMechanism = None

        self.contextFactory = kwargs['factory_contextFactory']
        del kwargs['factory_contextFactory']

        self.digestMgr = typed(kwargs['digestMgr'], AbstractHTTPDigestMgr)
        logger.debug('USING DIGEST MANAGER %r', self.digestMgr)
        del kwargs['digestMgr']

        self.goodSSLCertHandler = kwargs['goodSSLCertHandler']
        del kwargs['goodSSLCertHandler']

        self.realm = kwargs['realm']
        del kwargs['realm']

        self.session = kwargs['session']
        del kwargs['session']

        # In fact, C{HTTPClientFactory.__init__} sets or erases
        # C{self.postdata}, so we rewrite it after calling the base class.
        HTTPClientFactory.__init__(self, *args, **kwargs)



#
# Functions
#

@contract_epydoc
def get_page_wDigest(uri_auth_tuples,
                     digestMgr=None, goodSSLCertHandler=None, session=None,
                     *args, **kwargs):
    """Get the "page", performing the HTTP request, maybe with DIGEST auth.

    @param uri_auth_tuples: The iterable over the tuples
        (URI, realm, SSL context) which might be used to attempt
        to access the peer.
    @type uri_auth_tuples: deque

    @type session: IOutgoingSession, NoneType

    @precondition: consists_of(uri_auth_tuples, tuple) # uri_auth_tuples

    @returns: C{Deferred} object, whose {.callback} gets called when
        the body contents is received B{completely}.
        Its C{.errback} may be called with C{error.ConnectionRefusedError()}
        if there are no more URLs to try for this host, so the connection
        cannot be established at all.
        Also, if any other error occurs during the message processing
        (except C{error.ConnectError()}, C{error.UserError()},
        and C{SSL.Error()} which are handled internally), it may be used
        for errback as well.
    @rtype: defer.Deferred
    """
    assert in_main_thread
    if uri_auth_tuples:
        # There are still URIs in the list, so we may proceed.

        _uri, _realm, ssl_ctx_factory = uri_auth_tuples[0]

        if session is not None:
            session.connecting(_realm)

        if __debug__:
            logger.debug('Sending message to peer %s(%s)/%s',
                         _uri, _realm, uri_auth_tuples)

        assert _uri.startswith('https://'), \
               "{!r} doesn't start from a valid protocol name!".format(_uri)

        factory = _makeGetterFactory(str(_uri),  # Explicitly convert to str
                                     HTTPClientFactory_wDigest,
                                     contextFactory=ssl_ctx_factory,
                                     factory_contextFactory=ssl_ctx_factory,
                                     digestMgr=digestMgr,
                                     goodSSLCertHandler=goodSSLCertHandler,
                                     session=session,
                                     realm=_realm,
                                     *args, **kwargs)


        @exceptions_logged(logger)
        def get_page_success_handler(body):
            """
            @note: Deferred callback, exceptions logged.
            """
            if session is not None:
                session.connection_completed()
            return (body, factory)


        @exceptions_logged(logger)
        @contract_epydoc
        def get_page_error_handler(failure):
            """
            @type failure: Failure

            @note: Deferred errback, exceptions logged.
            """
            # Checking for the most general ConnectError/UserError errors,
            # rather than for their particular kinds.
            # Also, all SSL validation errors cause to switch the URL as well.
            if failure.check(error.ConnectError,
                             error.UserError,
                             SSL.Error):
                # Let's try to reconnect using the remaining URIs.
                uri_auth_tuples.popleft()
                logger.debug('Connection refused (%r): %r',
                             failure, failure.getErrorMessage())
                logger.verbose('Connection error traceback: %s',
                               failure.getTraceback(detail='verbose'))

                logger.debug('Reconnecting using %r', uri_auth_tuples)

                return get_page_wDigest(uri_auth_tuples=uri_auth_tuples,
                                        digestMgr=digestMgr,
                                        goodSSLCertHandler=goodSSLCertHandler,
                                        session=session,
                                        *args, **kwargs)

            elif failure.check(error.ConnectionDone):
                # Even though completed more-or-less normally,
                # still return a failure, but don't retry.
                if session is not None:
                    session.connection_completed()

            elif failure.check(error.ConnectionLost):
                # And this is bad! But don't retry either.
                if session is not None:
                    session.connection_failed()

            return failure


        d = factory.deferred

        d.addCallbacks(get_page_success_handler, get_page_error_handler)
        return d

    else:
        # No more URIs to try.
        if session is not None:
            session.connection_failed()
        return defer.fail(error.ConnectionRefusedError('All URIs unavailable'))
