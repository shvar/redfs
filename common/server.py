#!/usr/bin/python
"""HTTP Server implementation for the Node and the Host."""

#
# Imports
#

from __future__ import absolute_import

import collections as col
import logging
import multiprocessing
from functools import partial
from uuid import UUID

from OpenSSL import crypto

from zope.interface import implements

from twisted.cred import portal
from twisted.internet import reactor, threads
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure
from twisted.web import server, resource
from twisted.web.guard import HTTPAuthSessionWrapper

from contrib.dbc import contract_epydoc, consists_of

from .abstractions import AbstractInhabitant
from .ssl import ServerContextFactory
from .ssl_credential_factory import SSLCredentialFactory
from .utils import exceptions_logged, in_main_thread
from .twisted_utils import callFromThread, callInThread



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class HTTPAuthRealm(object):
    """
    @todo: Track http://twistedmatrix.com/trac/ticket/4410 to see
           if the proper Digest HTTP authentication is implemented
           in the upstream.
    """
    __slots__ = ('__resource_class', '__port', '__avatar_converter')

    implements(portal.IRealm)


    def __init__(self, resource_class, port, avatar_converter):
        self.__resource_class = resource_class
        self.__port = port
        self.__avatar_converter = avatar_converter


    @exceptions_logged(logger)
    def __on_logging_out(self, *args, **kwargs):
        logger.debug('Logging out: %r, %r', args, kwargs)


    @exceptions_logged(logger)
    def requestAvatar(self, avatarId, mind, *interfaces):
        logger.debug('REQUEST AVATAR ID %r %r', avatarId, interfaces)

        if resource.IResource in interfaces:
            logger.debug('Using resource %r', self.__resource_class)
            avatar = self.__avatar_converter(avatarId)
            logger.debug('Converted avatar id %r to avatar %r',
                         avatarId, avatar)
            return (resource.IResource,
                    self.__resource_class(avatar=avatar),
                    self.__on_logging_out)
        raise NotImplementedError('Only IProtocolUser interface is '
                                      'supported by this realm.')



class CalathiHTTPAuthSessionWrapper(HTTPAuthSessionWrapper):
    """
    @note: During the authentication, if the request is passing under SSL/TLS,
           and the peer certificate is available (assuming it is validated
           by the OpenSSL means),
           we spoof the "authentication" header like if the authentication
           method is "ssl"; then our own custom SSLCredentialFactory
           will handle it properly.
    """

    @exceptions_logged(logger)
    def _authorizedResource(self, request, *args, **kwargs):
        try:
            x509 = request.transport.getPeerCertificate()
        except Exception:
            logger.exception('Problems with getting the peer certificate')
            x509 = None

        if x509 is not None:
            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            logger.debug('CalathiHTTPAuthSessionWrapper login '
                             'with certificate: %r',
                         str(x509.get_subject()))
            request.requestHeaders.removeHeader('authorization')
            request.requestHeaders.addRawHeader('authorization',
                                                'ssl')

        return super(CalathiHTTPAuthSessionWrapper, self)\
                   ._authorizedResource(request, *args, **kwargs)



class Server(object):
    """
    An HTTP server.
    """
    __slots__ = ('app', 'me', 'port', 'resource_class',
                 'checkers', '__cred_factories', 'avatar_converter',
                 'ssl_cert', 'ssl_pkey',
                 'node_uuid', '__extra_ssl_cert',
                 '__server_port')


    @contract_epydoc
    def __init__(self, app, me, port, resource_class,
                 checkers, cred_factories, avatar_converter,
                 ssl_cert, ssl_pkey, node_uuid, extra_ssl_cert,
                 *args, **kwargs):
        """Constructor.

        @type me: AbstractInhabitant

        @param port: TCP port to listen.
        @type port: int

        @param resource_class: The class of the resource that is used
                               to render the pages.
        @precondition: issubclass(resource_class, DeferredProcessingResource)

        @type checkers: col.Sequence
        @type cred_factories: col.Sequence

        @type ssl_cert: crypto.X509
        @type ssl_pkey: crypto.PKey

        @type node_uuid: UUID
        """
        super(Server, self).__init__(*args, **kwargs)

        self.app = app
        self.me = me
        self.port = port
        self.resource_class = resource_class
        self.checkers = checkers
        self.__cred_factories = cred_factories
        self.avatar_converter = avatar_converter
        self.ssl_cert = ssl_cert
        self.ssl_pkey = ssl_pkey
        self.node_uuid = node_uuid
        self.__extra_ssl_cert = extra_ssl_cert
        self.__server_port = None


    def run(self):
        self.app.server_process = self
        my_resource = self.resource_class
        my_resource.init_environment_perprocess(self.port)

        wrapped_resource = CalathiHTTPAuthSessionWrapper(
            portal.Portal(HTTPAuthRealm(my_resource,
                                        self.port,
                                        self.avatar_converter),
                          self.checkers),
            # Only MD5 and SHA (in the form of SHA1)
            # are supported by the DigestCredentialFactory
            [SSLCredentialFactory()] + self.__cred_factories)

        factory = server.Site(wrapped_resource)

        logger.debug('%r: reactor %s listening on port %i',
                     self.me, reactor, self.port)
        context_factory = \
            ServerContextFactory(private_key=self.ssl_pkey,
                                 certificate=self.ssl_cert,
                                 node_uuid=self.node_uuid,
                                 extra_cert=self.__extra_ssl_cert)

        # pylint:disable=E1101,C0103
        _listenSSL = reactor.listenSSL
        _addTrigger = reactor.addSystemEventTrigger
        # pylint:enable=E1101,C0103

        #reactor.listenTCP(self.port, factory)
        self.__server_port = _listenSSL(self.port, factory, context_factory)
        self.port = self.__server_port.getHost().port
        return self.port

        _addTrigger('before', 'shutdown', self.stop)


    @exceptions_logged(logger)
    def stop(self):
        """
        @returns: Deferred object which is fired when the listening is stopped.
        @rtype: Deferred
        """
        logger.debug('Server process stopped.')
        d = self.__server_port.stopListening()
        if d is None:
            d = Deferred()
            d.callback(None)
        return d



class DeferredProcessingResource(resource.Resource):
    """
    A C{twisted.web.resource.Resource} implementation that performs the output
    rendering in a delayed way, deferring the calculation to the thread pool
    and waiting for their completion in the pool.

    @todo: Enable abstract class as soon as a Twisted's C{resource.Resource}
           becomes a new-style class.
    """
    __slots__ = ('_ready_to_finish', 'avatar', 'connection_alive')

    isLeaf = True


    @exceptions_logged(logger)
    def __init__(self, avatar, *args, **kwargs):
        """Constructor.

        @todo: This should be streamlined using super() as soon
               as twisted.web.resource.Resource becomes a new-style class

        @ivar _ready_to_finish: a C{Deferred} which is called when the resource
                                completes the response.
        @type _ready_to_finish: Deferred
        """
        resource.Resource.__init__(self, *args, **kwargs)

        self._ready_to_finish = Deferred()
        self.avatar = avatar

        self.connection_alive = True


    def __request_finish_in_thread(self, request):
        """
        While still in secondary thread,... if the response generation
        went successfully so far, call C{request.finish()}.
        """
        if self.connection_alive:
            # pylint:disable=E1101,C0103
            callFromThread(request.finish)


    @exceptions_logged(logger)
    def render(self, request):
        """
        This method is called by the HTTP server automatically
        to render the response page to a request.

        As per docs, it returns C{server.NOT_DONE_YET};
        but, as a side effect, launches the code
        (C{_render}; in the separate thread)
        that renders the output and writes the result it to C{request}
        (doing C{request.write} or attaching a Producer).
        After that, as soon as the whole data is written,
        the separate thread calls C{request.finish()}.
        """
        assert in_main_thread()
        request.notifyFinish().addErrback(self._handle_connection_close)

        callInThread(self.__render_in_thread,
                     request, self.avatar)

        return server.NOT_DONE_YET


    def __render_in_thread(self, request, avatar):
        assert not in_main_thread()
        logger.debug('Rendering in thread...')
        d = self._render(request, avatar)

        # To proceed, we need to wait until the response is completed.
        d.addCallback(lambda x: self._ready_to_finish)

        # These both will call __request_finish_in_thread() internally.
        d.addCallbacks(partial(self.__handle_success, request),
                       partial(self.__handle_error, request))

        logger.debug('Rendering in thread done')


    # @abstractmethod
    def _render(self, request, avatar):
        """This is a processor of any incoming request.

        Implement in any subclasses! Must return a C{Deferred}.

        Any child of this method must write all its output
        using C{request.write(data)}.
        But it doesn't have to call C{request.finish()}.

        @note: Deferred callback, exceptions logged.
        """
        raise NotImplementedError()
        # assert not in_main_thread()


    # @abstractmethod
    @exceptions_logged(logger)
    def _handle_error(self, request, failure):
        """
        This is a processor of any errors/exceptions
        occured during the rendering of any incoming request.

        Override this method!

        Any child of this method must write all its output using
        C{request.write(data)}.
        But it doesn't have to call C{request.finish()}.
        """
        self.connection_alive = False
        logger.debug('Some error %r in %r', failure, request)


    @exceptions_logged(logger)
    @contract_epydoc
    def _handle_connection_close(self, failure):
        """Process an unexpected connection close.

        @type failure: Failure
        """
        self.connection_alive = False
        logger.debug('The connection has been unexpectedly '
                         'closed by the client.')


    @exceptions_logged(logger)
    def __handle_success(self, request, result):
        assert result is None, repr(result)

        self.__request_finish_in_thread(request)


    @exceptions_logged(logger)
    def __handle_error(self, request, failure):
        """
        Process any errors/exceptions occured during the rendering
        of any incoming request.

        Do NOT override this method (and you won't be able anyway),
        override _handle_error instead!
        """
        self.connection_alive = False
        try:
            self._handle_error(request, failure)
        except Exception:
            # Very bad: we couldn't even handle the error without errors.
            logger.exception('EPIC FAIL in __handle_error')
        finally:
            self.__request_finish_in_thread(request)



class ServerProcess(Server, multiprocessing.Process):
    """
    The single dedicated process of a HTTP server.
    """

    @exceptions_logged(logger)
    def run(self):
        port = super(ServerProcess, self).run()


class MultiProcessServer(object):
    """
    The general class that creates several processes of the server
    on multiple ports.
    """
    __slots__ = ('processes',)


    @contract_epydoc
    def __init__(self,
                 app, resource_class, process_class, ports_map, checkers,
                 cred_factories, avatar_converter,
                 ssl_cert, ssl_pkey, extra_ssl_cert):
        """
        @param app: The abstraction containing all the app-specific features.

        @param resource_class: The class of the resource that is used
                               to render the pages.
        @precondition: issubclass(resource_class, DeferredProcessingResource)

        @param process_class: The class with the multiprocessing.Process
                              implementation which performs the appropriate
                              process startup.
        @precondition: issubclass(process_class, ServerProcess)

        @param ports_map: The dictionary mapping the ports where the servers
                          should be opened to the appropriate node instance.
        @type ports_map: dict
        @precondition: consists_of(ports_map.iterkeys(), int)
        @precondition: consists_of(ports_map.itervalues(), AbstractInhabitant)

        @type checkers: col.Sequence
        @type cred_factories: col.Sequence

        @type ssl_cert: crypto.X509
        @type ssl_pkey: crypto.PKey

        @type extra_ssl_cert: crypto.X509
        """
        # Hack our version string right into twisted.web.server
        assert server.version.startswith('TwistedWeb'), repr(server.version)
        server.version = resource_class.VERSION_STRING

        self.processes = [process_class(app=app,
                                        me=node_inh,
                                        port=port,
                                        resource_class=resource_class,
                                        checkers=checkers,
                                        cred_factories=cred_factories,
                                        avatar_converter=avatar_converter,
                                        ssl_cert=ssl_cert,
                                        ssl_pkey=ssl_pkey,
                                        node_uuid=node_inh.uuid,
                                        extra_ssl_cert=extra_ssl_cert)
                              for port, node_inh in ports_map.iteritems()]


    @exceptions_logged(logger)
    def start(self):
        """
        Start all the processes in the multi-process server
        """
        # Start server processes with reactors
        for prc in self.processes:
            prc.start()
