#!/usr/bin/python
"""
HTTP transport code, common for the server and the client.
"""

#
# Imports
#

from __future__ import absolute_import

import collections as col
import logging
import uuid
from abc import ABCMeta, abstractmethod
from itertools import ifilter
from types import NoneType

from twisted.internet import defer, error as internet_error, reactor
from twisted.python.failure import Failure
from twisted.python.util import InsensitiveDict
from twisted.web.http_headers import Headers, _DictHeaders
from twisted.web.server import Request

from contrib.dbc import contract_epydoc

from .abstractions import (
    AbstractApp, AbstractMessage, AbstractInhabitant, IPrintable
)
from .abstract_transaction_manager import (
    AbstractTransactionManager, TransactionProcessingException
)
from .bodyreceiver import RequestBodyReceiver
from .inhabitants import Host
from .lazyproducer import LazyPullProducer
from .server import DeferredProcessingResource
from .utils import exceptions_logged, NULL_UUID, in_main_thread
from .twisted_utils import callFromThread, callInThread



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class MessageProcessingException(IPrintable, Exception):

    @contract_epydoc
    def __init__(self,
                 text, message=None, level=logging.ERROR,
                 *args, **kwargs):
        r"""Constructor.

        >>> e = MessageProcessingException(text='ABC', message='def')
        >>> e
        MessageProcessingException(text='ABC', message='def')

        >>> # Must be pickleable
        >>> import cPickle as pickle
        >>> s = pickle.dumps(e, pickle.HIGHEST_PROTOCOL)
        >>> s  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        '\x80\x02ccommon.http_transport\nMessagePro...ub.'
        >>> # ... and unpickleable!
        >>> pickle.loads(s)
        MessageProcessingException(text='ABC', message='def')

        @param text: text to pass to the caller.
        @type text: basestring

        @param message: original message which could not be processed;
            may be C{None} if the message could not have been even parsed.
        @type message: AbstractMessage, NoneType

        @param level: Exception level, use logging levels for reference.
        @type level: int
        """
        self.level, self._message = level, message
        super(MessageProcessingException, self).__init__(text, *args, **kwargs)


    def __str__(self):
        r"""Implement C{IPrintable} interface,

        >>> # Minimal
        >>> MessageProcessingException(text='AB')
        MessageProcessingException(text='AB')

        >>> # Maximal
        >>> MessageProcessingException(text=u'AB', message=u'DE', level=u'FG')
        MessageProcessingException(text=u'AB', message=u'DE', level=u'FG')
        """
        opts = [
            u'text={!r}'.format(self.message) if self.message != ''
                                              else '',
            u'message={!r}'.format(self._message) if self._message is not None
                                                  else '',
            u'level={!r}'.format(self.level) if self.level != logging.ERROR
                                             else '',
        ]
        return ', '.join(o for o in opts if o)



class UnsupportedBuildException(MessageProcessingException):
    """
    The exception which is raised if the host or node build is unsupported.
    """
    pass



class HTTPTransport(object):
    """General HTTP transportation of protocol messages.

    Any child must define VERSION_STRING class variable.
    """

    __metaclass__ = ABCMeta

    tr_manager = None
    app = None


    @classmethod
    def init_environment(cls, app, tr_manager, force=False):
        """Initialize the common environment for any transport.

        @type app: AbstractApp

        @param tr_manager: a particular implementation of transaction manager
            to use with this HTTP transport.
        @type tr_manager: AbstractTransactionManager

        @param force: whether the environment should be initialized
                      even if it is initialized before already.
        @type force: bool
        """
        if not force:
            assert cls.tr_manager is None, repr(cls.tr_manager)
            assert cls.app is None, repr(cls.app)

        if force or (cls.tr_manager is None and cls.app is None):
            # Creating the transaction manager singleton
            cls.tr_manager = tr_manager
            cls.app = app

            logger.debug('Just %screated the transaction manager %r for %r.',
                         're' if force else '', cls.tr_manager, cls.app)


    @classmethod
    def deinit_environment(cls):
        """
        Deinitialize the common environment for any transport.
        """
        if cls.tr_manager is None:
            logger.warning('In deinit_environment(), cls.tr_manager is None')
        if cls.app is None:
            logger.warning('In deinit_environment(), cls.app')
        cls.tr_manager = None
        cls.app = None

        logger.debug('Just deinitialized the transaction manager.')


    @staticmethod
    @contract_epydoc
    def update_headers(headers, extra_headers):
        """
        Update the HTTP headers object, altering the previous values
        (if some key exists) to the new ones, or adding
        the new key/value pairs.

        @param headers: dict object which should be updated
                        with some extra header values.
        @type headers: InsensitiveDict

        @param extra_headers: The extra headers,
                              mapping from the header name to the single value.
        @type extra_headers: dict, InsensitiveDict
        """
        for k, v in extra_headers.iteritems():
            assert isinstance(v, col.Iterable), repr(v)
            headers[k] = headers.get(k, []) + v


    @exceptions_logged(logger)
    @contract_epydoc
    def initialize_headers(self, headers):
        """
        @param headers: dict object which should be updated
                        with the default header values.
        @type headers: InsensitiveDict
        """
        HTTPTransport.update_headers(headers,
                                     {'Content-Type':
                                          ['application/octet-stream']})


    @staticmethod
    @exceptions_logged(logger)
    def parse_pragma_header(pragma):
        """
        An utilitary function for parsing Pragma headers.

        @param pragma: The contents of the 'Pragma:' HTTP header.
        @type pragma: str

        @return: The Pragma contents parsed to the dictionary.
        @rtype: dict
        """
        return dict(i.split('=', 1) for i in pragma.split(','))


    @abstractmethod
    @exceptions_logged(logger, ignore=MessageProcessingException)
    def create_message_from_headers(self, headers, auth_peer):
        """
        @raises MessageProcessingException:
            if the message could not be processed properly.
        """
        pass


    @contract_epydoc
    def _create_message_from_headers(self, headers, auth_peer,
                                     me, known_inhs,
                                     must_know_peer=True, peer_proto=None):
        """
        Given the headers of the message, the "myself" reference
        to the receiving inhabitant, as well as the container of other
        known inhabitants, create a message.

        @param headers: The headers of the message.
        @type headers: InsensitiveDict

        @param auth_peer: What inhabitant authenticated as the originator of
                          the message?
        @type auth_peer: NoneType, AbstractInhabitant

        @param me: The inhabitant object of the system processing this message.
        @type me: AbstractInhabitant

        @param known_inhs: The dictionary-like mapping the UUIDs to other known
                           inhabitants of the system.

        @param must_know_peer: Whether we must know the peer beforehand to
                               process the message.
                               At the moment, it is used on the host, when it
                               receives the messages from unknown other hosts.
                               In the future, we ALWAYS must know the peer.
        @type must_know_peer: bool
        @todo: As soon as the Host->Host connections are preceded with the
               connection from the Node to the target Host (which notifies the
               target Host about the expected connection), C{must_know_peer}
               argument should be removed (and the code kept in the
               C{must_know_peer = True} position).

        @param peer_proto: The function which creates a peer object prototype
            depending upon the available information, if the peer is unknown.
            Default value is a subclass of C{AbstractInhabitant} constructor.
            Must accept all the usual C{AbstractInhabitant} constructor
            arguments.
        @todo: Remove peer_proto as long as must_know_peer is removed.

        @returns: The new message object.
        @rtype: AbstractMessage

        @raises MessageProcessingException: if the message could not be
                                            processed properly.
        """
        if peer_proto is None:
            peer_proto = AbstractInhabitant

        # Validate HTTP request
        pragma_data_list = headers.get('Pragma', None)
        if pragma_data_list is None or not pragma_data_list:
            raise MessageProcessingException('No message type header')

        assert isinstance(pragma_data_list, list)
        if len(pragma_data_list) != 1:
            raise MessageProcessingException(
                      'Message type header is misconstructed')

        pragma_data = HTTPTransport.parse_pragma_header(pragma_data_list[0])

        # Validate again
        if 'msgtype' not in pragma_data:
            raise MessageProcessingException('No message type')
        if 'msgid' not in pragma_data:
            raise MessageProcessingException('No message ID')
        if 'from' not in pragma_data:
            raise MessageProcessingException('No source peer UUID')
        if 'to' not in pragma_data:
            raise MessageProcessingException('No destination peer UUID')
        if 'status_code' not in pragma_data:
            logger.warning('No status code!')
            # TODO: after some time (when all clients have been migrated),
            # change it to "raise MessageProcessingException()" below.
            # raise MessageProcessingException('No status code')

        # Validate build version
        if 'build' in pragma_data:
            _build_str = pragma_data['build']
            try:
                self.validate_build(_build_str)
            except MessageProcessingException:
                raise
            except Exception:
                logger.exception('Received the unparseable build version %r:',
                                 _build_str)
                raise MessageProcessingException('Incorrect build version: {}'
                                                     .format(_build_str))

        # Validate status code
        try:
            status_code_txt = pragma_data.get('status_code', 0)
            int(status_code_txt)
        except:
            logger.error('Received the unparseable status_code %r:',
                         status_code_txt)
            # TODO: after some time (when all clients have been migrated),
            # change it to "raise MessageProcessingException()

        msg_type = pragma_data['msgtype']
        msg_uuid = uuid.UUID(pragma_data['msgid'])
        _from_uuid = uuid.UUID(pragma_data['from'])
        to_uuid = uuid.UUID(pragma_data['to'])
        status_code = int(pragma_data.get('status_code', 0))

        # Pre-validate origination peer
        if _from_uuid == NULL_UUID and auth_peer is None:
            raise MessageProcessingException('Missing source UUID: {}'
                                                 .format(_from_uuid))

        from_uuid = _from_uuid if _from_uuid != NULL_UUID \
                               else auth_peer.uuid
        # This still can be NULL_UUID at this point.
        if from_uuid == NULL_UUID:
            logger.debug('Totally no idea what host does %r have', auth_peer)

        # Validate termination UUID
        if me.uuid in (to_uuid, NULL_UUID):
            direct = True
            dst_inh = me
        elif to_uuid in known_inhs:
            direct = False
            dst_inh = known_inhs[to_uuid]
            logger.debug('Passthrough connection from %r to %r',
                         from_uuid, to_uuid)
        else:
            raise MessageProcessingException('Unknown destination UUID: {}'
                                                 .format(to_uuid))

        # Now create the AbstractMessage object according
        # to the received information.

        # Some more validation of the origination peer
        if auth_peer is not None:
            logger.debug('Assuming sender/auth_peer is %r', auth_peer)
            src_inh = auth_peer
        elif from_uuid not in known_inhs and must_know_peer:
            raise MessageProcessingException('Unknown source UUID: {}'
                                                 .format(from_uuid))
        else:
            logger.debug('Assuming sender/uuid is %r', from_uuid)
            # Try to get the peer somehow
            src_inh = known_inhs.get(from_uuid, None)
            if src_inh is None:
                src_inh = peer_proto(uuid=from_uuid)

        logger.debug('Finally, sender is %r', src_inh)

        assert self.tr_manager is not None
        message = self.tr_manager.create_message(name=msg_type,
                                                 src=src_inh,
                                                 dst=dst_inh,
                                                 uuid=msg_uuid,
                                                 direct=direct,
                                                 status_code=status_code)

        message.init_from_headers(InsensitiveDict(headers))

        return message


    @exceptions_logged(logger)
    def validate_build(self, build_string):
        """
        This method is assumed to be overriden in subclasses,
        to perform the necessary validation of the build string
        (whenever present in the message).

        May perform additional actions whenever build version inconsistencies
        are found.

        @param build_string: The originator-specific string
                             of the build version.
        @type build_string: str

        @returns: Whether the validation succeeded.
                  Default is True, override with actual implementation.
        @rtype: bool
        """
        return True


    @exceptions_logged(logger)
    def extra_pragma(self):
        """Get the extra contents for the HTTP "Pragma" header, if needed.

        May be freely overridden if required.
        """
        return ''



class HTTPServerTransport(DeferredProcessingResource, HTTPTransport):
    """The HTTP transport acting as an HTTP server.

    @ivar __response_msg: the response message while being sent out.
        If any issue (such as disconnection) occured, it may need to be
        reposted to the transaction manager.
    @todo: implement logic of C{__response_msg}, if needed.
    """
    __metaclass__ = ABCMeta

    __slogs__ = ('__response_msg',)


    @exceptions_logged(logger)
    def __init__(self, avatar):
        """
        @todo: This should be streamlined using super() as soon
               as twisted.web.resource.Resource becomes a new-style class.
        """
        logger.debug('****************HTTPServerTransport.__init__')
        #super(HTTPServerTransport, self).__init__(*args,
        #                                          **kwargs)
        DeferredProcessingResource.__init__(self, avatar)
        HTTPTransport.__init__(self)

        self.__response_msg = None


    @staticmethod
    def init_environment(app, tr_manager, force=False):
        """
        Initialize the common environment for any HTTP server.
        """
        HTTPTransport.init_environment(app, tr_manager, force)


    @staticmethod
    def deinit_environment():
        """
        Deinitialize the common environment for any HTTP server.
        """
        HTTPTransport.deinit_environment()


    @abstractmethod
    @exceptions_logged(logger)
    def _accept_passthrough_message(self, message):
        """
        Override this method with the implementation
        that verifies whether the passthrough message is allowed.

        The example implementation forbids the passthrough messages
        no matter of their contents.

        @returns: Whether the incoming message is accepted for passthrough.
        @rtype: bool
        """
        return False


    @exceptions_logged(logger)
    def _handle_connection_close(self, failure):
        """Overrides the method from C{DeferredProcessingResource}.

        @todo: use C{super()} as soon as a Twisted's C{resource.Resource}
            becomes a new-style class.

        @type failure: Failure
        """
        DeferredProcessingResource._handle_connection_close(self, failure)


    @defer.inlineCallbacks
    @exceptions_logged(logger)
    @contract_epydoc
    def _render(self, request, avatar):
        """Node-specific request processing.

        @param request: Web server request.
        @type request: Request

        @type avatar: Host

        @returns: nothing (not a C{Deferred} object!).
            But, due to C{inlineCallbacks}, yields the intermediate
            C{Deferred}s.

        @todo: Proper error handling.

        @todo: Properly handle the situation when the passthrough is
               restricted: generate the reply which contains the Error 404.

        @todo: This should be streamlined using super() as soon
               as C{twisted.web.resource.Resource} becomes a new-style class
        """
        assert not in_main_thread()
        # Kind-of-calling the parent, but the parent in this case should
        # return a Deferred as well,... let's not bother with it.
        # DeferredProcessingResource._render(self, request, avatar)

        logger.debug('')
        logger.debug('')
        logger.debug('Avatar %r;'
                         '\tincoming headers %r;'
                         '\tfrom %r',
                     avatar, request.requestHeaders, request.client)

        # We setup the default headers for response even before
        # we start analyzing the incoming message,
        # so that if analyzing fails for some reason, we have the headers
        # and can create a more-or-less proper error reply.
        _requestHeaders = InsensitiveDict(dict(request.requestHeaders
                                                      .getAllRawHeaders()))

        _responseHeaders = InsensitiveDict()
        self.initialize_headers(_responseHeaders)

        message = self.create_message_from_headers(headers=_requestHeaders,
                                                   auth_peer=avatar)

        message.body_receiver = RequestBodyReceiver(message=message,
                                                    request=request)

        # Make sure the body is received, before going further
        dummy_received_data = yield message.body_receiver.on_finish

        assert not in_main_thread()

        if message.direct:
            # If this is the message directed to ourselves,
            # process it and send the response
            logger.debug('')
            logger.debug('> Der Mitteilung Nahert: %r von %s',
                         message, message.src)

            # Process the incoming message
            try:
                self.tr_manager.handle_incoming_message(message)
            except TransactionProcessingException as e:
                # The incoming transaction cannot be processed.

                # TODO: handle error properly, send a error reply maybe
                logger.error('Error (%r) during incoming msg processing: %s',
                             e, e)

            logger.debug('Will send response to %r from %r %s',
                         message, message.src, hash(message.src))
            for k, v_list in _responseHeaders.iteritems():
                request.responseHeaders.setRawHeaders(k, v_list)
            logger.debug('Done with response headers')
        elif self._accept_passthrough_message(message):
            # Non-direct message, and passthrough allowed:
            # wait for the body, then put in the queue.

            logger.debug('Passthrough message %r: body already available',
                         message)
            self.tr_manager.post_message(message)

        else:
            # Passthrough is restricted for this message!
            raise NotImplementedError(u'Failed message {!r}:\n'
                                           u'from: {!r}\n'
                                           u'  to: {!r}'.format(message,
                                                                message.src,
                                                                message.dst))

        # How to check if we still need a message?
        still_need_checker = lambda: self.connection_alive

        # No matter if we are processing the message or passing it through,
        # we must send a response somehow.

        # If this is the message to us, we'd prefer a response to reply it;
        # but if this is a passthrough message, it will go to a different peer.
        prefer_msg_uuid = message.uuid if message.direct else None
        try:
            # Let's (asynchronously) wait for the response message here.
            resp_msg = self.__response_msg \
                     = yield self.tr_manager.wait_for_message_for_peer(
                                 inh=message.src,
                                 prefer_msg_uuid=prefer_msg_uuid,
                                 still_wait_checker=still_need_checker)
        except internet_error.ConnectionClosed as e:
            logger.warning('Stopped to wait for a message due to %r', e)
            self._handle_connection_close(Failure(e))
        except Exception as e:
            logger.exception('Stopped to wait for a message due to %r', e)
            self._handle_connection_close(Failure(e))
        else:
            # Since now and till the end of transfer we store the message
            # in self.__response_msg.

            assert not in_main_thread()

            if resp_msg is None:
                # The connection died already, while waiting for the message.
                assert not self.connection_alive

            else:
                # Got the message, send it back
                assert isinstance(resp_msg, AbstractMessage), repr(resp_msg)
                self.__send_http_response(message.src, request, resp_msg)


    @contract_epydoc
    def __send_http_response(self, inh, request, response_msg):
        """
        When we've know what message to send back to the HTTP client
        which has connected to the HTTP server, we issue this message back.

        @param inh: an inhabinant who will receive the response
        @type inh: AbstractInhabitant

        @param request: web server request.
        @type request: Request

        @param response_msg: response message to send.
        @type response_msg: AbstractMessage
        """
        if not self.connection_alive:
            logger.debug('Connection died inside %r while trying '
                             'to reply with %r',
                         self, response_msg)
            # That's very very bad! We should put the message back to queue.
            self.tr_manager.post_message(response_msg)
        else:
            logger.debug('Sending response %r to %r',
                         response_msg, request.requestHeaders)

            response_msg.in_transfer = True

            # Add the default server-specific headers,
            # then the message-specific headers.
            _headers = InsensitiveDict(dict(request.responseHeaders
                                                   .getAllRawHeaders()))
            _pragma = ','.join(ifilter(None, [response_msg.get_pragma(),
                                              self.extra_pragma()]))
            HTTPTransport.update_headers(_headers, {'Pragma': [_pragma]})
            HTTPTransport.update_headers(_headers, response_msg.get_headers())
            for k, v_list in _headers.iteritems():
                request.responseHeaders.setRawHeaders(k, v_list)

            logger.debug('New headers for %r are: %r',
                         response_msg, request.responseHeaders)


            @contract_epydoc
            def produce_body_in_main(body, msg):
                """
                @type body: col.Iterable
                @type msg: AbstractMessage
                """
                assert in_main_thread()

                try:
                    producer = LazyPullProducer(request, body)
                    producer.completed.addBoth(self._ready_to_finish.callback)
                except Exception:
                    logger.exception('Error during writing back the request:')
                    # We haven't succeeded to send the message to the client.
                    # Put it back to the queue.
                    callInThread(self.tr_manager.post_message, msg)


            assert not in_main_thread()  # get_body() may take long
            _body = response_msg.get_body()
            logger.debug('Writing body: %r', response_msg)

            callFromThread(produce_body_in_main, _body, response_msg)



class HTTPClientTransport(HTTPTransport):
    """
    The HTTP transport acting as an HTTP client.

    @ivar host: the Host object which stores the information
        about the local host.
    @type host: Host

    @note: It already has such fields as C{.tr_manager} available.
    """

    __slots__ = ('host', 'primary_node')


    def __init__(self, host, primary_node):
        self.host = host
        self.primary_node = primary_node


    def reconfigure_host(self, host):
        """
        At some point (login?) we may need to reconfigure the active host,
        but this may be done only by switching from NULL_UUID to non-NULL UUID.
        """
        assert self.host.uuid == NULL_UUID, repr(self.host)
        self.host = host
