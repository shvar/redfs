#!/usr/bin/python
"""
The main process running for a Host instance.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import gc
import json
import logging
import numbers
import operator
import os
import posixpath
import smtplib
import sys
import traceback
import zipfile
import glob
from collections import deque
from contextlib import closing
from cStringIO import StringIO
from datetime import datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import chain, ifilter, imap
from functools import partial
from operator import attrgetter
from os.path import abspath, basename, exists, isdir, isfile
from types import GeneratorType, NoneType
from uuid import UUID

from zope.interface import implements

from OpenSSL import crypto

import twisted
from twisted.application.internet import TimerService
from twisted.cred import error as cred_error
from twisted.cred.checkers import ICredentialsChecker, ANONYMOUS
from twisted.internet import reactor, threads, error as internet_error, defer
from twisted.python.failure import Failure
from twisted.python.util import InsensitiveDict
from twisted.web import http
from twisted.web.http_headers import Headers
from twisted.web import error as web_error

from contrib.dbc import contract_epydoc, consists_of

from common import (
    logger as common_logger, datatypes, os_ex, settings as common_settings,
    ssl, hacks, version)
from common.abstract_dataset import AbstractDataset
from common.abstractions import AbstractApp, AbstractMessage, IPrintable
from common.abstract_transaction_manager import TransactionProcessingException
from common.abstractions import AbstractApp, AbstractMessage
from common.bodyreceiver import ResponseBodyReceiver
from common.build_version import get_build_timestamp, timestamp_to_local_time
from common.chunk_storage import IChunkStorage
from common.chunks import (
    Chunk, ChunkFromFiles, EncryptedChunkFromFiles, FILTERS_TO_WILDCARDS)
from common.client import (
    AbstractHTTPDigestMgr, HTTPClientFactory_wDigest, get_page_wDigest)
from common.crypto import Cryptographer
from common.datasets import DatasetOnPhysicalFiles
from common.datatypes import LogReportingSettings
from common.db import Queries
from common.http_transport import (
    HTTPServerTransport, HTTPClientTransport,
    MessageProcessingException, UnsupportedBuildException)
from common.inhabitants import Host, Node, UserGroup
from common.itertools_ex import inonempty
from common.path_ex import relpath_nodot, normpath_nodot
from common.server import Server
from common.ssl_credential_factory import ISSLCertificate, SSLCredentials
from common.traffic_meter import TrafficMeter
from common.transaction_manager_in_memory import TransactionManagerInMemory
from common.twisted_utils import (
    callFromThread, callInThread, callLater, callLaterInThread)
from common.typed_uuids import DatasetUUID, UserGroupUUID
from common.utils import (
    coalesce, exceptions_logged, gen_uuid, get_mem_usage_dump, open_wb,
    repr_maybe_binary, duration_logged, NULL_UUID, in_main_thread, audit_leaks)

from uhost import settings as untrusted_host_settings

from protocol import messages

from . import (
    settings as host_settings, transactions, db, editions as host_editions)
from .auth_tokens import AuthTokenStore
from .chunk_progress import ChunkProgressNotificator
from .db import HostQueries
from .hostapp_api_async import HostAppAsyncAPIMixin
from .sessions import (
    SessionTracker, BoundOutgoingSession, BoundIncomingSession)
from .user_messages import Inbox

# TODO: ticket:143 - conflicts with signals
TMP_ZMQ_DISABLED = True

if TMP_ZMQ_DISABLED:
    ZMQ_AVAILABLE = False
else:
    # PyZMQ should be imported but only if available
    try:
        import zmq
    except ImportError:
        ZMQ_AVAILABLE = False
    else:
        ZMQ_AVAILABLE = True
        from common.zmq_ex import ZMQProtocol



#
# Constants
#

logger = logging.getLogger(__name__)
logger_status = logging.getLogger('status.host_process')
logger_status_chunks_hash = logging.getLogger('status.chunk_hash_calc')
logger_status_backup = logging.getLogger('status.backup')
logger_status_version = logging.getLogger('status.version')

AFTER_REACTOR_START = timedelta(seconds=1)
WAIT_ON_HEARTBEAT_ERROR = timedelta(seconds=5)
OUTGOING_MESSAGE_POLL_PERIOD = timedelta(seconds=1)
DROP_HEARTBEATS = 1  # We want to drop some first heartbeats. How many?

AUTO_START_BACKUPS_AFTER = timedelta(seconds=1) - AFTER_REACTOR_START
"""
After launching the HostApp main loop, how much time must pass
before the non-completed backups are auto-started?
"""

try:
    from host_magic import GLOBAL_AUDIT_PERIOD  # pylint:disable=F0401
except ImportError:
    GLOBAL_AUDIT_PERIOD = timedelta(minutes=1)

try:
    from host_magic import SEND_LOGS_PERIOD  # pylint:disable=F0401
except ImportError:
    SEND_LOGS_PERIOD = timedelta(minutes=5)

try:
    from host_magic import FORCE_PASSTHROUGH  # pylint:disable=F0401
except ImportError:
    FORCE_PASSTHROUGH = False

HOW_MANY_HEARTBEATS_FOR_EACH_NEED_INFO = 50

_DEBUG_EXIT_ON_INCOMING_CONNECTION = False
"""
Use only for debugging, when need to test that the incoming connection fails.
Setting to C{True} causes any incoming connection to break
the application immediately.
"""



#
# Classes
#

class HostResource(HTTPServerTransport):
    """
    The host acting as an HTTP server; a new instance is created
    for every request. Technically, this is an C{IResource}.
    """

    VERSION_STRING = host_settings.HOST_VERSION_STRING


    @exceptions_logged(logger)
    def __init__(self, avatar):
        """
        @todo: This should be streamlined using super() as soon
               as twisted.web.resource.Resource becomes a new-style class.
        """
        #super(HostServer, self).__init__(*args, **kwargs)
        HTTPServerTransport.__init__(self, avatar=avatar)


    @classmethod
    @exceptions_logged(logger)
    def init_environment_for_host_server(cls, app, known_peers, force):
        """
        Initialize the common environment for the whole host.
        """
        HTTPServerTransport.init_environment(
            app=app,
            tr_manager=TransactionManagerInMemory(
                           tr_classes=transactions.ALL_TRANSACTION_CLASSES,
                           app=app),
            force=force)

        cls.known_peers = known_peers


    @classmethod
    def init_environment_perprocess(cls, listen_port):
        """
        Initialize the environment on a per-process level.
        """
        # In fact, the cls.host is already available
        # by the init_environment time.
        logger.debug('Initializing perprocess http-server environment '
                         'for %s at port %i',
                     cls.app.host,
                     listen_port)


    @exceptions_logged(logger)
    def create_message_from_headers(self, headers, auth_peer=None):
        """Overrides the parent method.

        @note: host acts as an HTTP server.

        @todo: See the todo comment in the common/http_transport.py,
               HTTPTransport._create_message_from_headers()
        """
        if _DEBUG_EXIT_ON_INCOMING_CONNECTION:
            logger.error('Breaking the connection')
            os._exit(0)

        # If we don't know the receiver but received something from the node,
        # this is probably a pass-through message from another host.
        # Let's reply using pass-through too.
        # TODO: can it ever happen on host-as-a-server? Probably do, but...
        _peer_proto = partial(Node,
                              urls=self.app.primary_node.urls)
        return self._create_message_from_headers(headers=headers,
                                                 auth_peer=auth_peer,
                                                 me=self.app.host,
                                                 known_inhs=self.known_peers,
                                                 must_know_peer=False,
                                                 peer_proto=_peer_proto)


    @exceptions_logged(logger)
    def _accept_passthrough_message(self, message):
        """
        Overrides the related method from the HTTPServerTransport.
        """
        return False


    @exceptions_logged(logger)
    @contract_epydoc
    def _handle_error(self, request, failure):
        """
        In this function, the host acts as an HTTP server and performs
        host-server-specific error processing.

        @type failure: Failure
        """
        super(HostResource, self)._handle_error(request, failure)

        logger.error('failure: %r', failure)

        exc = failure.value
        if isinstance(exc, MessageProcessingException):
            # We have some extra information
            level = exc.level
            message = exc._message
            assert isinstance(message, AbstractMessage), repr(message)
            status_type = 'message_processing_error'
            msg_pragma = message.get_pragma()

        else:
            level = logging.CRITICAL
            message = str(exc)
            status_type = 'unknown_error'
            msg_pragma = None

        logger_status.log(level, message,
                          extra={'_type': status_type})

        if isinstance(exc, MessageProcessingException):
            # We know the details about the originating message,
            # so can construct the Pragma headed in the response.
            # Otherwise it will be empty!
            _pragma = ','.join(ifilter(None,
                                       [msg_pragma,
                                        self.extra_pragma()]))
            request.headers.update({'Pragma': _pragma})

        if isinstance(exc, (MessageProcessingException,
                            TransactionProcessingException)):
            request.setResponseCode(http.BAD_REQUEST)
            # This is more-or-less controlled exception,
            # like incoming message parsing failure.
            result_struct = {'error': {'text': str(exc),
                                       'level': level}}

            # pylint:disable=E1101,C0103
            callFromThread(request.write, json.dumps(result_struct))
        else:
            # This is completely unexpected exception, like a logic error.

            # Let's re-raise the exception to get its traceback
            try:
                failure.raiseException()
            except Exception as e:
                logger.exception('VERY BAD ERROR %s', e)



class HostDigestMgr(AbstractHTTPDigestMgr):
    """
    The class capable of providing the username/digest
    for any HTTP authentication realm.
    """
    def __init__(self, app):
        self.app = app


    def __repr__(self):
        return u'<HostDigestMgr for {!r}>'.format(self.app.host)


    @contract_epydoc
    def find_user_digest(self, realm, authuri):
        """
        @returns: a tuple of (username, digest).
        @rtype: tuple
        """
        _host = self.app.host
        _user = _host.user

        logger.debug('%r is authenticating at realm %r with %r',
                     _host, realm, self)

        if self.app.is_started and self.app.do_send_messages:
            if realm == common_settings.HTTP_AUTH_REALM_NODE:
                logger.debug('Authenticating %r for node connection with %r',
                             _host, self)

                return (_user.name, _user.digest)
            elif realm == common_settings.HTTP_AUTH_REALM_HOST:
                # When connecting to a host, use empty username and digest,
                # as the host connection is authenticated only via SSL
                logger.debug('Authenticating %r for host connection with %r',
                             _host, self)
                return ('', '')
            else:
                return ANONYMOUS
        else:
            logger.debug('The host %r already shut down, '
                             'so do not authenticate.',
                         _host)
            return ANONYMOUS



class HostSSLChecker(object):
    """
    The verificator of SSL/TLS authentication information on the Host.

    @cvar credentialInterfaces: required for ICredentialsChecker
    """
    __slots__ = ('__app',)

    implements(ICredentialsChecker)

    credentialInterfaces = (ISSLCertificate, )


    def __init__(self, app, *args, **kwargs):
        super(HostSSLChecker, self).__init__(*args, **kwargs)
        self.__app = app


    @exceptions_logged(logger)
    @contract_epydoc
    def requestAvatarId(self, creds):
        """
        @note: required for ICredentialCshecker

        @type creds: SSLCredentials

        @returns: as per the required interface, the C{Deferred} that fires
                  the host uuid string, or the string itself.
        """
        # On any error, consider it UnauthorizedLogin
        try:
            _subj = creds.cert.get_subject()
            assert _subj.OU == ssl.CERT_OU_HOST, \
                   repr(_subj.get_components())

            logger.debug('SSL: getting avatar from client %r', _subj.CN)
            # Only a host can send incoming message... but may it be a node?
            _peer = Host(uuid=UUID(_subj.CN))
            _session = BoundIncomingSession(tracker=self.__app.session_tracker,
                                            peer=_peer)
            _session.connected()

            return _subj.CN

        except Exception:
            logger.exception('SSL: could not get avatar')
            return defer.fail(cred_error.UnauthorizedLogin())



class Host_UnsupportedBuildException(UnsupportedBuildException):
    """
    The exception which happens when a host has an unsupported build,
    according to the reply from the node.
    """
    __slots__ = ('host_build', 'node_build',
                 'required_build', 'recommended_build')


    def __init__(self,
                 host_build, node_build,
                 required_build, recommended_build,
                 *args, **kwargs):
        r"""Constructor.

        >>> e = Host_UnsupportedBuildException(
        ...         host_build='1234',
        ...         node_build='5678',
        ...         required_build='abcd',
        ...         recommended_build='efgh')
        >>> e
        Host_UnsupportedBuildException('1234', '5678', 'abcd', 'efgh')

        >>> # Must be pickleable
        >>> import cPickle as pickle
        >>> s = pickle.dumps(e, pickle.HIGHEST_PROTOCOL)
        >>> s  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        '\x80\x02chost.host_process\nHost_UnsupportedB...\x04efghq\x05tRq\x06.'
        >>> # ... and unpickleable!
        >>> pickle.loads(s)
        Host_UnsupportedBuildException('1234', '5678', 'abcd', 'efgh')
        """
        self.host_build = host_build
        self.node_build = node_build
        self.required_build = required_build
        self.recommended_build = recommended_build
        super(Host_UnsupportedBuildException, self).__init__(text=str(self),
                                                             *args, **kwargs)


    def __str__(self):
        """
        >>> str(Host_UnsupportedBuildException(
        ...         host_build='1234',
        ...         node_build='5678',
        ...         required_build='abcd',
        ...         recommended_build='efgh'))
        'Unsupported build 1234 against abcd'
        """
        return 'Unsupported build {} against {}'.format(self.host_build,
                                                        self.required_build)


    def __repr__(self):
        """
        >>> Host_UnsupportedBuildException(
        ...         host_build='1234',
        ...         node_build='5678',
        ...         required_build='abcd',
        ...         recommended_build='efgh')
        Host_UnsupportedBuildException('1234', '5678', 'abcd', 'efgh')
        """
        return u'{}({!r}, {!r}, {!r}, {!r})'.format(
                   self.__class__.__name__,
                   self.host_build,
                   self.node_build,
                   self.required_build,
                   self.recommended_build)


    def __reduce__(self):
        """Magic method for pickling."""
        return (Host_UnsupportedBuildException,
                (self.host_build,
                 self.node_build,
                 self.required_build,
                 self.recommended_build))



class Host_LoginException(IPrintable, Exception):
    """
    Exception covering any issue during the login.
    """

    __slots__ = ('result', )


    def __init__(self, result):
        r"""Constructor.

        >>> e = Host_LoginException(result='oopsie')
        >>> e
        Host_LoginException(result='oopsie')


        >>> # Must be pickleable
        >>> import cPickle as pickle
        >>> s = pickle.dumps(e, pickle.HIGHEST_PROTOCOL)
        >>> # ... and unpickleable!
        >>> pickle.loads(s)
        Host_LoginException(result='oopsie')
        """
        assert isinstance(result, basestring) and result != 'ok', \
               repr(result)
        self.result = result


    def __str__(self):
        r"""Implement C{IPrintable} interface.

        >>> str(Host_LoginException(result='oopsie'))
        "result='oopsie'"

        >>> Host_LoginException(result='oopsie')
        Host_LoginException(result='oopsie')
        """
        return u'result={self.result!r}'.format(self=self)


    def __reduce__(self):
        """Magic method for pickling."""
        return (Host_LoginException,
                (self.result,))



if ZMQ_AVAILABLE:

    class MasterHostZMQControlProtocol(ZMQProtocol):

        __slots__ = ('app',)


        @contract_epydoc
        def __init__(self, app, *args, **kwargs):
            """
            The constructor is enhanced comparing to the regular ZMQ protocols,
            and contains an "app" argument.
            So, when passing to the reactor.bindZeroMQ(),
            it should be partially evaluated.

            @type app: HostApp
            """
            super(MasterHostZMQControlProtocol, self).__init__(*args, **kwargs)
            self.app = app


        @exceptions_logged(logger)
        def messageReceived(self, raw_message):
            message = json.loads(raw_message)
            logger.debug('ZMQ received %r at %r', message, self.app)
            res = json.dumps({'r': message['r'], 'new': message['r'] + 10})
            logger.debug('ZMQ sending back %r', res)
            self.transport.sendMessage(res)



class HostApp(HTTPClientTransport, AbstractApp, HostAppAsyncAPIMixin):
    """
    The main Host application entity in the backup system.
    Only a single instance of this object usually exists;
    contrary to the HostResource, which creates an instance
    for every incoming request.

    @ivar primary_node: the Node object of the primary node where this host is
        registered. May be None if this host is not yet registered on any Node.
    @type primary_node: NoneType, Node

    @ivar feature_set: the feature set.
    @type feature_set: host_editions.HostFeatureSet

    @ivar known_peers: the dictionary mapping the UUIDs of the
         AbstractInhabitant objects known to this host
         to the objects themselves.
    @type known_peers: dict

    @ivar chunk_storage: the chunk storage which is responsible
                         for reading and writing the chunks contents.
    @type chunk_storage: IChunkStorage

    @ivar session_tracker: the tracker of sessions status.
    @type session_tracker: SessionTracker

    @ivar traffic_meter_in: the traffic meter for all incoming traffic.
    @type traffic_meter_in: TrafficMeter

    @ivar traffic_meter_out: the traffic meter for all outgoing traffic.
    @type traffic_meter_out: TrafficMeter

    @ivar last_settings_sync_time: the last moment when the settings
                                   were successfully synced with the node.
    @type last_settings_sync_time: datetime

    @ivar known_builds: the tuple/list with the build versions known/last
                        received from the node;
                        the first item is the node own build,
                        the second item is the required host build,
                        the third item is the recommended host build.
    @type known_builds: list
    @invariant: len(known_builds) == 3 and \
                consists_of(known_builds, str) and \
                all(len(b) == 8 for b in known_builds)

    @ivar stopped: a C{Deferred} object which is fired whenever the HostApp
                   is stopped.
    @type stopped: defer.Deferred

    @ivar __cooling_down_to_backup: the set of the files which are cooling down
        after some changes has been done, before their backup is launched.
    @type __cooling_down_to_backup: col.MutableSet

    @ivar __backup_cooled_down_files_delayed_call: the delayed call which
        should be executed whenever the oldest file has cooled down enough
        to start backup.
    @type __backup_cooled_down_files_delayed_call: IDelayedCall, NoneType
    """

    __slots__ = ('primary_node', '__primary_node_certificate', 'feature_set',
                 'known_peers', 'chunk_storage', 'progress_notificator',
                 'session_tracker', 'traffic_meter_in', 'traffic_meter_out',
                 'hb_count', '__hb_to_drop',
                 '__all_timer_services', '__normal_mode_timer_services',
                 'last_settings_sync_time',
                 '__on_reactor_start_cb', 'auth_manager',
                 'known_builds',
                 'is_started', 'do_send_messages', 'do_heartbeats_revive',
                 'stopped',
                 'server', 'server',
                 '__backup_auto_starter',
                 'auto_start_backup',
                 '__cooling_down_to_backup',
                 '__backup_cooled_down_files_delayed_call')

    VERSION_STRING = host_settings.HOST_VERSION_STRING


    HOST_BUILD = '00000000'  # Not known until .first_start() is launched,
                             # and the data is read from the file

    # A class variable to make sure
    # that the other class variables are already initialized as well,
    _STARTED_ONCE = False  # until .first_start() is completed


    class HostInitExceptions(object):

        class BaseHostInitException(Exception):
            """
            Any exception during the host initialization.
            """
            __slots__ = ('base_exc',)

            def __init__(self, base_exc=None):
                self.base_exc = base_exc

        class IncorrectDefaultNodeSettingsException(BaseHostInitException):
            pass

        class NoNodeUrlsException(BaseHostInitException):
            pass

        class NoHostDbException(BaseHostInitException):
            pass

        class DBInitializationIssue(BaseHostInitException):
            pass

        class UnknownIssue(BaseHostInitException):
            pass


    @contract_epydoc
    def __init__(self, uuid, edition, chunk_storage,
                 on_reactor_start=None,
                 do_send_messages=True, do_auto_send_logs=False,
                 do_heartbeats_revive=True,
                 do_auto_start_backup=True):
        """C{HostApp} constructor.

        @type uuid: UUID

        @type edition: basestring
        @precondition: edition in host_editions.EDITIONS  # edition

        @type chunk_storage: IChunkStorage
        @param on_reactor_start: the callback function which is called
            as soon as reactor is started.
            It should accept the single argument, precisely the C{HostApp}.
        @precondition: on_reactor_start is None or callable(on_reactor_start)

        @param do_send_messages: will the regular heartbeats and other
            networkmessages be sent out?
            C{False} if you need the C{HostApp} but don't need the network.
        @param do_auto_send_logs: should the error/debug logs be autosent
            to the admins, after the app is switched to Normal Mode?
        @param do_heartbeats_revive: will the heartbeats be issued to "revive"
            the host on the node? C{Fail} if authentication.
        """
        # Make sure we can distinguish between unittest-running
        # and no-unittest-running.
        assert 'doctest' not in sys.modules, sys.modules.keys()
        assert 'unittest' not in sys.modules, sys.modules.keys()

        with db.RDB() as rdbw:
            _host = Queries.Inhabitants.get_host_with_user_by_uuid(uuid, rdbw)
        super(HostApp, self).__init__(
            host=_host,
            primary_node=host_settings.get_my_primary_node())

        self.feature_set = host_editions.EDITIONS[edition]
        logger.debug('For edition %r, got %r', edition, self.feature_set)
        self.server = None  # will be initialized
                                    # by the MultiProcessServer instance
        self.server = None

        self.do_send_messages = do_send_messages
        self.do_heartbeats_revive = do_heartbeats_revive
        self.chunk_storage = chunk_storage

        self.known_peers = {}

        self.session_tracker = SessionTracker(self)
        self.traffic_meter_in = TrafficMeter(is_direction_incoming=True)
        self.traffic_meter_out = TrafficMeter(is_direction_incoming=False)
        self.progress_notificator = ChunkProgressNotificator(self)

        # Use the following primary node
        _node = self.primary_node
        self.__primary_node_certificate = None
        self.known_peers.update({_node.uuid: _node})

        _hb_interval = HostQueries.HostSettings.get(Queries.Settings
                                                           .HEARTBEAT_INTERVAL)
        logger.debug('Will generate heartbeats once every %r seconds',
                     _hb_interval)
        logger.debug('Will poll for outgoing messages once every %s',
                     OUTGOING_MESSAGE_POLL_PERIOD)
        self.__all_timer_services = [
            TimerService(_hb_interval,
                         lambda: callInThread(self.__on_hb_timer)),
            TimerService(OUTGOING_MESSAGE_POLL_PERIOD.total_seconds(),
                         lambda: callInThread(self.__on_msg_timer)),
            TimerService(GLOBAL_AUDIT_PERIOD.total_seconds(),
                         lambda: callInThread(self.__on_audit_timer)),
        ]
        self.__normal_mode_timer_services = []

        if do_auto_send_logs and SEND_LOGS_PERIOD is not None:
            self.__normal_mode_timer_services.append(
                TimerService(SEND_LOGS_PERIOD.total_seconds(),
                             lambda: callInThread(self.__on_send_logs_timer)))

        self.last_settings_sync_time = datetime.min

        self.auth_manager = HostDigestMgr(self)

        self.hb_count = 0
        self.__hb_to_drop = DROP_HEARTBEATS

        if on_reactor_start is not None:
            assert callable(on_reactor_start), repr(on_reactor_start)
        self.__on_reactor_start_cb = on_reactor_start

        # Not actual until the first reply from the node
        self.known_builds = ['00000000'] * 3

        self.is_started = False
        self.stopped = defer.Deferred()

        self.__backup_auto_starter = None

        self.auto_start_backup = do_auto_start_backup

        self.__cooling_down_to_backup = set()

        self.__backup_cooled_down_files_delayed_call = None

        self.inbox = Inbox()
        self.auth_token_store = AuthTokenStore()


    def __repr__(self):
        return u'<HostApp for {!r}>'.format(self.host)


    @classmethod
    @contract_epydoc
    def proceed_with_host_uuid(cls,
                               my_uuid,
                               group_uuid=NULL_UUID,
                               do_create_db=False,
                               init_username=None,
                               init_digest=None,
                               init_listen_port=None):
        """
        Given the host UUID, setup the global variables so that the host UUID
        is current now.
        This code is common between any CLI or GUI.

        @param my_uuid: host UUID
        @type my_uuid: UUID

        @param group_uuid: user group UUID
        @type group_uuid: UUID

        @param do_create_db: Whether the database file should be created
                             if absent (default: False).

        @raises common_settings.UnreadableConfFileException:
            if the .conf-file could not be read at all.
        @raises HostInitExceptions.BaseHostInitException: on some error.
        """
        # Read and validate the settings
        node_settings = untrusted_host_settings.get_default_node_settings()
        if node_settings is None:
            raise HostApp.HostInitExceptions \
                         .IncorrectDefaultNodeSettingsException()

        node_uuid, node_urls = node_settings

        if not node_urls:
            raise HostApp.HostInitExceptions.NoNodeUrlsException()

        # We are ready to use this host.

        try:
            host_settings.proceed_with_host_uuid_ex(
                my_uuid,
                group_uuid,
                init_username,
                init_digest,
                init_listen_port,
                node_uuid,
                node_urls,
                do_reconfigure_host=False,
                do_create_db=do_create_db)

        except host_settings.NoHostDbException as e:
            raise HostApp.HostInitExceptions.NoHostDbException(e)

        except host_settings.DBInitializationIssue as e:
            raise HostApp.HostInitExceptions.DBInitializationIssue(e)

        except Exception as e:
            raise HostApp.HostInitExceptions.UnknownIssue(
                      traceback.format_exc())

        else:
            logger.debug('Initialized %s', my_uuid)


    @classmethod
    @contract_epydoc
    def init_host(cls,
                  edition, chunk_storage_cb, proceed_func, on_end_func,
                  username, digest, my_listen_port):
        """Initialize a particular host.

        @type edition: basestring

        @param chunk_storage_cb: a function that creates the chunk storage.
            Must return an instance of C{IChunkStorage}.
        @type chunk_storage_cb: col.Callable

        @type proceed_func: col.Callable
        @type on_end_func: col.Callable
        """
        assert edition in host_editions.EDITIONS, repr(edition)

        # We don't know a correct host UUID at the moment,
        # thus we are using the fake null UUID.
        my_uuid = NULL_UUID


        def _proceed(username_, uuid, group_uuid):
            proceed_func(uuid,
                         group_uuid,
                         do_create_db=True,
                         init_username=username_,
                         init_digest=digest,
                         init_listen_port=my_listen_port)


        @defer.inlineCallbacks
        @exceptions_logged(logger)
        @contract_epydoc
        def on_reactor_start(app):
            """
            @type app: HostApp
            """
            assert in_main_thread()


            @exceptions_logged(logger)
            def on_login_received(login_ack):
                """
                @todo: do we have time to fully finish here,
                    before C{app.terminate_host()} starts the massacre?
                """
                # Now, we terminate host as early as possible,
                # so that it won't send any extra heartbeats,..
                assert in_main_thread()

                d = app.terminate_host()
                # Let's start the terminating progress, but, while it's going,
                # we'll finish here.
                # Maybe we'll have enough time.

                ack_username = username

                try:
                    if not isinstance(login_ack, Failure):
                        (ack_result,
                         ack_username,
                         ack_host_uuid,
                         ack_user_groups,
                         ack_ssl_cert,
                         ssl_pkey) = login_ack

                        # Stopping to use the old UUID;
                        # also stopping the network activity.
                        # Should be done no matter of the ack_result.
                        host_settings.unproceed_with_host_uuid()

                        if ack_result == 'ok':
                            # We must reinitialize the database.

                            #app._stop() # we've terminated it already in fact
                            logger.debug('Now using %s RDB', ack_host_uuid)
                            # Which group UUID is mine?
                            mine_user_group = \
                                (gr
                                     for gr in ack_user_groups
                                     if gr.name == ack_username).next()
                            _proceed(ack_username,
                                     ack_host_uuid,
                                     mine_user_group.uuid)

                            with db.RDB() as rdbw:
                                # Now setting up
                                # the SSL certificate/private key
                                logger.debug('Writing new settings into %r',
                                             rdbw.rdb_url)

                                HostQueries.HostSettings.set(
                                    HostQueries.HostSettings.SSL_CERTIFICATE,
                                    ack_ssl_cert,
                                    rdbw=rdbw)
                                HostQueries.HostSettings.set(
                                    HostQueries.HostSettings.SSL_PRIVATE_KEY,
                                    ssl_pkey,
                                    rdbw=rdbw)
                                HostQueries.HostUsers.set_my_user_groups(
                                    ack_username,
                                    ack_user_groups,
                                    rdbw=rdbw)
                                logger.debug('Written result '
                                                 'for Login transaction.')

                except Exception as e:
                    logger.exception('Error on init_host')
                    login_ack = e


                on_end_func(ack_username, login_ack)


            assert in_main_thread()
            login_ack = yield app.query_login(username)

            callFromThread(on_login_received, login_ack)


        _proceed(username, my_uuid, NULL_UUID)

        chunk_storage = chunk_storage_cb()

        # Launch the main host app
        host_app = HostApp(my_uuid, edition, chunk_storage,
                           on_reactor_start=on_reactor_start,
                           do_heartbeats_revive=False)
        host_app.first_start()
        # But it is not yet started, until the reactor is launched as well.

        # Launch reactor
        host_app.start_reactor()


    def handle_transaction_before_create(self, tr, state):
        """Implements the @abstractmethod from C{AbstractApp}.

        @type tr: AbstractTransaction
        @type state: AbstractTransaction.State
        """
        logger.debug(u'%r will be created...', tr)


    def handle_transaction_after_create(self, tr, state):
        """Implements the @abstractmethod from C{AbstractApp}.

        @type tr: AbstractTransaction
        @type state: AbstractTransaction.State
        """
        logger.debug(u'%r just created!', tr)


    def handle_transaction_destroy(self, tr, state):
        """Implements the @abstractmethod from C{AbstractApp}.

        @type tr: AbstractTransaction
        @type state: AbstractTransaction.State
        """
        logger.debug(u'%r just destroyed!', tr)


    def __audit_leaks(self):
        """Perform various operations useful to audit leaks.

        Should not be called during normal process flow.
        """
        audit_leaks(str(self.host.uuid))


    @exceptions_logged(logger)
    def __on_hb_timer(self):
        """This function is called on every heartbeat timer tick.

        Do as little as possible here,
        ideally deferring the tasks to the threadpool.
        """
        assert not in_main_thread()

        if not self.do_heartbeats_revive:
            logger.debug('Heartbeat timer fired, but omitting a heartbeat')
        else:
            if self.__hb_to_drop > 0:
                self.__hb_to_drop -= 1
                logger.debug('Dropping heartbeat')
            else:
                gc.collect()
                # self.__audit_leaks()  # disabled for now
                self.__try_to_generate_heartbeat()


    @exceptions_logged(logger)
    def __on_msg_timer(self):
        """This function is called on every message timer tick.

        Do as little as possible here,
        ideally deferring the tasks to the threadpool.
        """
        assert not in_main_thread()

        self.__send_messages()


    @exceptions_logged(logger)
    def __on_audit_timer(self):
        """
        This function is called on every audit timer tick.
        """
        cls = self.__class__
        assert not in_main_thread()

        # Regular memory audit is heavy resource-consuming
        # so disabled for now.
        logger.info('----------------------------------------\n'
                    '\n'
                    'Regular host audits: %s\n'
                    '%s',
                    datetime.utcnow(),
                    #get_mem_usage_dump(),
                    cls._get_reactor_threadpool_stats())


    @staticmethod
    def _get_reactor_threadpool_stats():
        """
        Collect the stats of the reactor threadpool and return in textual form.

        @rtype: basestring
        """
        tp = reactor.getThreadPool()
        queue = list(tp.q.queue)
        waiters = list(tp.waiters)
        working = list(tp.working)
        threads = list(tp.threads)
        l_queue = len(queue)
        l_waiters = len(waiters)
        l_working = len(working)
        l_threads = len(threads)

        return ('Queue:   {tp.q.queue!r}\n'
                'waiters: {tp.waiters!r}\n'
                'workers: {tp.working!r}\n'
                'total:   {tp.threads!r}'
                    .format(tp=tp))


    def __get_settings_for_heartbeat(self):
        """
        Get settings to transport in the HEARTBEAT transaction.

        Also, it is assumed that when this function is called,
        it is done for the purpose of sending the settings,
        so when it is called, the last_settings_sync_time
        is reset to the current time.

        @rtype: dict
        """
        logger.debug('Checking for settings newer than %s',
                     self.last_settings_sync_time)
        new_settings = host_settings.get_all_settings_newer_than(
                           self.last_settings_sync_time)

        if new_settings:
            logger.debug('Some settings changed: %r', new_settings)

        # Special-case settings (not stored in setting table)
        _my_urls = new_settings['urls'] = \
            (list(host_settings.get_my_announced_urls()),
             datetime.min)
        logger.debug('Announcing urls: %r', _my_urls)

        # Remove the settings which should never be rewritten on the Host.
        for never_update_setting in chain(Queries.Settings
                                                 .SETTINGS_NODE_UPDATE_ONLY,
                                          HostQueries.HostSettings
                                                     .ALL_LOCAL_SETTINGS):
            if never_update_setting in new_settings:
                del new_settings[never_update_setting]

        self.last_settings_sync_time = datetime.utcnow()
        logger.debug('Setting last_settings_sync_time to %s',
                     self.last_settings_sync_time)
        return new_settings


    def __try_to_generate_heartbeat(self):
        """Create the HEARTBEAT transaction, if possible.

        Does not create a heartbeat, for example, when some other heartbeat
        is in the transfer already.

        Occurs on every heartbeat timer tick.
        """
        assert not in_main_thread()

        if self.is_started and self.do_send_messages:
            dst_node = self.primary_node

            logger.debug('Trying to make heartbeat from %r to %r',
                         self.host, dst_node)

            if dst_node is None:
                logger.critical('EPIC FAIL: not registered on any node')

            else:
                logger.debug('\n'
                             '\n'
                             '> - - - - - - - - Das Loop - - - - - - - - \n'
                             'Looking for settings newer than %s',
                             self.last_settings_sync_time)

                # We don't want to create a new heartbeat,
                # if any existing heartbeat is still sitting in the queue.
                all_hb_tr_states = \
                    self.tr_manager.get_tr_states(class_name='HEARTBEAT')
                all_hb_tr_uuids = (state.tr_uuid for state in all_hb_tr_states)

                any_other_hb_uuid_not_in_transfer = \
                    self.__find_any_hb_uuid_not_in_transfer(all_hb_tr_uuids)

                if any_other_hb_uuid_not_in_transfer is not None:
                    logger.debug('Pending outgoing heartbeat already '
                                     'present (%r), avoiding another one.',
                                 any_other_hb_uuid_not_in_transfer)
                else:
                    self.__generate_heartbeat_indeed(dst_node)


    def __find_any_hb_uuid_not_in_transfer(self, hb_uuids):
        """The UUID of any heartbeat which is not in transfer (or C{None}).

        @type hb_uuids: col.Iterable
        @rtype: UUID, NoneType
        """
        for hb_uuid in hb_uuids:
            tr = self.tr_manager.get_tr_by_uuid(hb_uuid)
            if tr is None:
                logger.debug('No heartbeat already with %r', hb_uuid)
            else:
                if not tr.message.in_transfer:
                    return hb_uuid

        return None


    def __generate_heartbeat_indeed(self, dst_node):
        # What settings should be sent out?.
        # We don't need to send settings on login phase, when the heartbeats
        # are not reviving.
        settings_getter = \
            self.__get_settings_for_heartbeat if self.do_heartbeats_revive \
                                              else dict

        hb_tr = self.tr_manager \
                    .create_new_transaction(name='HEARTBEAT',
                                            src=self.host,
                                            dst=dst_node,
                                            # HEARTBEAT-specific
                                            settings_getter=settings_getter)


        def hb_success(ignore_result):
            logger.debug('Heartbeat successful')

        def hb_error(failure):
            logger.debug('Heartbeat error: %r', failure)


        hb_tr.completed.addCallback(hb_success)
        hb_tr.completed.addErrback(hb_error)


        # Possibly, create NEED_INFO transaction to obtain the statistics
        if self.hb_count % HOW_MANY_HEARTBEATS_FOR_EACH_NEED_INFO == 1:
            if self.do_heartbeats_revive:
                self.query_overall_cloud_stats()
            else:
                logger.debug("Should've requested cloud stats but omitting")

        # Increase the heartbeat count
        self.hb_count += 1


    def __send_messages(self):
        assert not in_main_thread()

        _tr_manager = self.tr_manager
        dst_node = self.primary_node

        # Node communication is top priority.
        # Do we have any messages to deliver for the node? (loop over it)
        message_to_deliver = True

        while self.is_started and self.do_send_messages and message_to_deliver:
            # First try to deliver a message for the node;
            # otherwise, try to deliver a message for any host.
            message_to_deliver = \
                _tr_manager.deliver_message_for_peer(dst_node,
                                                     prefer_msg_uuid=None) or \
                _tr_manager.deliver_any_message()


            if message_to_deliver is not None:
                logger.debug('We have a message to send: %r',
                             message_to_deliver)

                @exceptions_logged(logger)
                def _could_not_send(failure, message):
                    logger.error('on __send_messages, %s occured', failure)
                    callInThread(_tr_manager.post_message,
                                 message)
                    return failure

                @exceptions_logged(logger)
                def _wrapped_send_message(message):
                    d = threads.deferToThread(
                            exceptions_logged(logger)(
                                self.__send_message_to_peer),
                            message)
                    d.addErrback(partial(_could_not_send, message=message))

                callFromThread(_wrapped_send_message,
                               message_to_deliver)



    @contract_epydoc
    def __good_ssl_cert_received(self, x509):
        """
        This callback function is passed to the HTTP getter,
        and it is called back as soon
        as the SSL certificate of the peer (we connect to) is received.

        @note: Even though it is a callback, it is called from our functions
               on which are already wrapped with @exceptions_logged,
               so it should not be wrapped with one.

        @type x509: crypto.X509
        """
        subj = x509.get_subject()
        # We are probably connecting to the Node or to the Host.
        # We are interested only in the Node.
        assert subj.OU in (ssl.CERT_OU_NODE, ssl.CERT_OU_HOST), \
               repr(subj)

        # If we've received the reply from the Node,
        # and our server is not started yet, start it.
        if subj.OU == ssl.CERT_OU_NODE and self.server is None:
            if self.is_started:
                logger.debug('Using cert/%r as the primary '
                                 'node certificate for %r',
                             str(x509.get_subject()), self.host)
                self.__primary_node_certificate = x509
                self.start_server(x509)
            else:
                logger.debug('Ignoring cert/%r as the primary '
                                 'node certificate for %r, '
                                 'as it is stopped already.',
                             str(x509.get_subject()), self.host)


    @contract_epydoc
    def __send_message_to_peer(self, message):
        """Initiate an HTTP session and send out a message.

        @type message: AbstractMessage
        @precondition: isinstance(message.dst.urls, list)
        @returns: NoneType
        """
        message.in_transfer = True

        if self.is_started and self.do_send_messages:
            peer = message.dst

            # Initialize headers; first defaults, then client-specific,
            # then message-specific.
            headers = InsensitiveDict()
            self.initialize_headers(headers)
            _pragma = ','.join(filter(None,
                                      [message.get_pragma(),
                                       self.extra_pragma()]))
            HostResource.update_headers(headers, {'Pragma': [_pragma]})
            HostResource.update_headers(headers, message.get_headers())

            logger.debug('send_message_to_host: '
                             'the following urls are available for %r: %r',
                         peer, peer.urls)

            # Need to flatten the headers dictionary values
            assert consists_of(headers.itervalues(), list), \
                   repr(headers.values())
            #assert all(len(lst) == 1 for lst in headers.itervalues()), \
            # repr(headers.values)
            #flat_headers = {k: v[0] for k, v in headers.iteritems()}

            _pkey = HostQueries.HostSettings.get(HostQueries.HostSettings
                                                            .SSL_PRIVATE_KEY)
            _cert = HostQueries.HostSettings.get(HostQueries.HostSettings
                                                            .SSL_CERTIFICATE)
            if __debug__:
                if _cert is _pkey is None:
                    logger.debug('SSL cert/pkey are absent.')
                else:
                    logger.debug('SSL cert=%r, pkey=%r', _cert, _pkey)

            node_ctx_factory = ssl.ClientToNodeContextFactory(
                                   private_key=_pkey,
                                   certificate=_cert,
                                   node_uuid=self.primary_node.uuid)

            # If we are connecting not over the primary node,
            # we might need to use the "via Node" NAT traversal URLs
            # as a fallback if the target cannot be reached directly.
            if peer == self.primary_node:
                # Connecting to a Node
                _peers = [self.primary_node]
                host_ctx_factory = None

            else:
                # Connecting to a Host, but maybe via the node
                if FORCE_PASSTHROUGH:
                    _peers = [self.primary_node]
                else:
                    _peers = [peer, self.primary_node]

                _node_cert = self.__primary_node_certificate
                host_ctx_factory = \
                    ssl.ClientToHostContextFactory(private_key=_pkey,
                                                   certificate=_cert,
                                                   node_uuid=self.primary_node
                                                                 .uuid,
                                                   host_uuid=peer.uuid,
                                                   extra_ssl_cert=_node_cert)

            # What URIs shall we connect to (and assuming what realms)?
            _uri_auths = \
                deque((u,
                       p.dst_auth_realm,
                       node_ctx_factory if p.dst_auth_realm ==
                                           common_settings.HTTP_AUTH_REALM_NODE
                                        else host_ctx_factory)
                          for p in _peers
                          for u in p.urls)

            assert all((realm in (common_settings.HTTP_AUTH_REALM_NODE,
                                  common_settings.HTTP_AUTH_REALM_HOST) and
                        ssl_context is not None)
                           for u, realm, ssl_context in _uri_auths), \
                   repr(_uri_auths)

            logger.debug('Issuing %r to %r using %r',
                         message, peer, _uri_auths)

            _session = BoundOutgoingSession(tracker=self.session_tracker,
                                            peer=peer)

            assert not in_main_thread()  # get_body() may take long
            _body = message.get_body()
            assert isinstance(_body, col.Iterable), repr(_body)

            # Is it possible to calculate the length of the body?
            try:
                l = len(_body)
            except TypeError:
                # No, not possible. So we must pack the body right now
                _body = ''.join(_body)

            if self.is_started and self.do_send_messages:  # still
                @exceptions_logged(logger)
                def _wrapped_get_page_w_digest():
                    d = get_page_wDigest(
                                uri_auth_tuples=_uri_auths,
                                headers=headers,
                                method='POST',
                                postdata=_body,
                                agent=HostApp.VERSION_STRING,
                                # For HTTPClientFactory_wDigest
                                digestMgr=self.auth_manager,
                                goodSSLCertHandler=
                                    self.__good_ssl_cert_received,
                                session=_session)
                    d.addCallbacks(partial(self._received_http_response,
                                           orig_message=message),
                                   partial(self._error_on_send_http_request,
                                           message=message))
                callFromThread(_wrapped_get_page_w_digest)

        else:  # if not(self.is_started and self.do_send_messages):
            logger.debug('Abandoning the %r as the host is already shut down.',
                         message)


    def extra_pragma(self):
        """Overrides the parent method, see the definition in C{HTTPTransport}.

        When the Host sends the request to the node,
        it adds the extra "build=<myversion>" data to the Pragma header
        """
        orig = super(HostApp, self).extra_pragma()
        assert not orig, repr(orig)
        return 'build={}'.format(HostApp.HOST_BUILD)


    @exceptions_logged(logger, ignore=MessageProcessingException)
    def create_message_from_headers(self, headers, auth_peer=None):
        """Overrides the parent method.

        @note: host acts as an HTTP client.

        @todo: See the todo comment in the common/http_transport.py,
               HTTPTransport._create_message_from_headers()
        """
        # If we don't know the receiver but received something from the node,
        # this is probably a pass-through message from another host.
        # Let's reply using pass-through too.
        _peer_proto = partial(Node,
                              urls=self.primary_node.urls)
        return self._create_message_from_headers(headers=headers,
                                                 auth_peer=auth_peer,
                                                 me=self.host,
                                                 known_inhs=self.known_peers,
                                                 must_know_peer=False,
                                                 peer_proto=_peer_proto)


    @exceptions_logged(logger)
    def _received_http_response(self, response_tuple, orig_message):
        """Handle HTTP response, in the main thread yet.

        At this stage, the HTTP response body is B{fully downloaded}!

        @returns: defer.Deferred
        """
        assert in_main_thread()

        return threads.deferToThread(self.__received_http_response_in_thread,
                                     response_tuple, orig_message)


    # The ignored exceptions are handled inside
    # self._error_on_send_http_request
    @exceptions_logged(logger, ignore=(MessageProcessingException,
                                       TransactionProcessingException))
    @contract_epydoc
    def __received_http_response_in_thread(self, response_tuple, orig_message):
        """Handle HTTP response, in a secondary thread.

        At this stage, the HTTP response body is B{fully downloaded}!

        @param response_tuple: the response received from the interlocutor, as
            the tuple of the message body and the factory.
        @type response_tuple: tuple
        @precondition: (len(response_tuple) == 2 and
                        isinstance(response_tuple[0], str) and
                        isinstance(response_tuple[1],
                                   HTTPClientFactory_wDigest)) # response_tuple

        @type orig_message: AbstractMessage

        @raises MessageProcessingException:
            (from inside create_message_from_headers()) if processing
            of the message somehow failed.

        @todo: This is very similar to C{HTTPServerTransport._render},
            need to refactor.
        @todo: Proper error handling.

        @note: Deferred callback, exceptions logged.
        """
        assert not in_main_thread()

        response_body, response_factory = response_tuple

        _resp_headers_dict = \
            InsensitiveDict(dict(response_factory.response_headers))

        logger.debug('Inside __received_http_response to %r, headers: %r',
                     orig_message, _resp_headers_dict)

        # We must proceed with processing the message only if the host
        # is started.
        if (self.app is not None and
            self.app.is_started and
            self.app.do_send_messages):

            message = self.create_message_from_headers(_resp_headers_dict)

            message.body_receiver = ResponseBodyReceiver(message)

            # But why should it? The receiver already has all the data.
            message.body_receiver.on_finish.addErrback(
                lambda failure:
                    logger.error('The error in client body receiver: %r',
                                 failure))

            _len = len(response_body)
            logger.debug('Reading body: %i bytes, %r',
                         _len,
                         response_body if _len < 64
                                       else '{} ... {}'
                                                .format(response_body[:32],
                                                        response_body[-32:]))
            # Just cause the body_receiver.callback be called, since we've got
            # all the data already.
            message.body_receiver.deliver_response_body(response_body)

            _tr_manager = self.tr_manager

            # Process the incoming message
            try:
                _tr_manager.handle_incoming_message(message)
                ok = True

            except TransactionProcessingException as e:
                # The incoming transaction cannot be processed.

                # TODO: handle error properly, send a error reply maybe
                logger.warning('Error during incoming message processing: %s',
                               e)
                ok = False

            if ok:
                # Generate the response, if needed.

                # Do we have any message for reply?
                logger.debug('Try to send a reply to the response')
                reply_msg = _tr_manager.deliver_message_for_peer(
                                message.src,
                                prefer_msg_uuid=message.uuid)

                if reply_msg is not None:
                    self.__send_message_to_peer(reply_msg)

        else:  # if not (self.app.is_started and self.app.do_send_messages)
            logger.debug('Shutting down already, do nothing')


    @exceptions_logged(logger, ignore=Host_UnsupportedBuildException)
    def validate_build(self, build_string):
        """
        Overrides the parent method, see the definition in HTTPTransport.

        @raises Host_UnsupportedBuildException: If the build is unsupported.
        """
        node_build, req_build, recom_build = build_string.split('/')
        (known_node_build,
         known_req_build,
         known_recom_build) = self.known_builds

        _extra_dict = {'our_build': HostApp.HOST_BUILD,
                       'node_build': node_build,
                       'req_build': req_build,
                       'recom_build': recom_build}

        # The new value is checked against the known one,
        # so that we don't re-invoke the status logger
        # whenever the similar message is received;
        # but re-invoke if the node information is changed.
        if HostApp.HOST_BUILD < req_build:
            # Red alert - we do not satisfy the required version
            if known_req_build < req_build:
                # Report to the GUI only once, but fail the processing anyway.
                _extra_dict.update({'alert_type': 'red'})
                logger_status_version.info('Insufficient required build: '
                                               '%s < %s',
                                           HostApp.HOST_BUILD, req_build,
                                           extra=_extra_dict)
                known_req_build = req_build

            valid = False

        elif (HostApp.HOST_BUILD < recom_build and
              known_recom_build < recom_build):
            # Yellow alert - we do not satisfy the recommended version
            _extra_dict.update({'alert_type': 'yellow'})
            logger_status_version.info('Insufficient recommended build: '
                                           '%s < %s',
                                       HostApp.HOST_BUILD, recom_build,
                                       extra=_extra_dict)
            known_recom_build = recom_build
            valid = True

        elif known_node_build < node_build:
            # Version notification - "green alert" - we satisfy everything
            _extra_dict.update({'alert_type': 'green'})
            logger_status_version.info('Host build %s, node build %s',
                                       HostApp.HOST_BUILD, node_build,
                                       extra=_extra_dict)
            known_node_build = node_build
            valid = True

        else:
            # No alert, True as well
            valid = True

        self.known_builds = [known_node_build,
                             known_req_build,
                             known_recom_build]
        if not valid:
            raise Host_UnsupportedBuildException(
                      host_build=HostApp.HOST_BUILD,
                      node_build=known_node_build,
                      required_build=known_req_build,
                      recommended_build=known_recom_build)


    def get_build_compatibility_alert_type(self):

        (known_node_build,
         known_req_build,
         known_recom_build) = self.known_builds
        alert_type = None

        if HostApp.HOST_BUILD < known_req_build:
            # Red alert - we do not satisfy the required version
            alert_type = 'red'
        elif HostApp.HOST_BUILD < known_recom_build:
            # Yellow alert - we do not satisfy the recommended version
            alert_type = 'yellow'
        else:
            # Version notification - "green alert" - we satisfy everything
            alert_type = 'green'
        return alert_type


    @exceptions_logged(logger)
    @contract_epydoc
    def _error_on_send_http_request(self, failure, message):
        """Handle the failure completely (thus don't return it).

        @type failure: Failure
        @type message: AbstractMessage
        @rtype: NoneType

        @todo: When the message sending has failed, we need to KEEP the message
               (to attempt to re-send it later), instead of deleting it.

        @note: Deferred callback, exceptions logged.
        """
        assert in_main_thread()
        logger.debug('_error_on_send_http_request to %r: %r',
                     message, failure)

        # Determine the type of log needed to be generated, and generate it
        if failure.check(internet_error.ConnectionRefusedError,
                         internet_error.ConnectionDone,
                         internet_error.ConnectionLost):
            # Reporting should have been done with the session manager,
            # do nothing here.
            log_method = None
            _error_type = None
        elif failure.check(Host_UnsupportedBuildException):
            # Reporting was done during the validation; do nothing here,
            # just silently ignore.
            log_method = None
            _error_type = None
        elif failure.check(MessageProcessingException):
            _error_type = 'message_processing_error'
            log_method = logger_status.info
        elif failure.check(TransactionProcessingException):
            _error_type = 'transaction_processing_error'
            log_method = logger_status.info
        elif failure.check(internet_error.DNSLookupError):
            _error_type = 'dns_lookup_error'
            log_method = logger_status.warning
        elif failure.check(cred_error.Unauthorized, cred_error.LoginFailed):
            log_method = logger_status.warning
            _error_type = 'unauthorized_error'
        elif failure.check(web_error.Error):
            log_method = logger_status.warning
            _error_type = 'twisted_web_error'
        else:
            _error_type = 'unknown_message_sending_error'
            log_method = logger_status.error

        if log_method is not None:
            # We don't know what the error contains; it may contain
            # the non-pickleable objects, which will cause a problem
            # when sending to the GUI.
            # Thus, we must interpolate the message eagerly here.
            _tb = failure.getBriefTraceback()
            interpolated_message = \
                'Problem {problem!r} on sending message {msg!r}{op_tb!s}' \
                    .format(problem=failure.value,
                            msg=message,
                            op_tb=':\n\t{!s}'.format(_tb)
                                      if log_method is logger_status.error
                                      else '')
            log_method(interpolated_message,
                       extra={'_type': _error_type})

        if failure.check(TransactionProcessingException):
            # The problem is in the incoming transaction, not outgoing;
            # do nothing.
            pass
        else:
            # Deal with the transaction remainders.
            if self.tr_manager is None:
                logger.debug('Dealing with %r in %r while tr_manager '
                                 'is stopped already',
                             failure, message)
            else:
                transaction_to_wipe = self.tr_manager \
                                          .get_tr_by_uuid(message.uuid)
                if transaction_to_wipe is not None:
                    logger.debug('Wiping transaction %s', transaction_to_wipe)
                    # In fact, we may need to pause the transaction
                    # for a while, for not to flood the peer.


                    @exceptions_logged(logger)
                    def __on_error():
                        assert not in_main_thread()

                        logger.debug('Really wiping transaction %s',
                                     transaction_to_wipe)
                        transaction_to_wipe.fail(failure)
                        #_tr_manager.destroy_transaction_unquestionnably(
                        #    transaction_to_wipe)


                    callLaterInThread(WAIT_ON_HEARTBEAT_ERROR.total_seconds(),
                                      __on_error)


    @classmethod
    def get_nonempty_log_filenames(cls):
        """
        Return the filenames for the logs (on the current app)
        which are non-empty.

        @rtype: col.Iterable
        """
        return cls.__get_nonempty_filepaths(common_logger.LOG_FILE_PATHS)

    @classmethod
    def get_nonempty_error_log_filenames(cls):
        """
        Return the filenames for the error logs (on the current app)
        which are non-empty.

        @rtype: col.Iterable
        """
        return cls.__get_nonempty_filepaths(common_logger.ERROR_LOG_FILE_PATHS)

    @staticmethod
    def __get_nonempty_filepaths(paths):
        """Return the sequence of filepaths which are non-empty"""
        log_path_candidates = list(paths)
        for filepath in paths:
            for backup_path in glob.glob(filepath + '.[0-9]*'):
                # check, if there are only point and number after filename
                # ex: filename.log.123, not filename.log.123.bz.234
                try:
                    int(backup_path[len(filepath) + 1:])
                except ValueError:
                    pass
                else:
                    log_path_candidates.append(backup_path)

        # Now log_path_candidates contains the original log names,
        # plus any logrotate-caused variations.

        # Don't do the stat access twice (launching os.path.exists()
        # and os.stat() separately).
        for path in log_path_candidates:
            try:
                st = os.stat(path)
            except os.error:
                # Like in os.path.exists()
                pass  # don't add it to the sequence
            else:
                if st.st_size > 0:
                    yield path


    @classmethod
    def notify_about_logs(cls):
        """
        Check the existence of error/regular logs and generate a log
        about them.
        """
        error_log_paths = list(cls.get_nonempty_error_log_filenames())
        if error_log_paths:
            logger_status.warning('Error logs found: %r',
                                  error_log_paths,
                                  extra={'_type': 'error_logs_found',
                                         'log_filenames': error_log_paths})
        else:
            log_paths = list(cls.get_nonempty_log_filenames())
            if log_paths:
                logger_status.warning('Logs found: %r',
                                      log_paths,
                                      extra={'_type': 'logs_found',
                                             'log_filenames': log_paths})

    def any_start(self):
        """
        The preparations for the launch, which occur on any start,
        no matter first, second or further.
        """
        HostApp.notify_about_logs()
        callLater(AFTER_REACTOR_START.total_seconds(),
                  self.__on_reactor_start)

        self.start()


    @exceptions_logged(logger)
    def __on_reactor_start(self):
        """The actions that are executed when the reactor is started."""
        assert in_main_thread()

        logger.debug('Do some actions on reactor start')
        if self.__on_reactor_start_cb is not None:
            self.__on_reactor_start_cb(self)

        if (self.is_started and
            self.do_send_messages and
            self.do_heartbeats_revive):
            # If this is the "really running main loop" execution, let's
            # start some additional actions

            self._when_starting_normal_mode()


    @contract_epydoc
    def __bind_zmq_sockets(self, control_socket_urls):
        """
        @note: It is assumed that the C{zmq} module has been imported already.
        @precondition: ZMQ_AVAILABLE and control_socket_urls
        """
        for url in control_socket_urls:
            try:
                _bindZeroMQ = reactor.bindZeroMQ  # pylint:disable=E1101,C0103
                dummy = _bindZeroMQ(url,
                                    zmq.REP,
                                    lambda: MasterHostZMQControlProtocol(self))
                logger.debug('Listening for ZMQ on %s', url)

            except zmq.ZMQError:
                logger.exception('Cannot bind for ZMQ to %s', url)


    def add_control_sockets(self, urls):
        if ZMQ_AVAILABLE:
            self.__bind_zmq_sockets(urls)
        else:
            if not TMP_ZMQ_DISABLED:
                logger.critical('No python-zmq available, '
                                    'cannot bind control sockets: %r',
                                urls)


    def first_start(self):
        """
        Make all the preparations for launch, so that the only thing remaining
        is to launch the reactor.
        """
        assert in_main_thread()
        assert not HostApp._STARTED_ONCE, \
               'Running first_start() for the second time!'

        # Determine the build version
        try:
            HostApp.HOST_BUILD = _build = get_build_timestamp()
            _build_time = timestamp_to_local_time(_build)
        except Exception:
            _build, _build_time = 'N/A', 'N/A'

        logger.debug('Launching host %r: build %s (%s local).',
                     self.host, _build, _build_time)

        HostApp._STARTED_ONCE = True

        HostResource.init_environment_for_host_server(
            app=self, known_peers=self.known_peers, force=False)
        #HostResource.init_environment_perprocess()

        self.any_start()


    def second_start(self, control_socket_urls=()):
        """
        Make all the preparations for launch, assuming
        that it has been started already at least once.
        "Second" here may mean "third", "fourth", etc.

        @param control_socket_urls: the list of ZMQ control socket URLs
                                    which should permit controlling the host.
        @type control_socket_urls: list, tuple
        """
        assert in_main_thread()
        # Actually, ZMQ sockets may occur only on the Trusted Host,
        # where they may be initialized only on the second start.
        if control_socket_urls:
            if ZMQ_AVAILABLE:
                self.__bind_zmq_sockets(control_socket_urls)
            else:
                if not TMP_ZMQ_DISABLED:
                    logger.critical('No python-zmq available, '
                                        'cannot bind control sockets: %r',
                                    control_socket_urls)

        HostResource.init_environment_for_host_server(
            app=self, known_peers=self.known_peers, force=True)
        #HostResource.init_environment_perprocess()

        self.any_start()


    @staticmethod
    @exceptions_logged(logger)
    def ssl_avatar_converter(avatar):
        result = Host(uuid=UUID(avatar))
        logger.debug('For host server, converting avatar %r to %r',
                     avatar, result)
        return result


    @contract_epydoc
    def start_server(self, primary_node_cert):
        """
        All actions regarding starting the host server.

        @type primary_node_cert: crypto.X509

        @precondition: self.server is None
        """
        logger.debug('Likely starting server...')

        ssl_cert = HostQueries.HostSettings.get(HostQueries.HostSettings
                                                           .SSL_CERTIFICATE)
        ssl_pkey = HostQueries.HostSettings.get(HostQueries.HostSettings
                                                           .SSL_PRIVATE_KEY)
        assert in_main_thread()

        if ssl_cert is not None and ssl_pkey is not None:
            self.server = \
                Server(app=self,
                       me=self.host,
                       port=host_settings.get_my_listen_port(),
                       resource_class=HostResource,
                       checkers=[HostSSLChecker(self)],
                       cred_factories=[],
                       avatar_converter=HostApp.ssl_avatar_converter,
                       ssl_cert=ssl_cert,
                       ssl_pkey=ssl_pkey,
                       node_uuid=self.primary_node.uuid,
                       extra_ssl_cert=primary_node_cert)

            # Note: not start()! We are not launching it in a separate process,
            # in fact.
            port = self.server.run()
            logger.debug('SSL: Launching the server: %r, %r',
                         ssl_cert, ssl_pkey)

            logger.debug('Updating listened port in db to %r', port)


            @exceptions_logged(logger)
            def update_listened_port_in_db(port):
                with db.RDB() as rdbw:
                    HostQueries.HostSettings.set(
                        Queries.Settings.PORT_TO_LISTEN, port, rdbw=rdbw)


            callInThread(update_listened_port_in_db, port)
        else:
            logger.debug('SSL: Not launching the server yet: %r, %r',
                         ssl_cert, ssl_pkey)


    @exceptions_logged(logger)
    def start(self):
        """
        Start the host process.
        """
        assert in_main_thread()
        assert HostApp._STARTED_ONCE, repr(HostApp._STARTED_ONCE)
        logger.debug('Host process: starting...')

        # pylint:disable=E1101,C0103
        _addTrigger = reactor.addSystemEventTrigger
        # pylint:enable=E1101,C0103
        _addTrigger('after', 'startup', self.__on_reactor_startup)
        _addTrigger('before', 'shutdown', self.__on_reactor_shutdown)

        # Start the chain of heartbeat transactions.
        self.is_started = True

        for timer_service in self.__all_timer_services:
            timer_service.startService()

        self.traffic_meter_in.start()
        self.traffic_meter_out.start()


        def _new_writeSomeData(orig_func, self_, data):
            result = orig_func(self_, data)
            # result may contain either a number, or t.i.error.ConnectionLost
            # (or maybe even some other exception).
            if isinstance(result, numbers.Number) and result != 0:
                self.traffic_meter_out.add_tick(result)
            return result


        def _new_dataReceived(orig_func, self_, data):
            self.traffic_meter_in.add_tick(len(data))
            return orig_func(self_, data)


        if (twisted.version.major, twisted.version.minor) >= (11, 1):
            hacks.bind_tcp_Client_writeSomeData(_new_writeSomeData)
        else:
            hacks.bind_TLSMixin_writeSomeData(_new_writeSomeData)
        hacks.bind_LineReceiver_dataReceived(_new_dataReceived)

        logger.debug('Host process: started.')


    def _stop(self):
        """Stop the host.

        Most likely, this should never be called manually;
        call .terminate_host() instead.
        """
        logger.debug('Host process: stopping...')
        assert in_main_thread()

        if (self.__backup_auto_starter is not None and
            self.__backup_auto_starter.active()):
            self.__backup_auto_starter.cancel()

        if (twisted.version.major, twisted.version.minor) >= (11, 1):
            hacks.unbind_tcp_Client_writeSomeData()
        else:
            hacks.unbind_TLSMixin_writeSomeData()
        hacks.unbind_LineReceiver_dataReceived()

        if self.traffic_meter_in.started:
            self.traffic_meter_in.stop()
        else:
            logger.warning('Inbound traffic meter stopped already!')
        if self.traffic_meter_out.started:
            self.traffic_meter_out.stop()
        else:
            logger.warning('Outbound traffic meter stopped already!')

        for timer_service in self.__all_timer_services:
            timer_service.stopService()
        # The services below might be even non-launched, if not in normal mode.
        for timer_service in self.__normal_mode_timer_services:
            try:
                timer_service.stopService()
            except AttributeError as e:
                logger.debug('Error at stopping timer service: %r', e)

        self.is_started = False

        if self.server is not None:
            logger.debug('Stopping the server %r',
                         self.server)
            d = self.server.stop()
        else:
            d = defer.Deferred()
            d.callback(None)

        HostResource.deinit_environment()
        logger.debug('Host process: stopped.')
        # We should've called self.stopped.callback() now,
        # but actually, "d" variable contains a Deferred
        # which is probably still pending.
        # So self.stopped will be called back only when d is completed.
        d.addBoth(self.stopped.callback)


    @exceptions_logged(logger)
    def _stop_host_and_reactor(self):
        """Stop both the host and the reactor.

        Most likely, this should never be called manually;
        call .terminate_host() instead.
        """
        logger.debug('Shutting down host %r and %r', self, reactor)
        assert in_main_thread()

        self._stop()
        if not reactor._stopped:  # pylint:disable=E1101
            logger.debug('Stopping reactor %r', reactor)
            reactor.stop()
        else:
            logger.debug('%r stopped already!', reactor)


    @staticmethod
    @exceptions_logged(logger)
    def start_reactor():
        """
        Start the reactor cycle, until the internal code decides
        that it should quit.
        """
        logger.debug('%r starting', reactor)
        reactor.run()  # pylint:disable=E1101
        logger.debug('%r just completed', reactor)
        # Notify QT that the host termination is completed.
        logger_status.info('Host terminated',
                           extra={'_type': 'terminated'})


    @exceptions_logged(logger)
    def __auto_start_backups(self):
        """Launch the backups of the already existing datasets."""
        assert not in_main_thread()

        with db.RDB() as rdbw:
            all_datasets = Queries.Datasets.get_just_datasets(self.host.uuid,
                                                              rdbw)

            incomplete_datasets_exist, incomplete_datasets = \
                inonempty(ds
                              for ds in all_datasets
                              if not ds.completed)

            if incomplete_datasets_exist:
                logger.debug('Auto-starting incomplete backups: %r',
                             incomplete_datasets)
                for backup_index, ds in enumerate(incomplete_datasets):
                    logger.debug('%i: autostarting %r', backup_index + 1, ds)


                    @exceptions_logged(logger)
                    def on_completed(res):
                        logger.debug('Auto-started backup done: %r', res)


                    self.start_backup(ds.uuid, on_completed)

                logger.debug('Auto-started %i backup(s)!', backup_index + 1)


    def cancel_files_deletion(self, files):
        """ @todo: cancel file deletion """
        return True


    @classmethod
    def _should_file_path_be_backed_up(cls, file_path):
        """
        Given a file path, return the Boolean indicating whether such file
        should be backed up.
        """
        return os.path.basename(file_path) not in ('.DS_Store',
                                                   'desktop.ini',
                                                   'Thumbs.db')


    @exceptions_logged(logger)
    def logout_host_from_node_if_needed(self):
        """Logout the host from the Node, if needed.

        @rtype: defer.Deferred
        """
        if (self.is_started and
            self.do_send_messages and
            self.do_heartbeats_revive):

            logger.debug('Logging out...')
            assert not in_main_thread()

            self.do_heartbeats_revive = False


            def logout_handler(result):
                logger.debug('Handling logout transaction end.')
                if isinstance(result, Failure):
                    # Unsuccessful
                    # Convert to string early.
                    logger_status.warning(
                        'The node logout could not be performed: {0!r}, {1}'
                            .format(result, result.getErrorMessage()),
                        extra={'_type': 'logout',
                               'value': False})
                else:
                    # Successful
                    logger_status.info('Logged out successfully',
                                       extra={'_type': 'logout',
                                              'value': True})


            lo_tr = self.tr_manager \
                        .create_new_transaction(name='LOGOUT',
                                                src=self.host,
                                                dst=self.primary_node)

            result = lo_tr.completed
            result.addBoth(logout_handler)

        else:
            logger.debug('... no need to logout (%s, %s, %s)',
                         self.is_started,
                         self.do_send_messages,
                         self.do_heartbeats_revive)

            result = defer.Deferred()
            result.callback(None)

        return result


    @exceptions_logged(logger)
    def __on_reactor_startup(self):
        """
        Perform actions whenever the reactor is successfully started
        """
        assert in_main_thread()

        logger_status.info('Reactor started',
                           extra={'_type': 'startup'})


    @exceptions_logged(logger)
    def __on_reactor_shutdown(self):
        """Execute host termination, only in case of shutdown."""
        assert in_main_thread()

        logger.debug('Terminating host on shutdown...')

        @exceptions_logged(logger)
        def logout_in_thread():
            assert not in_main_thread()
            self.logout_host_from_node_if_needed()

        alert_type = self.get_build_compatibility_alert_type()

        if alert_type == 'red':
            # Logout transaction can not be performed on this level of
            # compatibility alert
            d = defer.Deferred()
        else:
            d = threads.deferToThread(logout_in_thread)
            d.addBoth(lambda res: logger.debug(
                                      'Host shutdown-related termination '
                                          'is completed with %r',
                                      res))

            def _try_stop_threadpool():
                logger.debug('Stopping reactor\'s threadpool')
                tp = reactor.getThreadPool()
                tp.stop()
                logger.debug('Reactor\'s threadpool stopped')


            d.addBoth(lambda res: _try_stop_threadpool())
        return d


    def terminate_host(self):
        """Forcefully execute host termination.

        Before termination, it attempts to send out the LOGOUT message.
        After termination, it forcefully stops the Twisted reactor.

        In parallel, it starts a deadline count, so that if LOGOUT hasn't
        succeeded in 10 seconds, everything will be forcefully killed.
        (Well, in fact, in 10 seconds everything will be forcefully killed
        IN ANY CASE).

        @result: the C{Deferred} called when the termination is completed
                 (not matter if it succeeds or fails).
        @rtype: defer.Deferred

        @todo: Terminate host process (should we?).
               For some reasons it can not be terminated from GUI
               by multiprocessing.Process.terminate()
               may be because host have no appropriate signal handling?
        """
        logger.debug('Force terminating host...')


        @exceptions_logged(logger)
        def hard_shutdown():
            try:
                reactor.stop()
            except internet_error.ReactorNotRunning:
                sys.exit(0)  # that's what we want, isn't it?
            else:
                sys.exit(3)  # had to use strong means, that's bad.

        @exceptions_logged(logger)
        def _logout_in_thread(d):
            """
            @param d: the deferred that must be called as soon as logout
                is completed
            """
            assert not in_main_thread()
            d_ = self.logout_host_from_node_if_needed()
            # After the logout is completed, call the original "d"
            d_.addCallback(lambda ignore: d.callback(None))


        d = defer.Deferred()

        # We have 10 seconds to shutdown, but meanwhile we try
        # to logout properly
        callLater(10, hard_shutdown)
        callInThread(_logout_in_thread, d)

        # We've got Deferred "d" by now.

        # If we are running messages, and probably reviving the host,
        # let's logout properly.
        d.addBoth(lambda ignore: callFromThread(self._stop_host_and_reactor))

        return d


    @contract_epydoc
    def query_login(self, username):
        """Request the UUID information for the host given its username.


        @returns: C{Deferred} which is called with login_ack when the querying
                  is completed.
        """
        # Do we have SSL private key available?
        # If not, generate.
        ssl_pkey = HostQueries.HostSettings.get(HostQueries.HostSettings
                                                           .SSL_PRIVATE_KEY)
        if ssl_pkey is None:
            ssl_pkey = ssl.createKeyPair()
            with db.RDB() as rdbw:
                HostQueries.HostSettings.set(HostQueries.HostSettings
                                                        .SSL_PRIVATE_KEY,
                                             ssl_pkey,
                                             rdbw=rdbw)

        assert isinstance(ssl_pkey, crypto.PKey), repr(ssl_pkey)

        # Do we have SSL certificate available?
        # If not, request.
        ssl_cert = HostQueries.HostSettings \
                              .get(HostQueries.HostSettings.SSL_CERTIFICATE)
        if ssl_cert is None:
            ssl_req = ssl.createCertRequest(ssl_pkey,
                                            OU=ssl.CERT_OU_HOST)
        else:
            assert isinstance(ssl_cert, crypto.X509), repr(ssl_cert)
            ssl_req = None

        # We may have some host databases present here. Maybe, one of them
        # fits our login/password?
        host_uuid_candidates = [uuid for uuid
                                    in host_settings.get_available_db_uuids()
                                    if uuid != NULL_UUID]

        lt_completed = defer.Deferred()


        @exceptions_logged(logger)
        def error_handler(failure):
            # Need to call repr() explicitly, to make sure it is pickleable.
            logger_status.warning('The node authentication could not '
                                      'be performed: %r, %s',
                                  failure, failure.getErrorMessage(),
                                  extra={'_type':
                                             'error during authentication'})
            lt_completed.errback(failure)


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(l_state):
            """
            @type l_state: transactions.LoginTransaction_Host.State
            """
            d = defer.Deferred()
            logger.debug('Login body received, state %r: '
                             '%r, %r, %r, %r, %r',
                         l_state,
                         l_state.ack_result,
                         l_state.ack_username,
                         l_state.ack_host_uuid,
                         l_state.ack_groups,
                         l_state.ack_ssl_cert)
            login_ack = (l_state.ack_result,
                         l_state.ack_username,
                         l_state.ack_host_uuid,
                         l_state.ack_groups,
                         l_state.ack_ssl_cert,
                         ssl_pkey)

            lt_completed.callback(login_ack)

        @exceptions_logged(logger)
        def on_create_transaction_success(li_tr):
            li_tr.completed.addCallbacks(success_handler, error_handler)


        @exceptions_logged(logger)
        def on_create_transaction_failure(failure):
            logger.error('login transaction could not be created %r', failure)


        # Finally, launch transaction
        if in_main_thread():
            d = threads.deferToThread(
                    exceptions_logged(logger)(
                        self.tr_manager.create_new_transaction),
                    name='LOGIN',
                    src=self.host,
                    dst=self.primary_node,
                    # LOGIN-specific
                    username=username,
                    host_uuid_candidates=host_uuid_candidates,
                    ssl_req=ssl_req)
            d.addCallback(on_create_transaction_success)
            d.addErrback(on_create_transaction_failure)
        else:
            li_tr = self.tr_manager.create_new_transaction(
                        name='LOGIN',
                        src=self.host,
                        dst=self.primary_node,
                        # LOGIN-specific
                        username=username,
                        host_uuid_candidates=host_uuid_candidates,
                        ssl_req=ssl_req)

            # Do not use addCallbacks() here
            li_tr.completed.addCallback(success_handler)
            li_tr.completed.addErrback(error_handler)

        return lt_completed


    @contract_epydoc
    def select_paths_for_backup(
            self, ds_name, ds_uuid, ugroup_uuid, sync, paths_map):
        """
        Given a dataset name and a set of path information
        (for either directories or files),
        select them for backup and create the appropriate dataset.

        @type ds_uuid: UUID
        @type ugroup_uuid: UserGroupUUID

        @type sync: bool

        @param paths_map: the mapping from each selected path name
            to the extra information that should be used for this path.
            Example:
            {'/home/user': {'f+': ['all'],
                            'f-': ['video', 'audio'],
                            'stat': os.stat('/home/user')}}
            Or:
            ifiles = [
                [
                    RelVirtualFile(...),
                    RelVirtualFile(...),
                    RelVirtualFile(...)
                ],
                [
                    RelVirtualFile(...),
                [
                    RelVirtualFile(...)
                ]
            ]
            paths_map = {base_dir: {'ifiles': imap(iter, ifiles),
                                    'stat': os.stat(base_dir)}}

        @type paths_map: dict

        @note: this is a calculation-heavy operation, and must be executed
            in the secondary thread!

        @todo: _chunks and encrypted_chunks are heavy memory-consuming;
            may they be turned to generators?

        @rtype: NoneType, DatasetOnPhysicalFiles
        """
        assert not in_main_thread()

        cls = self.__class__
        dataset = None

        ds_uuid = DatasetUUID.safe_cast_uuid(ds_uuid)  # force type

        if self.feature_set.per_group_encryption:
            # Read group key from the user group
            with db.RDB() as rdbw:
                _ugroup = Queries.Inhabitants.get_ugroup_by_uuid(ugroup_uuid,
                                                                 rdbw)
            group_key = _ugroup.enc_key
        else:
            group_key = None

        cryptographer = Cryptographer(group_key=group_key,
                                      key_generator=None)

        if __debug__:
            # Let's ensure that paths_map is well-formed.
            for per_path_setting in paths_map.itervalues():
                # Example: per_path_setting = {'f+': ['all'],
                #                              'f-': ['video', 'audio'],
                #                              'stat': os.stat(...)}
                assert isinstance(per_path_setting, dict), \
                       repr(per_path_setting)
                # Only inclusive and exclusive filters are supported for now.
                assert set(per_path_setting.iterkeys()) \
                           <= set(('f+', 'f-', 'ifiles', 'stat')), \
                       repr(per_path_setting)
                # If there are filters on a map, let's replace them with
                # the actual wildcards.
                for f_type in ('f+', 'f-'):
                    # Example: f_data = ['video', 'audio']
                    f_data = per_path_setting.get(f_type, [])
                    assert set(f_data) <= \
                               set(FILTERS_TO_WILDCARDS.iterkeys()), \
                           (f_data, FILTERS_TO_WILDCARDS)

        # TODO: for now, the extended information from paths_map
        # is not used yet.
        paths_with_stats = [(path, os_ex.safe_stat(path))
                                for path in paths_map.iterkeys()]

        validator = lambda st: (st is None or
                                os_ex.stat_isdir(st) or
                                os_ex.stat_isfile(st))
        _bad_paths = (path
                          for path, st in paths_with_stats
                          if not validator(st))

        try:
            logger.verbose('The following paths are chosen for backup: %r',
                           paths_with_stats)

            del paths_with_stats  # help GC

            # At this moment, we start the operations.
            # So we must display the widget immediately.
            # To do that, we send a dummy progress message.
            logger_status_chunks_hash.info('Starting hash calculation.',
                                           extra={'num': 0,
                                                  'of': 0,
                                                  'dataset_uuid': ds_uuid})

            bad_paths_exist, bad_paths = inonempty(_bad_paths)

            # This is just a prevalidation, not the final one.
            if bad_paths_exist:
                logger_status.warning(
                    'The following paths are invalid for backup: %r',
                    list(bad_paths),
                    extra={'_type': 'Ok'})

            _dataset = DatasetOnPhysicalFiles.from_paths(
                           ds_name,
                           ds_uuid,
                           ugroup_uuid,
                           sync,
                           paths_map,
                           datetime.utcnow(),
                           cryptographer)
            assert ugroup_uuid == _dataset.ugroup_uuid, \
                   (ugroup_uuid, _dataset.ugroup_uuid)

            logger.info('Backup paths are analyzed, dataset %r created.',
                        ds_name)

            # But does it make sense?
            _files_in_ds_iter = \
                chain.from_iterable(sm + lg
                                        for sm, lg in _dataset.directories
                                                              .itervalues())
            _files_exist, _files_in_ds_iter_sink = inonempty(_files_in_ds_iter)

            if not _files_exist:
                logger.debug('But this dataset is empty! Ignoring')
            else:
                dataset = _dataset

                with db.RDB() as rdbw:
                    ds_uuid2 = \
                        HostQueries.HostDatasets.create_dataset_for_backup(
                            self.host.uuid, dataset, rdbw)
                assert dataset.uuid == ds_uuid == ds_uuid2, \
                       (dataset.uuid, ds_uuid, ds_uuid2)
                logger.info('Dataset %s added to RDB.', ds_uuid)

                _chunks = list(dataset.chunks())

                # Collect all the chunk information.
                # For proper hash calculation, the chunks should turn
                # into Encrypted ones.
                encrypted_chunks = \
                    [EncryptedChunkFromFiles.from_non_encrypted(cryptographer,
                                                                _non_encr_ch)
                         for _non_encr_ch in _chunks]

                # And also, precalculate the chunk hashes,
                # with progress indication
                total_chunks_bytesize = sum(chunk.size()
                                                for chunk in encrypted_chunks)
                calculated_hashes = 0
                for chunk in encrypted_chunks:
                    assert isinstance(chunk, EncryptedChunkFromFiles), \
                           repr(chunk)

                    # GC collection is forced, because otherwise,
                    # during the massive chunks rereading,
                    # the memory is clogged very soon.
                    gc.collect()

                    dummy = chunk.hash

                    calculated_hashes += chunk.size()
                    logger_status_chunks_hash.info(
                        'Hashes calculated for %i byte(s) of %i.',
                        calculated_hashes, total_chunks_bytesize,
                        extra={'num': calculated_hashes,
                               'of': total_chunks_bytesize,
                               'dataset_uuid': ds_uuid,
                               'dataset_name': ds_name})

                # Among encrypted_chunks, there may be some pairs of chunks
                # which have the same size/hash pairs.
                with db.RDB() as rdbw:
                    Queries.Chunks.store_backup_chunks_blocks(
                        self.host.uuid, ds_uuid, encrypted_chunks, rdbw=rdbw)
                if __debug__:
                    logger.debug('%i total chunks, %i different chunks.',
                                 len(encrypted_chunks),
                                 len({(c.size(), c.hash)
                                          for c in encrypted_chunks}))
                logger_status.info('%i chunk(s) for dataset %s prepared.',
                                   len(encrypted_chunks), ds_uuid,
                                   extra={'_type': 'files_selection_completed',
                                          'dataset_uuid': ds_uuid,
                                          'dataset_name': ds_name})

        except Exception as e:
            logger_status.error(
                'Unknown error during files selection: %s',
                e,
                extra={'_type': 'error during selecting files',
                       'dataset_uuid': ds_uuid,
                       'dataset_name': ds_name})
            logger.exception('Unknown error during files selection')

        return dataset


    def relaunch_backup(self, ds_uuid, after_mins):
        """Restart backing up a dataset which already failed to backup before.
        """

        @exceptions_logged(logger)
        def _relaunch_backup_cb(ds_uuid):
            assert not in_main_thread()
            tr = transactions.BackupTransaction_Host \
                             .per_dataset_transactions \
                             .get(ds_uuid)

            if tr is None or tr.paused:
                self.perform_action_on_backup('resume', ds_uuid)


        callLaterInThread(60 * after_mins,
                          _relaunch_backup_cb, ds_uuid)



    @contract_epydoc
    def perform_action_on_backup(self, action, ds_uuid):
        """Perform some action on a backup.

        @param action: The action to perform for backup.
        @type action: basestring
        @precondition: action in ('kill', 'pause', 'resume')

        @param ds_uuid: Dataset UUID for the backup
        @type ds_uuid: UUID
        """
        tr = transactions.BackupTransaction_Host \
                         .per_dataset_transactions.get(ds_uuid)

        if action == 'kill':
            if tr is not None:
                self.tr_manager.destroy_transaction_unquestionnably(tr)

            with db.RDB() as rdbw:
                HostQueries.HostDatasets.delete_dataset(ds_uuid, rdbw)

            logger_status.info('Backup of dataset %r was aborted by user.',
                               ds_uuid,
                               extra={'_type': 'backup_aborted',
                                      'value': ds_uuid})

        elif action == 'pause':
            if tr is None:
                logger.error('Pausing the non-existing backup transaction %r',
                             ds_uuid)
            else:
                if tr.paused:
                    logger.warning('Backup of dataset %s was already paused!',
                                   ds_uuid)
                else:
                    tr.paused = True

                logger_status.info('Backup of dataset %r was paused by user.',
                                   ds_uuid,
                                   extra={'_type': 'backup_paused',
                                          'value': ds_uuid})

        elif action == 'resume':
            if tr is None:
                logger.debug('Resuming a non-existing backup transaction %r; '
                                 'actually restarting',
                             ds_uuid)
                self.start_backup(ds_uuid)
            else:
                if not tr.paused:
                    # There may be a valid case of twice resume:
                    # when it is resumed automatically in a minute
                    # after a fail, and at the same time one presses a button
                    # to resume it manually.
                    logger.debug("Backup of dataset %s was already resumed!",
                                 ds_uuid)
                else:
                    tr.paused = False

            logger_status.info('Backup of dataset %r was resumed by user.',
                               ds_uuid,
                               extra={'_type': 'backup_resumed',
                                      'value': ds_uuid})

        else:
            raise NotImplementedError(repr(action))


    @contract_epydoc
    def store_currently_selected_paths(self, paths):
        """
        Given the information about the currently selected directories/files,
        store it in the settings.

        @param paths: the dictionary mapping each selected path
            to the filter sets which should be applied to this path.
            Example:
            paths = {'/home/john': {'f+': ['all'],
                                    'f-': ['video',
                                           'audio'],
                                    'stat': os.stat('/home/john')}}
        @type paths: dict
        """
        if __debug__:
            # precheck the filter set structure
            for per_path_info in paths.itervalues():
                # example: per_path_info = {'f+': ['all'],
                #                           'f-': ['video', 'audio'],
                #                           'stat': os.stat(...)}
                for filter_type, filters in per_path_info.iteritems():
                    # example: filter_type = 'f+',
                    #          filters = ['video', 'audio']

                    # Let's support only inclusion and exclusion filters
                    # for now.
                    assert filter_type in ('f+', 'f-'), repr(filter_type)
                    # Let's check that the builtin filter names are correct.
                    assert set(filters) <= \
                               set(FILTERS_TO_WILDCARDS.iterkeys()), \
                           repr(filters)

        # The dictionary should be modified to store the absolute paths.
        paths_conv_to_abs = {abspath(k): v
                                 for k, v in paths.iteritems()}

        host_settings.store_selected_paths_to_backup(paths_conv_to_abs)


    @contract_epydoc
    def __send_error_logs_via_protocol(self,
                                       logs_to_send, on_completed=None):
        """
        Given some (error) logs, send them to the Node
        using the usual Host-Node protocol.

        @param on_completed: a callable which is called (with either
            the C{Failure} or the C{None} object if everything is good)
            when the logs are sent out successfully.
        @type on_completed: col.Callable, NoneType

        @returns: a C{Deferred} which is fired when the NOTIFY_NODE transaction
            is completed.
        @rtype: defer.Deferred
        """
        nn_tr = self.tr_manager.create_new_transaction(name='NOTIFY_NODE',
                                                       src=self.host,
                                                       dst=self.primary_node,
                                                       # NOTIFY_NODE-specific
                                                       logs=logs_to_send)


        @exceptions_logged(logger)
        def error_handler(failure):
            if on_completed is not None:
                on_completed(failure)


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(nn_state):
            """
            @type nn_state: transactions.NotifyNodeTransaction_Host.State
            """
            if on_completed is not None:
                on_completed(None)


        # Do not use addCallbacks() here
        nn_tr.completed.addCallback(success_handler)
        nn_tr.completed.addErrback(error_handler)

        return nn_tr.completed


    @contract_epydoc
    def __send_log_message_via_smtp(self, msg, smtp_settings):
        """
        Given a proper BASE64-encoded message containing the logs,
        send it to the developers.
        """
        logger.debug('Sending logs via SMTP')

        server = smtplib.SMTP(smtp_settings.server)
        #server.set_debuglevel(1)

        if smtp_settings.starttls:
            logger.debug('Starting TLS...')
            server.starttls()

        _lp = smtp_settings.loginpassword
        if _lp != (None, None):
            logger.debug('Logging in as %r', _lp.login)
            server.login(coalesce(_lp.login, ''),
                         coalesce(_lp.password, ''))
        else:
            logger.debug('No authentication')

        server.sendmail(_lp.login, smtp_settings.recipient, msg)
        server.quit()
        logger.debug('Log email sent out')


    @contract_epydoc
    def __send_error_logs_via_smtp(self,
                                   logs_to_send, smtp_settings,
                                   on_completed=None):
        """
        Given some (error) logs, send them to the Node
        using the SMTP protocol.

        @param on_completed: a callable which is called (with either
            the C{Failure} or the C{None} object if everything is good)
            when the logs are sent out successfully.
        @type on_completed: col.Callable, NoneType
        """
        try:
            # First, start with creating the email message.
            msg = MIMEMultipart()

            # Create a memory-based file for the ZIP contents...
            with closing(StringIO()) as zip_fh:
                with zipfile.ZipFile(zip_fh,
                                     'w',
                                     zipfile.ZIP_DEFLATED) as zip_file:
                    for log_filename, log_contents in logs_to_send.iteritems():
                        zip_file.writestr(log_filename, log_contents)

                msg.attach(MIMEText('Sending error logs', 'plain'))
                part = MIMEApplication(zip_fh.getvalue(), 'zip')

            part.add_header('Content-Disposition',
                            'attachment; filename="logs.zip"')

            msg.attach(part)

            try:
                username = self.host.user.name
            except AttributeError:
                username = None
            try:
                host_uuid = self.host.uuid
            except AttributeError:
                host_uuid = None

            if (username is not None) and (host_uuid is not None):
                msg['Subject'] = u'client - {}: {}'.format(username, host_uuid)
            elif host_uuid is not None:
                msg['Subject'] = 'client - {}'.format(host_uuid)
            elif username is not None:
                msg['Subject'] = u'client - {}'.format(username)
            else:
                msg['Subject'] = 'client - unknown'
            msg['From'] = '{} client'.format(version.project_title)
            msg['To'] = '{} developers'.format(version.project_title)

            # Well, message is ready, now we can send it.
            self.__send_log_message_via_smtp(msg.as_string(), smtp_settings)
            on_completed(None)

        except Exception as e:
            on_completed(Failure(e))


    @contract_epydoc
    def action_with_error_logs(self, action,
                               report_settings=None, on_completed=None):
        """Perform some action with the logs, depending on the selector.

        @precondition: action in ('delete', 'send_and_delete') # action

        @type report_settings: NoneType, LogReportingSettings

        @param on_completed: a callable which is called when
                             the action over the logs has been completed.
            If C{None}, nothing is called, the data is just output
            via the logger.
            Note that this function will most likely be called
            from the secondary thread.
        @type on_completed: col.Callable, NoneType
        """
        callInThread(self.__action_with_error_logs_in_thread,
                     action, report_settings, on_completed)


    def __action_with_error_logs_in_thread(self, action, report_settings,
                                           on_completed):
        """
        Perform an action with the error log, but make sure to do it
        in a secondary thread.
        """

        assert not in_main_thread()

        # Notify about the logs if they are present
        log_paths = list(HostApp.get_nonempty_log_filenames())


        def clean_file(filename):
            """
            Actually, just truncate it to don't affect the logging.
            """
            if os.path.splitext(filename)[1] == '.log':
                with open(filename, 'wb') as fh:
                    fh.truncate()
            else:
                try:
                    os.remove(filename)
                except Exception as e:
                    logger.warning('Exception during log removal %r', e)

        def get_log_contents(log_path):
            with open(log_path, 'rb') as fh:
                return fh.read()

        def delete_files():
            for l in log_paths:
                clean_file(l)


        # The completion may occur asynchronously, so let's the async part
        # to run on_completed() in that case.
        completed = False

        if action == 'delete':
            delete_files()
            logger_status.info('Logs removed.',
                               extra={'_type': 'send_logs_result',
                                      'result': 'delete: success'})
            completed = True

        elif action == 'send_and_delete':
            logs_to_send = {basename(path): get_log_contents(path)
                                for path in log_paths}


            def sent_out_probably(res):
                if isinstance(res, Failure):
                    logger_status.warning('Could not send logs: %r %s',
                                        res, res.getErrorMessage(),
                                        extra={'_type': 'send_logs_result',
                                               'result': 'send: error'})
                else:
                    delete_files()
                    logger_status.info('Sent out logs.',
                                       extra={'_type': 'send_logs_result',
                                              'result': 'send: success'})

                completed = True

            if report_settings.method == 'internal':
                d = self.__send_error_logs_via_protocol(
                        logs_to_send=logs_to_send,
                        on_completed=sent_out_probably)
                # Note the asynchronous completion

            elif report_settings.method == 'smtp':
                self.__send_error_logs_via_smtp(
                    logs_to_send=logs_to_send,
                    smtp_settings=report_settings.smtp_settings,
                    on_completed=sent_out_probably)
                # Note the asynchronous completion

            else:
                raise NotImplementedError(report_settings.method)

        else:
            assert False, repr(action)

        if completed:
            logger.debug("We've just done the %r action", action)
            if on_completed is not None:
                on_completed(None)


    @exceptions_logged(logger)
    def __on_send_logs_timer(self):
        """A callback which is executed by the "send logs" timer."""
        self.send_error_logs_if_nonempty()


    def send_error_logs_if_nonempty(self):
        do_send_logs, _ignored = \
            inonempty(HostApp.get_nonempty_error_log_filenames())
        if do_send_logs:
            self.action_with_error_logs(
                'send_and_delete',
                untrusted_host_settings.get_log_reporting_settings())


    def get_free_chunk_space_at_path(self, path):
        """
        Given a path, return (and also yield via logging) the free space
        available at that location for the chunks.
        """
        free_space = self.chunk_storage.get_free_chunk_space_at_path(path)
        logger_status.info(u'Free space for chunks at %r is %f',
                           path, free_space,
                           extra={'_type': 'free_space_for_chunks',
                                  'value': (path, free_space)})
        return free_space


    @staticmethod
    def set_schedule(schedule_items):
        """
        Given the schedule items, store them as the setting in the database.
        """
        with db.RDB() as rdbw:
            HostQueries.HostSettings.set(Queries.Settings.BACKUP_SCHEDULE,
                                         schedule_items,
                                         rdbw=rdbw)


    @staticmethod
    def get_schedule():
        """Return the backup schedule, as stored in the DB."""
        return HostQueries.HostSettings.get(Queries.Settings.BACKUP_SCHEDULE)


    def to_normal_mode(self):
        """
        Switches system to normal mode in which regular heartbeats
        and other network messages are sent out
        and heartbeats revive the host on the node.
        """
        assert self.is_started, 'is started'

        self.do_heartbeats_revive = True
        self.do_send_messages = True

        callFromThread(self._when_starting_normal_mode)


    def _when_starting_normal_mode(self):
        """
        This actions are performed when the application is starting normal mode
        no matter if starting from scratch or switching from login mode.
        """
        assert in_main_thread()
        # Start syncing, but only after some additional pause.
        assert self.is_started
        assert self.do_heartbeats_revive
        assert self.do_send_messages
        assert self.host.uuid != NULL_UUID, \
               (self.host, self.host.uuid)

        for timer_service in self.__normal_mode_timer_services:
            timer_service.startService()

        logger.debug('Starting normal mode: setup syncers, check changes')

        _callLater = reactor.callLater  # pylint:disable=E1101,C0103

        if self.auto_start_backup:
            self.__backup_auto_starter = \
                _callLater(AUTO_START_BACKUPS_AFTER.total_seconds(),
                           lambda: callInThread(self.__auto_start_backups))
        else:
            logger.debug("Don't auto start backups.")


    @classmethod
    def __scan_directory(cls, dir_path, followlinks=True):
        """
        Returns a non-reiterable Iterable over the tuples containing
        the dirname and the Iterable over the file states in the directory.
        The inner Iterable iterates over the files in a single directory
        (and yields the tuples of the rel_dirpath, rel_filepath and the
        C{HostQueries.HostFiles.FileState} objects),
        the outer iterable iterates over the directories.
        The symbolic links are followed by default.
        """
        for (subdir_path, dirnames, filenames) \
                in os.walk(dir_path, followlinks=followlinks):
            # Walk over the subdirectories

            rel_dirpath = relpath_nodot(subdir_path, dir_path)

            # Scan the directory in depth.
            _full_paths = \
                (os.path.join(subdir_path, fpath)
                     for fpath in sorted(filenames)
                         if cls._should_file_path_be_backed_up(fpath))

            # For each file, get its relative path and the file information.
            #
            # By the time the generator is traversed, the FS situation
            # may have been already changed, so we need to check that the path
            # still exists.
            _files_with_fstat = ((relpath_nodot(full_path, dir_path),
                                  os_ex.safe_stat(full_path))
                                     for full_path in _full_paths)

            _fstate = HostQueries.HostFiles.FileState
            # "fstat is not None" is semantically equal to exists(full_path),
            # but only here.
            file_states = (_fstate(rel_dir=rel_dirpath,
                                   rel_file=os.path.basename(rel_path),
                                   time_changed=os_ex.stat_get_mtime(fstat),
                                   size=fstat.st_size)
                               for (rel_path, fstat) in _files_with_fstat
                               if fstat is not None)

            yield (rel_dirpath, file_states)


    @classmethod
    @duration_logged()
    def take_base_directory_snapshot(cls, base_dir_path, ugroup_uuid):
        """Take the snapshop of the base directory and put it into the DB.

        @param base_dir_path: the path to the base directory.
        @type base_dir_path: basestring

        @param ugroup_uuid: the UUID of the user group,
            to which the base directory is bound (if it is not bound yet).
        @type ugroup_uuid: UserGroupUUID
        """
        base_dir_path = os.path.abspath(base_dir_path)

        with db.RDB() as rdbw:
            base_dir_id = HostQueries.HostFiles \
                                     .add_or_get_base_directory(base_dir_path,
                                                                ugroup_uuid,
                                                                rdbw)
            # It definitely exists now

        cls.take_directory_snapshot(dir_path=base_dir_path,
                                    base_dir_path=base_dir_path,
                                    base_dir_id=base_dir_id)


    @classmethod
    @duration_logged()
    def take_directory_snapshot(cls, dir_path, base_dir_path, base_dir_id):
        """
        Take the snapshop of the directory (maybe it is a subdirectory
        under the base directory) and put it into the DB.

        @param dir_path: the path to the directory.
        @type dir_path: basestring

        @param base_dir_path: the (already known) path to the base directory,
            inside which the C{dir_path} is located.
            Note that C{dir_path} MUST be a path inside (or equal)
            to C{base_dir_path}!

        @param base_dir_id: the (already known) ID field (in the DB)
            of C{base_dir}. The record containing the C{base_dir}
            must be present in the DB already.
        """
        dir_path = os.path.abspath(dir_path)
        base_dir_path = os.path.abspath(base_dir_path)

        # Imagine that basedir = C:\Users, dir = C:\Users\Alex
        # Does dir starts with basedir? It must.
        if __debug__:
            _dir_path_with_slash = dir_path + os.path.sep
            _base_dir_path_with_slash = base_dir_path + os.path.sep
            assert _dir_path_with_slash.startswith(_base_dir_path_with_slash),\
                   (_dir_path_with_slash, _base_dir_path_with_slash)

        with db.RDB() as rdbw:
            file_states_rel_to_dir = cls.__scan_directory(dir_path)

            # The file_states are relative to the dir_path.
            # Rebase them to be relative to base_dir_path
            #
            # For basedir = C:\Users, dir = C:\Users\Alex, what will be rel?
            # rel = Alex
            rel = relpath_nodot(dir_path, base_dir_path)

            _fstate = HostQueries.HostFiles.FileState  # minor optimization
            _pjoin = os.path.join  # minor optimization
            _pnormjoin = lambda *p: \
                             normpath_nodot(_pjoin(*p))  # minor optimization

            # Create a function that converts the non-rebased FileState
            # to the rebased one.
            rebased_fstate = \
                lambda fstate: _fstate(rel_dir=_pnormjoin(rel, fstate.rel_dir),
                                       rel_file=fstate.rel_file,
                                       time_changed=fstate.time_changed,
                                       size=fstate.size)

            # Finally, rebase the generator (with inside generators as well)
            file_states_rel_to_base_dir = \
                ((_pnormjoin(rel, rel_dirpath),
                  imap(rebased_fstate, file_states))
                     for rel_dirpath, file_states in file_states_rel_to_dir)

            # Function update_file_states() takes the current state
            # of the directory, and decides itself what should be added
            # and what should be deleted.
            HostQueries.HostFiles.update_file_states(
                datetime.utcnow(), base_dir_id, file_states_rel_to_base_dir,
                rdbw)


    @contract_epydoc
    def add_me_to_group(self, ugroup):
        """Given a group, add the current user to some group.

        This is usually needed when the Node notified us that we were added
        to some group; so let's add it to the appropriate group in the local
        database as well.

        @type ugroup: UserGroup
        """
        with db.RDB() as rdbw:
            # 1. Let's make sure the user group is present in the DB.
            Queries.Inhabitants.upsert_ugroup(ugroup, self.host.uuid, rdbw)
            # 2. Let's add the user to this group in the DB
            Queries.Inhabitants.add_user_to_group(self.host.user.name,
                                                  ugroup.uuid,
                                                  rdbw)
            # 3. Let's reload the main user
            self.host = Queries.Inhabitants.get_host_with_user_by_uuid(
                            self.host.uuid, rdbw)


    def get_host_uuid(self):
        """Get the UUID of the current Host."""
        return self.host.uuid
