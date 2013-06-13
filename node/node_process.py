#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Main process of the Node."""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import json
import logging
import multiprocessing
import numbers
import os
import re
import signal
import sys
import urlparse
from collections import namedtuple
from datetime import datetime, timedelta, tzinfo
from functools import partial
from itertools import imap, ifilter
from operator import itemgetter, attrgetter
from random import Random
from types import NoneType
from uuid import UUID

from zope.interface import implements

from twisted.application.internet import TimerService
from twisted.cred import error as cred_error, credentials
from twisted.cred.checkers import ICredentialsChecker, ANONYMOUS
from twisted.internet import reactor, defer
from twisted.internet.defer import Deferred, DeferredList
from twisted.python.util import InsensitiveDict
from twisted.python.failure import Failure
from twisted.web import http

from contrib.dbc import contract_epydoc, consists_of

from common import settings as common_settings
from common.abstract_peerbook import AbstractPeerBook
from common.abstract_transaction_manager import (
    AbstractTransactionManager, TransactionProcessingException)
from common.abstractions import (
    AbstractApp, AbstractMessage, AbstractInhabitant)
from common.build_version import get_build_timestamp, timestamp_to_local_time
from common.chunks import Chunk
from common.crypto import gen_rand_key
from common.datatypes import TrustedHostCaps
from common.db import Queries
from common.server import MultiProcessServer, ServerProcess
from common.inhabitants import Node, Host, HostAtNode, User, UserGroup
from common.itertools_ex import inonempty
from common.http_transport import (
    HTTPServerTransport, MessageProcessingException)
from common.typed_uuids import DatasetUUID, HostUUID, UserGroupUUID
from common.utils import (
    coalesce, exceptions_logged, update_var_run_ppid, gen_uuid, identity,
    in_main_thread, get_mem_usage_dump, NULL_UUID, open_wb,
    randevent, audit_leaks)
from common.twisted_utils import (
    callFromThread, callInThread, callLaterInThread)

from trusted import db, docstore as ds
from trusted.data_queries import DataQueries
from trusted.data_wrapper import DataWrapper
from trusted.db import TrustedQueries
from trusted.docstore.fdbqueries import FDBQueries
from trusted.transaction_manager_in_fastdb_bigdb import \
    TransactionManagerInFastDBBigDB
from trusted.peerbook_in_fastdb import PeerBookInFastDB

from protocol.messages import ChunksMessage

from . import transactions, settings
from .auth_any import AuthAvatar
from .auth_digest import AuthDigestChecker, \
                         DigestWithCandidatesCredentialFactory
from .auth_ssl import NodeSSLChecker
from .restore_request_processor import RestoreRequestProcessor
from .syncer import Syncer
from .zmq import NodeZMQControlProtocol



#
# Constants
#

logger = logging.getLogger(__name__)

# How many copies of each chunk should exist in the cloud?
try:
    from node_magic import MAX_CHUNKS_REPLICAS  # pylint:disable=F0401
except ImportError:
    MAX_CHUNKS_REPLICAS = 3

# How many chunks (maximum) should be replicated
# in a single replication iteration?
try:
    from node_magic import \
        MAX_CHUNKS_IN_REPLICATION_ITERATION  # pylint:disable=F0401
except ImportError:
    MAX_CHUNKS_IN_REPLICATION_ITERATION = 64

# How many seconds should we wait before replication iteration attempts?
try:
    from node_magic import \
        REPLICATION_ITERATIONS_PERIOD  # pylint:disable=F0401
except ImportError:
    REPLICATION_ITERATIONS_PERIOD = timedelta(minutes=10)

BACKUP_SCHEDULES_REREAD_PERIOD = timedelta(seconds=60)

HEARTBEATS_BETWEEN_NEED_INFO_CHUNKS_AVG = 100
"""
How many heartbeats should pass from a host, before it is re-requested
to tell what chunks does it have?
Note that it is non-deterministic.
"""

MAX_DATASET_AGE = timedelta(days=30)
"""
Max dataset age. We should delete/merge/whatever the datasets which are older
than this.
@todo: implement completely/properly, and probably - in Trusted Worker
    rather than here!
"""

# How often to perform a slow, probably resource-consuming, audit?
try:
    from node_magic import GLOBAL_SLOW_AUDIT_PERIOD  # pylint:disable=F0401
except ImportError:
    GLOBAL_SLOW_AUDIT_PERIOD = timedelta(minutes=1)

# How often to perform a fast and simple audit?
try:
    from node_magic import GLOBAL_FAST_AUDIT_PERIOD  # pylint:disable=F0401
except ImportError:
    GLOBAL_FAST_AUDIT_PERIOD = timedelta(seconds=5)

CHUNK_HEAL_ITERATION_PERIOD = timedelta(seconds=15)
"""How often to check for requests for chunk heal?"""

MAX_AGE_TO_REGENERATE_RESTORE_PROGRESS = timedelta(minutes=1)

RNG = Random(42)



#
# Classes
#

class NodeResource(HTTPServerTransport):
    """The node acting as an HTTP server.

    Technically, this is an IResource.

    @cvar node: The appropriate Node object for the node being processed.
    @type node: Node
    """
    __slots__ = tuple()

    VERSION_STRING = settings.NODE_VERSION_STRING


    def __init__(self, avatar):
        """
        @todo: This should be streamlined using super() as soon
               as twisted.web.resource.Resource becomes a new-style class.
        """
        #cls = self.__class__

        #super(NodeResource, self).__init__(*args, **kwargs)
        HTTPServerTransport.__init__(self, avatar=avatar)


    @classmethod
    @contract_epydoc
    def init_environment_for_node_server(cls, ports_map, app):
        """
        Initialize the common environment for the whole node,
        for all processes.

        @param ports_map: The mapping from the port to the appropriate
                          Node object for a node serving that port.
        @type ports_map: dict
        """
        HTTPServerTransport.init_environment(
            app=app,
            tr_manager=TransactionManagerInFastDBBigDB(
                           fdbw_factory=ds.FDB,
                           bdbw_factory=ds.BDB,
                           tr_classes=transactions.ALL_TRANSACTION_CLASSES,
                           app=app))

        cls.ports_to_nodes = ports_map


    @classmethod
    def init_environment_perprocess(cls, listen_port):
        """
        Initialize the environment on a per-process level,
        after it is already initialized for all processes.
        """
        try:
            _build = get_build_timestamp()
            _build_time = timestamp_to_local_time(_build)
        except:
            _build, _build_time = 'N/A', 'N/A'

        cls.node = cls.ports_to_nodes[listen_port]
        settings.configure_logging(postfix=cls.node.uuid)

        logger.debug('Launching node %s at port %i: build %s (%s local time).',
                     cls.node.uuid, listen_port, _build, _build_time)


    @exceptions_logged(logger)
    def create_message_from_headers(self, headers, auth_peer=None):
        """
        Overrides the parent method.
        """
        return self._create_message_from_headers(headers=headers,
                                                 auth_peer=auth_peer,
                                                 me=self.node,
                                                 known_inhs=self.app
                                                                .known_hosts)


    @exceptions_logged(logger)
    def _accept_passthrough_message(self, message):
        """
        Overrides the related method from the HTTPServerTransport.
        """
        # The passthrough is allowed if the target peer is known to this node.
        # Previously there was a check that it is alive, but in fact it is not
        # necessary, the Host may attempt to send the chunks to another Host
        # even if the target is not alive
        return message.dst.uuid in self.app.known_hosts


    @contract_epydoc
    def _handle_error(self, request, failure):
        """
        Node-specific error processing.

        @type failure: Failure
        """
        super(NodeResource, self)._handle_error(request, failure)

        exc = failure.value
        if isinstance(exc, MessageProcessingException):
            # We have some extra information
            level = exc.level
            message = exc._message
            assert message is None or isinstance(message, AbstractMessage), \
                   repr(message)
        else:
            level = logging.WARNING
            message = None

        if message is not None:
            # We know the details about the originating message,
            # so can construct the Pragma header in the response.
            # Otherwise it will be empty!
            _pragma = ','.join(ifilter(None, [message.get_pragma(),
                                              self.extra_pragma()]))
            request.headers.update({'Pragma': _pragma})

        if isinstance(exc, (MessageProcessingException,
                            TransactionProcessingException)):
            request.setResponseCode(http.BAD_REQUEST)
            # This is more-or-less controlled exception, like incoming message
            # parsing failure.
            result_struct = {'error': {'text': str(exc),
                                       'level': level}}
            callFromThread(request.write, json.dumps(result_struct))
        else:
            # This is completely unexpected exception, like a logic error.
            # TODO: add logging here!

            # Let's re-raise the exception to get its traceback
            try:
                failure.raiseException()
            except Exception:
                logger.exception('VERY BAD ERROR')


    def extra_pragma(self):
        """Overrides the parent method, see the definition in HTTPTransport.

        When the Node replies to the Host, it adds the extra
        "build=<cur>/<req>/<rec>" data to the Pragma header,
        to define the build versions of the Node and ones suggested by it.
        """
        orig = super(NodeResource, self).extra_pragma()
        assert not orig
        return 'build={}/{}/{}'.format(*settings.get_common().build_versions)



class ServerProcessWReactor(ServerProcess):
    """
    A special version of ServerProcess which executes a dedicated reactor
    on execution.

    It also contains the memory audit which is performed regularly.

    Also, it contains the process of regular replication. But note that
    it is not a regular audit: every iteration is launched and waits until
    all the replication commands are completed, then waits for some time
    and only then runs the next replication iteration. This helps to prevent
    several replication commands to occur simultaneously.

    Also it contains the code to refresh the information about what chunks are
    available on which host. But this code is not executed regularly, instead,
    it is manually called after several number of heartbeats is received from
    that host; that is, each refresh occurs not after some fixed time,
    but after some fixed number of heartbeats.

    @todo: What if any of the transactions in the replication iteration
           hangs or stuck?
           This may block the whole replication. Should impose a time limit
           on the replication attempts, or maybe on the particular
           transactions, after which they are forcefully killed.

    @note: The .app and .me fields are available!
    """
    __slots__ = ('backup_scheduler',
                 '__hosts_previously_alive', '__restore_request_processor',
                 '__all_timer_services')

    PID_FILE_WRITE_LOCK = multiprocessing.Lock()


    def __init__(self, *args, **kwargs):
        super(ServerProcessWReactor, self).__init__(*args, **kwargs)
        self.__hosts_previously_alive = set()


    @property
    @contract_epydoc
    def tr_manager(self):
        """
        @rtype: AbstractTransactionManager
        """
        assert self.is_alive(), repr(self)
        return self.resource_class.tr_manager


    @contract_epydoc
    def __bind_zmq_sockets(self, control_socket_urls):
        """
        @note: It is assumed that the C{zmq} module has been imported already.
        @precondition: control_socket_urls
        """
        assert in_main_thread()

        for url in control_socket_urls:
            try:
                _bindZeroMQ = reactor.bindZeroMQ  # pylint:disable=E1101,C0103
                dummy = _bindZeroMQ(url,
                                    zmq.REP,
                                    lambda: NodeZMQControlProtocol(self))
                logger.debug('Listening on %s', url)

            except zmq.ZMQError as e:
                logger.error('Cannot bind to %s: %s', url, e)


    @exceptions_logged(logger)
    def run(self):
        assert self.is_alive(), repr(self)

        super(ServerProcessWReactor, self).run()
        # TODO: Add "if user is suspended" check when
        # TODO: BackupScheduler will be uncommented.
        # self.backup_scheduler = BackupScheduler(self)
        self.__restore_request_processor = RestoreRequestProcessor(self)

        self.__all_timer_services = [
            TimerService(GLOBAL_SLOW_AUDIT_PERIOD.total_seconds(),
                         exceptions_logged(logger)(callInThread),
                         self.__do_slow_audits),
            TimerService(GLOBAL_FAST_AUDIT_PERIOD.total_seconds(),
                         exceptions_logged(logger)(callInThread),
                         self.__do_fast_audits),
            TimerService(CHUNK_HEAL_ITERATION_PERIOD.total_seconds(),
                         exceptions_logged(logger)(callInThread),
                         self.__do_chunk_heal_iteration),
            self.app.syncer,
        ]

        try:
            with ServerProcessWReactor.PID_FILE_WRITE_LOCK:
                update_var_run_ppid()
        except IOError as e:
            logger.warning('Cannot update the pid file: %s', e)
        logger.info('Launching reactor %s at port %i', reactor, self.port)

        # Listen on ZMQ sockets
        control_socket_urls = []
        if control_socket_urls:
            try:
                global zmq
                import zmq
            except ImportError:
                logger.critical('No python-zmq available, cannot bind '
                                    'control sockets: %r',
                                control_socket_urls)
            else:
                self.__bind_zmq_sockets(control_socket_urls)
                logger.debug()

        # Start reactor
        callLaterInThread(1.0, self.on_reactor_start)

        reactor.run()  # pylint:disable=E1101
        logger.debug('Reactor at port %i completed!', self.port)


    @exceptions_logged(logger)
    @contract_epydoc
    def on_reactor_start(self):
        """
        All actions which must be executed
        when the reactor for this process is started.

        @precondition: self.is_alive()
        """
        assert not in_main_thread()

        # Launch various subsystems, depending on the feature set.
        feature_set = settings.get_feature_set()

        if feature_set.p2p_storage:
            # The P2P storage requires the regular replications of the chunks.
            # So launch the first replication attempt,
            # but not earlier than several heartbeats
            # may have chance to succeed.
            logger.debug('Launching replication subsystem')
            start_replication_after = \
                REPLICATION_ITERATIONS_PERIOD.total_seconds()
            logger.debug(u'Start the first replication in %s seconds on %s…',
                         start_replication_after, self.me)
            callLaterInThread(start_replication_after,
                              self.__do_replication_iteration)

        if feature_set.web_initiated_restore:
            # TODO: it is not stopped anywhere, should be stopped (if ever)
            # together with the self.backup_scheduler.
            logger.debug('Launching web-restore subsystem...')
            callFromThread(self.__restore_request_processor.start)

        # self.backup_scheduler.start() # not needed anymore, actually

        # Regular memory audit is heavy resource-consuming
        # so disabled for now.
        for timer_service in self.__all_timer_services:
            callFromThread(timer_service.startService)


    def on_reactor_stop(self):
        """
        All actions which must be executed
        when the reactor for this process is stopped.
        """
        for timer_service in self.__all_timer_services:
            callFromThread(timer_service.stopService)


    @exceptions_logged(logger)
    def __do_slow_audits(self):
        """
        Execute audits which are resource-consuming and may take quite a time.
        """
        logger.info('----------------------------------------\n'
                    '\n'
                    'Regular node audits: %s\n',
                    datetime.utcnow())
        # Maybe, calculate get_mem_usage_dump())

        # audit_leaks('Node')

        for h in self.app.known_hosts.peers():
            how_many_removed = self.app.remove_outdated_datasets_for_host(h)
            if how_many_removed:
                logger.debug('%i datasets for %r removed.',
                             how_many_removed, h)


    @exceptions_logged(logger)
    def __do_fast_audits(self):
        """
        Execute audits which are usually fast to process
        and may be executed often.
        """
        logger.info('----------------------------------------\n'
                    '\n'
                    'Fast node audits: %s',
                    datetime.utcnow())

        _known_hosts = self.app.known_hosts

        all_hosts = set(_known_hosts.peers())
        hosts_alive = set(_known_hosts.alive_peers())
        just_born = hosts_alive - self.__hosts_previously_alive
        just_died = self.__hosts_previously_alive - hosts_alive

        if self.__hosts_previously_alive == hosts_alive:
            logger.debug('Nothing interesting at node %s.', self.node_uuid)

        else:
            logger.debug('Available hosts at node %s:', self.node_uuid)

            for host in sorted(all_hosts):
                logger.debug('  %s: %s%s%s',
                             host.uuid,
                             ' alive' if host in hosts_alive else '',
                             ', just born' if host in just_born else '',
                             ', just died' if host in just_died else '')
            self.__hosts_previously_alive = hosts_alive


    ReplicationPlanType = namedtuple('namedtuple',
                                     ('chunk', 'from_peer', 'to_peer'))

    @exceptions_logged(logger)
    def __do_replication_iteration(self):
        """Execute the next iteration for the replication."""
        assert not in_main_thread()

        assert self.is_alive(), repr(self)

        global RNG
        _known_hosts = self.app.known_hosts
        _manager = self.tr_manager

        # 1. Collect statistics.
        # This is an explicitly eagerly evaluated list, so that the query
        # doesn't hold the database lock for too long.
        with db.RDB() as rdbw:
            chunks_for_iteration = \
                list(TrustedQueries.ChunksReplicaStat
                                   .get_some_chunk_data_for_replication(
                                        MAX_CHUNKS_REPLICAS,
                                        MAX_CHUNKS_IN_REPLICATION_ITERATION,
                                        rdbw))

        alive_peers = [h.uuid for h in _known_hosts.alive_peers()]

        hosts_free_space = {host_uuid: stat.free_size
                                for host_uuid, stat
                                    in TrustedQueries.SpaceStat
                                                     .get_hosts_space_stat()
                                                     .iteritems()}

        if __debug__:
            logger.debug('-----------------------\n'
                         'Replication started on %(me)s:\n'
                         'Alive peers: %(alive)r\n'
                         'Per-host space: %(per_host_space)r\n'
                         'Potential chunks: \n%(potential)s',
                         {'me': self.me,
                          'alive': alive_peers,
                          'per_host_space': hosts_free_space,
                          'potential':
                              '\n'.join('\t{!r} from {!r} to {!r}'.format(*i)
                                            for i in chunks_for_iteration)})

        replication_plan = []

        # 2. Create the replication plan: which chunk should be copied
        #    from what host to what host.
        for chunk, _src_host_uuids, _dst_host_uuids \
            in chunks_for_iteration:

            alive_src_host_uuids = \
                filter(lambda i: i in alive_peers, _src_host_uuids)
            alive_dst_host_uuids = \
                filter(lambda i: i in alive_peers, _dst_host_uuids)

            _chunk_size = chunk.size()

            # Replicate this chunk,
            # but only if there are hosts which have it,
            # and there are hosts which need it.
            if alive_src_host_uuids and alive_dst_host_uuids:
                src_host_uuid = RNG.choice(alive_src_host_uuids)
                RNG.shuffle(alive_dst_host_uuids)

                # Try destination host, but check if it has enough free space
                planned = False
                while alive_dst_host_uuids and not planned:
                    dst_host_uuid = alive_dst_host_uuids.pop()

                    if (dst_host_uuid in hosts_free_space and
                        hosts_free_space[dst_host_uuid] > _chunk_size):
                        _item = ServerProcessWReactor.ReplicationPlanType(
                                    chunk=chunk,
                                    from_peer=_known_hosts[src_host_uuid],
                                    to_peer=_known_hosts[dst_host_uuid])
                        # That's a good destination host for this chunk!
                        if __debug__:
                            logger.debug('Planning to move %r from %r to %r',
                                         chunk,
                                         alive_src_host_uuids,
                                         alive_dst_host_uuids)
                        replication_plan.append(_item)
                        hosts_free_space[dst_host_uuid] -= _chunk_size
                        planned = True

        # 3. If the replication plan is empty, pause;
        #    else group it by the sender host the by the receiver host.
        if not replication_plan:
            logger.debug('Replication plan is empty at %r, pausing for %s.',
                         self.me, REPLICATION_ITERATIONS_PERIOD)
            callLaterInThread(REPLICATION_ITERATIONS_PERIOD.total_seconds(),
                              self.__do_replication_iteration)

        else:  # Replication plan is non-empty

            if __debug__:
                logger.debug('Actual raw replication plan, %i item(s):\n%s',
                             len(replication_plan),
                             '\n'.join('\t{}/{:d} from {} to {}'
                                           .format(i.chunk.uuid,
                                                   i.chunk.size(),
                                                   i.from_peer,
                                                   i.to_peer)
                                           for i in replication_plan))

            # replication_tr_plan = {(from_peer, to_peer): [chunk,...]}
            replication_tr_plan = {}
            for chunk, from_peer, to_peer in replication_plan:
                replication_tr_plan.setdefault((from_peer, to_peer), []) \
                                   .append(chunk)

            if __debug__:
                logger.debug(
                    'Replication message plan, %i item(s):\n%s',
                    len(replication_tr_plan),
                    '\n'.join('\tfrom {} to {} send {!r}'.format(from_peer,
                                                                 to_peer,
                                                                 chunks)
                                  for (from_peer, to_peer), chunks
                                      in replication_tr_plan.iteritems()))

            # 4. The replication transaction plan now contains all
            #    the chunk lists, grouped by the from/to hosts.
            #    Now let's create the grouped transactions.
            assert replication_tr_plan, repr(replication_tr_plan)
            assert all(replication_tr_plan.itervalues()), \
                   repr(replication_tr_plan)

            # Create the subtransactions and collect all their deferreds
            # into the DeferredList
            _transactions = []


            @exceptions_logged(logger)
            @contract_epydoc
            def _on_send_chunks_success(sc_state, what_chunks):
                """
                @precondition: consists_of(what_chunks, Chunk)
                """
                logger.debug('Replicated the chunks successfully: %r',
                             what_chunks)

            @exceptions_logged(logger)
            @contract_epydoc
            def _on_send_chunks_error(failure, what_chunks):
                """
                @precondition: consists_of(what_chunks, Chunk)
                """
                logger.error('Chunk replicating issue: %r;\n%r',
                             failure, what_chunks)


            @exceptions_logged(logger)
            @contract_epydoc
            def _on_receive_chunks_success(rc_state, receiver, what_chunks):
                """
                @type rc_state: transactions.ReceiveChunksTransaction_Node \
                                            .State
                @precondition: consists_of(what_chunks, Chunk)
                """
                logger.debug('Notified about the upcoming replication to %r '
                                 'successfully for the chunks: %r',
                             receiver, what_chunks)

                assert not rc_state.chunks_to_restore, \
                       repr(rc_state.chunks_to_restore)

                _rc_expect = rc_state.chunks_to_replicate
                assert len(_rc_expect.keys()) == 1, \
                       repr(_rc_expect)

                sender, chunks = _rc_expect.items()[0]

                assert chunks and chunks == what_chunks, \
                       (chunks, what_chunks)

                sc_tr = _manager.create_new_transaction(
                            name='SEND_CHUNKS',
                            src=self.me,
                            dst=sender,
                            # SEND_CHUNKS-specific
                            chunks_map={receiver: what_chunks})
                sc_tr.completed \
                     .addCallbacks(partial(_on_send_chunks_success,
                                           what_chunks=what_chunks),
                                   partial(_on_send_chunks_error,
                                           what_chunks=what_chunks))

                return sc_tr.completed


            @exceptions_logged(logger)
            @contract_epydoc
            def _on_receive_chunks_error(failure, what_chunks):
                """
                @precondition: consists_of(what_chunks, Chunk)
                """
                logger.error('Chunk replication notification issue: %r;\n%r',
                             failure, what_chunks)


            #
            # Now, when we have the replication transaction plan ready,
            # send the appropriate messages and perform the actual replication.
            # It consists of two phases:
            # on the first phase, for every replication chunk set,
            # the RECEIVE_CHUNKS notification is sent to the receiving side,
            # to prepare it to accept the chunks.
            # On the second phase, if (and only if) the RECEIVE_CHUNKS
            # succeeds, the SEND_CHUNKS message is sent to the sending side,
            # as a command to perform the actual chunks copying.
            #
            for (from_peer, to_peer), chunks \
                    in replication_tr_plan.iteritems():

                if __debug__:
                    logger.debug('Creating replication transaction: '
                                     'from %r to %r, send %r',
                                 from_peer, to_peer, chunks)
                rc_tr = _manager.create_new_transaction(
                            name='RECEIVE_CHUNKS',
                            src=self.me,
                            dst=to_peer,
                            # RECEIVE_CHUNKS-specific
                            chunks_to_replicate={from_peer.uuid: chunks})
                rc_tr.completed \
                     .addCallbacks(partial(_on_receive_chunks_success,
                                           receiver=to_peer,
                                           what_chunks=chunks),
                                   partial(_on_receive_chunks_error,
                                           what_chunks=chunks))
                _transactions.append(rc_tr)

            def_list = DeferredList(deferredList=[tr.completed
                                                      for tr in _transactions],
                                    consumeErrors=True)

            # Make single final handler for all the RECEIVE_CHUNKS/SEND_CHUNKS
            # transactions.


            @exceptions_logged(logger)
            @contract_epydoc
            def _on_all_send_chunks_completed(res_list):
                """Called when all SEND_CHUNKS transactions are completed.

                @param res_list: either a C{Failure} object, or a list
                    of results per each C{Deferred}.
                @type res_list: Failure, list
                """
                if isinstance(res_list, Failure):
                    logger.error('Error on sending all chunks: %r', res_list)
                else:
                    if any(not success
                               for success, result in res_list):
                        logger.error('Error on replicating chunks: %r',
                                     res_list)
                    else:
                        if __debug__:
                            logger.debug('The following chunks are replicated '
                                             'successfully: %r',
                                         [chunk
                                              for (chunk, from_peer, to_peer)
                                                  in replication_plan])
                            logger.debug('The following transactions '
                                             'are done: %r',
                                         _transactions)


                logger.debug('Replication plan completed, pausing for %s.',
                             REPLICATION_ITERATIONS_PERIOD)

                # Let's wait some time, until the PROGRESS messages arrive
                # from the replicated hosts.
                callLaterInThread(
                    REPLICATION_ITERATIONS_PERIOD.total_seconds(),
                    self.__do_replication_iteration)


            logger.debug('Waiting for these transactions all to end: %r',
                         _transactions)
            def_list.addBoth(_on_all_send_chunks_completed)


    def __heal_chunks(self, chunk_uuids_to_heal):
        """Perform actual healing procedure over multiple chunks.

        @type chunk_uuids_to_heal: col.Iterable
        """
        logger.debug('Going to heal %i chunks: %r',
                     len(chunk_uuids_to_heal), chunk_uuids_to_heal)

        healers_by_chunk_uuid = {}  # mapping from chunk UUID to the host UUIDs
        with db.RDB() as rdbw:
            # TODO: some day, may change this function to find
            # all the solutions for all the chunk UUIDs in a single
            # DB request, rather than in multiple ones.
            _find_healers_func = \
                TrustedQueries.TrustedChunks.find_chunk_potential_heal_sources
            for chunk_uuid in chunk_uuids_to_heal:
                healers_by_chunk_uuid[chunk_uuid] = \
                    frozenset(_find_healers_func(chunk_uuid=chunk_uuid,
                                                 rdbw=rdbw))

        # C{healers_by_chunk_uuid} now contains the mapping from chunk UUID to
        # the UUIDs of all (!) the hosts which contain it.
        # But we need to filter out:
        # 1. all the offline hosts,
        # 2. all the Trusted Hosts (for-storage), cause our aim is exactly
        #    to send these chunks to them.
        logger.verbose('The preliminary healing solution is: %r',
                       healers_by_chunk_uuid)

        # By now, we've found the C{healers_by_chunk_uuid} - the mapping from
        # the chunk (to heal) UUID to the UUIDs of all the hosts.

        alive_peers_by_uuid = \
            {h.uuid: h for h in self.app.known_hosts.alive_peers()}
        alive_peers_uuids = alive_peers_by_uuid.viewkeys()

        with db.RDB() as rdbw:
            thosts = list(TrustedQueries.HostAtNode.get_all_trusted_hosts(
                              for_storage=True, rdbw=rdbw))
        trusted_host_uuids = frozenset(h.uuid for h in thosts)
        alive_acceptors = [th for th in thosts if th.uuid in alive_peers_uuids]

        # By now, we have the chunks UUIDs to heal, the UUIDs of the hosts
        # which contain each of these chunks, the sets of alive hosts
        # and the set of Trusted Hosts (our potential heal acceptors).
        # Now, for each of the donor hosts, let's create the orders.
        heal_orders_by_donor_uuid = {}
        for chunk_uuid_to_heal, donor_uuids \
                in healers_by_chunk_uuid.iteritems():
            good_donors_found = False
            for donor_uuid in donor_uuids:
                if (donor_uuid in alive_peers_uuids and
                    donor_uuid not in trusted_host_uuids):
                    # Ok, this is a good donor
                    good_donors_found = True
                    heal_orders_by_donor_uuid.setdefault(donor_uuid, [])\
                                             .append(chunk_uuid_to_heal)
            if not good_donors_found:
                logger.warning('Requested to heal chunk %r, '
                                   'but no donors found',
                               chunk_uuid_to_heal)

        # Now, C{heal_orders_by_donor_uuid} maps the host UUID to the chunks
        # it can provide.
        logger.debug('%i host(s) can restore %i chunk(s)',
                     len(heal_orders_by_donor_uuid),
                     len(healers_by_chunk_uuid))

        _manager = self.app.tr_manager
        for donor_uuid, per_donor in heal_orders_by_donor_uuid.iteritems():
            donor = alive_peers_by_uuid[donor_uuid]
            acceptor = RNG.choice(alive_acceptors)
            logger.debug('Host %r will heal %r to %r',
                         donor_uuid, per_donor, acceptor)
            sc_tr = _manager.create_new_transaction(
                        name='SEND_CHUNKS',
                        src=self.me,
                        dst=donor,
                        # SEND_CHUNKS-specific
                        chunks_map={acceptor: what_chunks})
            sc_tr.addCallback(
                exceptions_logged(logger)(
                    lambda res: logger.debug(
                        'Healing of %r will heal %r to %r succeeded with %r',
                        donor_uuid, per_donor, acceptor, res)))
            sc_tr.addErrback(
                exceptions_logged(logger)(
                    lambda res: logger.error(
                        'Healing of %r will heal %r to %r failed with %r',
                        donor_uuid, per_donor, acceptor, res)))
        logger.debug('Healing iteration completed')


    def __do_chunk_heal_iteration(self):
        """Try to heal some chunks, if there are any requests."""
        assert not in_main_thread()
        logger.debug('Trying to heal some more chunks...')
        done = False

        # 1. Get all chunk UUIDs to heal
        chunk_uuids_to_heal = []
        while not done:
            with ds.FDB() as fdbw:
                heal_req = \
                    FDBQueries.HealChunkRequests \
                              .atomic_start_oldest_restore_request(fdbw)
            if heal_req is None:
                done = True
            else:
                chunk_uuids_to_heal.append(heal_req.chunk_uuid)

        # 2. For each chunk, find hosts which may help to heal it
        #    and start healing
        if chunk_uuids_to_heal:
            self.__heal_chunks(chunk_uuids_to_heal)

        # 3. As a convenience, purge the old chunk heal requests
        logger.debug('Also, purging the old chunk heal requests')
        with ds.FDB() as fdbw:
            FDBQueries.HealChunkRequests.purge_healing_requests(fdbw=fdbw)
        logger.debug('Purged!')



class NodeApp(AbstractApp):
    """
    The main Node application entity in the backup system.
    Only a single instance of this object usually exists; contrary to
    the NodeResource, which creates an instance for every incoming request.
    Note that the single NodeApp may handle multiple Node instances,
    if they are running in a multiprocess configuration.

    @ivar known_hosts: The object capable of mapping the uuids of the Host
                       objects known to this node to the objects themselves,
                       and store/update the information about the known hosts.
    @type known_hosts: AbstractPeerBook

    @todo: Refactor NodeApp to MultiNodeAppLauncher and NodeApp.
    """
    __slots__ = ('known_hosts',
                 'ports_map', 'multi_process_server',
                 '__old_signal_handler', 'server_process',
                 'syncer')


    def __init__(self, node_settings_map):
        self.known_hosts = PeerBookInFastDB(rdbw_factory=db.RDB,
                                            fdbw_factory=ds.FDB)

        # Will be initialized by the MultiProcessServer instance
        self.server_process = None

        try:
            update_var_run_ppid(append=False)  # Reset the file
        except IOError as e:
            logger.error('Cannot initialize the pid file: %s', e)

        self.__old_signal_handler = signal.signal(signal.SIGUSR1,
                                                  self.signal_handler)

        # Create several ports (and listening nodes),
        # one per each section defined in the .conf file.
        self.ports_map = {port: Node(node_settings.uuid)
                              for port, node_settings
                                  in node_settings_map.iteritems()}

        portal_checkers = [NodeSSLChecker(self.known_hosts),
                           AuthDigestChecker(self.known_hosts)]

        ssl_cert = settings.get_common().ssl_cert
        ssl_pkey = settings.get_common().ssl_pkey

        err_text = None
        if ssl_cert is None:
            err_text = 'Cannot use the SSL certificate from the settings!'
        elif ssl_pkey is None:
            err_text = 'Cannot use the SSL private key from the settings!'
        if err_text is not None:
            logger.critical(err_text)
            raise Exception(err_text)

        extra_cred_factories = [
            DigestWithCandidatesCredentialFactory(
                'MD5'.upper(),
                common_settings.HTTP_AUTH_REALM_NODE)
        ]

        self.multi_process_server = \
            MultiProcessServer(app=self,
                               resource_class=NodeResource,
                               process_class=ServerProcessWReactor,
                               ports_map=self.ports_map,
                               checkers=portal_checkers,
                               cred_factories=extra_cred_factories,
                               avatar_converter=self._avatar_converter,
                               ssl_cert=ssl_cert,
                               ssl_pkey=ssl_pkey,
                               extra_ssl_cert=ssl_cert)

        self.syncer = Syncer(self.known_hosts, self)


    @contract_epydoc
    def _avatar_converter(self, avatar):
        """
        @type avatar: AuthAvatar

        @rtype: AbstractInhabitant
        """
        _username = avatar.username
        _host_uuid_candidates = avatar.host_uuid_candidates
        _do_create = avatar.do_create
        logger.debug('Converting %r from %r', _username, avatar)

        # Let's find out the matching host UUID, or create one
        # if no matches.
        candidates = frozenset(_host_uuid_candidates)
        available = frozenset(h.uuid
                                  for h in self.known_hosts
                                               .get_user_hosts(_username))
        logger.debug('For %r, %r were suggested, but %r are known',
                     _username, candidates, available)
        can_be_reused = available & candidates

        if can_be_reused:
            result_uuid = can_be_reused.__iter__().next()
            result = self.known_hosts[result_uuid]
            logger.debug('%r can be reused, choosing %r',
                         can_be_reused, result)
        else:
            if _do_create:
                result = self.known_hosts.create_new_host_for_user(_username)
                logger.debug('Cannot reuse an UUID, creating new: %r', result)
            else:
                # No host UUID known for this user; but it requested
                # it should not be created. Let's use a dummy UUID.
                dummy_uuid = gen_uuid()
                result = HostAtNode(uuid=dummy_uuid,
                                    urls=[],
                                    # Host-specific
                                    name='Dummy for {}/{}'.format(dummy_uuid,
                                                                  _username),
                                    user=self.known_hosts
                                             .get_user_by_name(_username),
                                    # HostAtNode-specific
                                    last_seen=None)
                logger.debug('No host available for %s, but using dummy %r',
                             _username, result)

        return result


    @property
    def tr_manager(self):
        """
        @rtype: AbstractTransactionManager
        """
        assert isinstance(self.server_process.tr_manager,
                          AbstractTransactionManager), \
               repr(self.server_process.tr_manager)
        return self.server_process.tr_manager


    def reload_configuration(self):
        """
        This method should be called when the configuration of the application
        is to be dynamically reloaded.
        Currently it happens on SIGUSR1 signal, and reloads
        the database cache only.
        """
        logger.debug('Reloading configuration...')
        # self.known_hosts.flush_cache()
        # self.known_hosts.reread_cache()
        logger.debug('Configuration reloaded.')


    @exceptions_logged(logger)
    def signal_handler(self, sig, frame):
        """Unix signal handler."""
        assert sig in (signal.SIGUSR1,), \
               repr(sig)

        logger.debug('Receiving signal %i', sig)

        if callable(self.__old_signal_handler):
            self.__old_signal_handler(sig, frame)

        # Let's wait a bit before reloading the page, as the new user
        # may have not been committed yet.
        _callLater = reactor.callLater  # pylint:disable=E1101,C0103
        # Let's don't put do deferToThread() here, just for safety.
        _callLater(2.0, self.__on_sigusr1)


    @exceptions_logged(logger)
    def __on_sigusr1(self):
        assert in_main_thread()
        self.reload_configuration()


    def handle_transaction_before_create(self, tr, state):
        """Implements the @abstractmethod from C{AbstractApp}."""
        logger.debug('%r will be created...', tr)

        if tr.is_incoming():
            _message = tr.message
            # Do we need to revive a host?
            logger.debug('Incoming transaction %r: %s',
                         tr,
                         'revives' if _message.revive else 'not revives')
            me = _message.dst
            from_host = _message.src
            host_uuid = from_host.uuid

            if _message.revive:
                logger.debug('Revive message for host %s', host_uuid)

                # Important: maintain strict order of the operations below:

                # 1. Obviously, we need to check if peer is alive, before
                #    the host is forcibly marked as alive.
                just_became_alive = \
                    not self.known_hosts.is_peer_alive(host_uuid)

                if _message.name == 'HEARTBEAT':
                    _urls, _time = _message.settings_getter() \
                                           .get('urls', ([], None))
                    host_urls = \
                        filter(lambda u: urlparse.urlparse(u).port != 0,
                               _urls)
                else:
                    host_urls = []

                # 2. We need to mark host as alive (and write down the )
                logger.debug('Marking as just_seen_alive: %r at %r',
                             host_uuid, host_urls)
                self.known_hosts.mark_as_just_seen_alive(host_uuid, host_urls)

                # 3. Finally, call the actions when the host just became alive
                if just_became_alive:
                    self.syncer.host_just_became_alive(me, from_host.uuid)
                    # But the actual marking of the host as alive will happen
                    # a bit later.


    def handle_transaction_after_create(self, tr, state):
        """Implements the @abstractmethod from C{AbstractApp}."""
        logger.debug('%r just created!', tr)


    def handle_transaction_destroy(self, tr, state):
        """Implements the @abstractmethod from C{AbstractApp}."""
        logger.debug('Transaction %r just destroyed!', tr)


    def first_start(self):
        """
        Perform all the actions which are needed on the first start
        of the Node.
        """
        NodeResource.init_environment_for_node_server(ports_map=self.ports_map,
                                                      app=self)


    @classmethod
    @contract_epydoc
    def add_ugroup(cls, group):
        """
        @param group: the user group.
        @type group: UserGroup
        """
        with db.RDB() as rdbw:
            Queries.Inhabitants.add_ugroups([group], rdbw)


    @classmethod
    @contract_epydoc
    def add_user(cls, group_uuid, username, digest, is_trusted=False):
        """
        @param group_uuid: the UUID of the user group.
        @type group_uuid: UserGroupUUID

        @param username: the name of the added user (and his group).
        @type username: str

        @param is_trusted: whether the host stands for the Trusted Hosts.
        @type is_trusted: bool

        @param digest: Password digest, as a printable string
        @type digest: str
        @precondition: len(digest) == 40 # digest
        """
        enc_key = gen_rand_key()

        with db.RDB() as rdbw, ds.FDB() as fdbw:
            dw = DataWrapper(rdbw=rdbw, fdbw=fdbw, bdbw=None)

            DataQueries.Inhabitants.add_user_with_group(
                username=username,
                group_uuid=UserGroupUUID.safe_cast_uuid(group_uuid),
                digest=digest,
                is_trusted=is_trusted,
                dw=dw)


    @staticmethod
    @contract_epydoc
    def add_user_to_group(username, group_uuid):
        """
        @param username: The name of the user to add to the group.
        @type username: str

        @param group_uuid: The UUID of the group where the user
                           should be added.
        @type group_uuid: UserGroupUUID
        """
        with db.RDB() as rdbw:
            Queries.Inhabitants.add_user_to_group(username, group_uuid,
                                                  rdbw)


    @classmethod
    @contract_epydoc
    def change_user(cls, name, digest):
        """
        @param name: the name of the added user (and his group).
        @type name: str

        @param digest: Password digest, as a printable string
        @type digest: str
        @precondition: len(digest) == 40 # digest
        """
        with db.RDB() as rdbw, ds.FDB() as fdbw:
            dw = DataWrapper(rdbw=rdbw, fdbw=fdbw, bdbw=None)

            DataQueries.Inhabitants.update_user_digest(name, digest, dw=dw)


    @classmethod
    @contract_epydoc
    def add_host(cls, username, hostname, host_uuid, trusted_host_tuple=None):
        """
        @param username: The username to who the host should be bound.
        @type username: str

        @param hostname: The desired hostname.
        @type hostname: basestring

        @param host_uuid: The UUID of the added host.
        @type host_uuid: UUID

        @param trusted_host_tuple: None if the host is not Trusted one,
            or a tuple of (for_storage, for_restore) otherwise.
        @type trusted_host_tuple: NoneType, tuple
        """
        if trusted_host_tuple is None:
            trusted_host_caps = None
        else:
            _storage, _restore = trusted_host_tuple
            trusted_host_caps = TrustedHostCaps(storage=_storage,
                                                restore=_restore)

        with db.RDB() as rdbw, ds.FDB() as fdbw:
            dw = DataWrapper(rdbw=rdbw, fdbw=fdbw, bdbw=None)

            DataQueries.Inhabitants.add_host(
                username=username,
                hostname=hostname,
                host_uuid=HostUUID.safe_cast_uuid(host_uuid),
                trusted_host_caps=trusted_host_caps,
                dw=dw)


    def start_reactor(self):
        """
        Launch the Twisted reactors in child processes
        as well as the reactor in the primary process.
        """
        # This way, we execute several processing servers,
        # one per each CPU core
        self.multi_process_server.start()

        # By this line, we also execute an additional reactor in
        # the Main Thread, for the case if we ever need to run some operations
        # in a single specific thread (i.e. using single-thread code).
        reactor.run()  # pylint:disable=E1101
        settings.configure_logging(postfix='common')

        # Thus, on a typical 2-core system, we have 3 reactors.


    @contract_epydoc
    def get_restore_progress_info(self, host_uuid):
        """
        Given the UUID of the host, return the information
        about the presence and the progress of every restore operation.

        @param host_uuid: the UUID of the host.
        @type host_uuid: UUID

        @returns: a (non-reiterable) Iterable over the restore progresses.
        @rtype: col.Iterable

        @todo: if refreshed progress is recalculated, it should be cached
            for some time. Otherwise it maybe pretty much resource-consuming.
        """
        restore_states = self.tr_manager.get_tr_states(class_name='RESTORE',
                                                       dst_uuid=host_uuid)

        # Choose the birthday moment, since which the states will survive
        # without recalculating.
        survivor_birthday = \
            datetime.utcnow() - MAX_AGE_TO_REGENERATE_RESTORE_PROGRESS

        logger.debug('Getting restore info for %r, newer than %r',
                     host_uuid, survivor_birthday)

        # These states may be non-actual, and need recalculating.
        latest_restore_states = {}
        for state in restore_states:
            still_fresh = state.last_progress_recalc_time >= survivor_birthday
            logger.verbose('Analyzing state %r; %s >= %s? %r',
                           state, state.last_progress_recalc_time,
                           survivor_birthday, still_fresh)

            if not still_fresh:
                logger.warning('Refreshing the progress; TODO: may be pretty '
                                   'much resource consuming, must be cached!')

            new_state = \
                state if still_fresh \
                      else transactions.RestoreTransaction_Node \
                                       .refreshed_progress(self.tr_manager,
                                                           state.tr_dst_uuid,
                                                           state.tr_uuid)

            if new_state is not None:
                latest_restore_states[state.tr_uuid] = new_state
            else:
                logger.debug('For %r, new state is missing', state.tr_uuid)

        return (st.get_progress()
                    for st in latest_restore_states.itervalues())


    @contract_epydoc
    def remove_outdated_datasets_for_host(self, host):
        """Given the host UUID, remove all the datasets which are outdated.

        @todo: implement!

        @returns: How many datasets were removed.
        @rtype: numbers.Integral
        """
        return 0
        older_than = datetime.utcnow() - MAX_DATASET_AGE
        with db.RDB() as rdbw:
            logger.debug('Removing datasets older than %s', older_than)
            outdated_ds_uuids_iter = \
                Queries.Datasets.get_outdated_ds_uuids(host.uuid, older_than,
                                                       rdbw)
            Queries.Datasets.delete_datasets(host.uuid, outdated_ds_uuids_iter,
                                             rdbw)


    def maybe_refresh_chunks_info(self, node, host, parent_tr=None):
        """
        For a given host, look at the heartbeat_count and, probably,
        launch the process of refreshing the list of the chunks on that host.
        """
        feature_set = settings.get_feature_set()

        if (feature_set.p2p_storage and
            randevent(1, HEARTBEATS_BETWEEN_NEED_INFO_CHUNKS_AVG)):
            # If the system supports P2P storage, some host may sometimes
            # lose their chunks; so we need to regularly re-audit
            # their storages.
            logger.debug('Running non-deterministic NEED_INFO '
                         'and requesting chunks')
            # Do nested NEED_INFO transaction.
            self.__refresh_chunks_info_for_host(node, host, parent_tr)


    def __refresh_chunks_info_for_host(self, node, host, parent_tr):
        """
        Create a NEED_INFO_FROM_HOST transaction to request
        the list of available chunks.

        @param parent_tr: Parent transaction for NEED_INFO_FROM_HOST,
                          may be None.

        @rtype: Deferred
        """
        _query = [{'select': 'uuid', 'from': 'chunks'},
                  {'select': 'uuid', 'from': 'datasets'}]

        nifh_tr = self.tr_manager.create_new_transaction(
                      name='NEED_INFO_FROM_HOST',
                      src=node,
                      dst=host,
                      parent=parent_tr,
                      # NEED_INFO_FROM_HOST-specific
                      query=_query)


        @exceptions_logged(logger)
        def error_handler(failure):
            logger.error('Could not query host chunks '
                             'from the host %r: %r, %s',
                         host, failure, failure.getErrorMessage(),
                         extra={'_type': 'error_on_querying_host_chunks'})


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(nifh_state):
            """
            @type nifh_state: transactions.NeedInfoFromHostTransaction_Node \
                                          .State
            """
            assert isinstance(nifh_state.ack_result, col.Mapping), \
                   repr(nifh_state.ack_result)
            host_chunks_info = nifh_state.ack_result['chunks::uuid']
            host_datasets = nifh_state.ack_result['datasets::uuid']

            actual_chunks = set(host_chunks_info['fs'])

            # Outdated datasets are removed on each audit,
            # but for safety, make sure to remove it anew
            # right after we've received the list of chunks
            # from the host.
            dummy_num = self.remove_outdated_datasets_for_host(host)

            with db.RDB() as rdbw:
                expected_chunks_for_host = \
                    set(TrustedQueries.TrustedChunks.get_chunk_uuids_for_host(
                            host.uuid, rdbw))

            # Unexpected: the chunks which present on the host
            #             actually, but not marked accordingly
            #             on the node DB.
            # Missing: the chunks which are marked in the node DB
            #          as present on the host, but actually
            #          missing from it.
            # Note: these are actually "chunk UUIDs" rather than
            #       the Chunk objects!
            unexpected_chunks = actual_chunks - expected_chunks_for_host
            missing_chunks = expected_chunks_for_host - actual_chunks

            if unexpected_chunks:
                logger.warning('Unexpected chunks on host %s: '
                                   '%i chunks, %r',
                               host.uuid,
                               len(unexpected_chunks),
                               unexpected_chunks)

                # TODO: temp table? UNION is slow and dangerous.
               # unexpected_nonexisting_chunks = \
               #     TrustedQueries.TrustedChunks.check_chunks_for_existence(
               #         unexpected_chunks)

               # if unexpected_nonexisting_chunks:
               #     logger.warning(
               #         'Nonexisting chunks on host %s: %i\n\t%s',
               #         host.uuid,
               #         len(unexpected_nonexisting_chunks),
               #         ', '.join(uc.hex for uc in unexpected_chunks))
               #     # TODO: enable when proper cloud cleanup logic
               #     # is implemented
               #     self.delete_nonexisting_chunks_from_host(
               #         node=node,
               #         host=host,
               #         chunk_uuids=unexpected_nonexisting_chunks,
               #         parent_tr=nifh_tr)

            else:
                logger.debug('No unexpected chunks on host %s',
                             host.uuid)

            if missing_chunks:
                logger.debug('Deleting missing chunks on host %s: [%s]',
                             host.uuid, missing_chunks)
                with db.RDB() as rdbw:
                    TrustedQueries.TrustedChunks \
                                  .delete_missing_chunks_for_host(
                                       host.uuid, missing_chunks, rdbw)
            else:
                logger.debug('No missing chunks on host %s', host.uuid)


        nifh_tr.completed.addCallback(success_handler)
        nifh_tr.completed.addErrback(error_handler)

        return nifh_tr.completed


    @contract_epydoc
    def delete_nonexisting_chunks_from_host(self,
                                            node, host, chunk_uuids,
                                            parent_tr=None):
        """
        For a given host, run a transaction which removes
        the nonexisting chunks (which UUIDs are passed) from it.

        @type chunk_uuids: collections.Iterable

        @rtype: Deferred
        """
        eoh_tr = self.tr_manager.create_new_transaction(
                     name='EXECUTE_ON_HOST',
                     src=node,
                     dst=host,
                     parent=parent_tr,
                     # EXECUTE_ON_HOST-specific
                     chunk_uuids_to_delete=chunk_uuids)

        @exceptions_logged(logger)
        def error_handler(failure):
            logger.error('Could not delete chunks from the host %r: %r, %s',
                         host, failure, failure.getErrorMessage())
            logger.error('Bad chunks: %r', chunk_uuids)


        @exceptions_logged(logger)
        def success_handler(eoh_state):
            logger.debug('Deleted %i nonexisting chunks from the host %r: %r',
                         len(chunk_uuids), host, chunk_uuids)


        eoh_tr.completed.addCallback(success_handler)
        eoh_tr.completed.addErrback(error_handler)

        return eoh_tr.completed
