#!/usr/bin/python
"""
RESTORE transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from collections import defaultdict, namedtuple, deque
from datetime import datetime, timedelta
from itertools import ifilter
from functools import partial
from types import NoneType
from uuid import UUID

from twisted.internet import defer

from contrib.dbc import consists_of, contract_epydoc

from common.abstract_transaction_manager import AbstractTransactionManager
from common.abstractions import (
    pauses, unpauses, AbstractInhabitant, AbstractTransaction)
from common.chunks import Chunk
from common.db import DatabaseWrapperSQLAlchemy, Queries
from common.typed_uuids import DatasetUUID, PeerUUID, TransactionUUID
from common.utils import (
    coalesce, duration_logged, exceptions_logged, in_main_thread)
from common.twisted_utils import callLaterInThread

from protocol import transactions
from protocol.messages import RestoreMessage

from trusted import db, docstore as ds
from trusted.db import TrustedQueries
from trusted.docstore.models.transaction_states.restore import \
    RestoreTransactionState_Node

from node import settings

from ._node import AbstractNodeTransaction
from .receive_chunks import ReceiveChunksTransaction_Node
from .send_chunks import SendChunksTransaction_Node



#
# Constants
#

logger = logging.getLogger(__name__)

RESTORE_RETRY_PERIOD_IF_NO_HOSTS = timedelta(seconds=30)
"""
If we have not found any "donor" host to proceed with the restore,
how much we'll delay before the retry?
"""

MAX_RETRIES_IF_NO_DONORS = 3
"""
If we have not found any "donor" host to proceed with the restore,
how many retries we'll do?
In total, there will be C{MAX_RETRIES_IF_NO_DONORS + 1} attempt to check for
donors: an original one + C{MAX_RETRIES_IF_NO_DONORS} retries.
"""



#
# Classes
#

class RestoreTransaction_Node(transactions.RestoreTransaction,
                              AbstractNodeTransaction):
    """The RESTORE processing on the Node."""

    __slots__ = ()

    State = RestoreTransactionState_Node


    @pauses
    def on_begin(self):
        """
        Perform the actual restoring process, and then send
        the RESTORE message to the host.
        """
        assert self.is_outgoing()

        host_uuid = self.message.dst.uuid

        with db.RDB() as rdbw:
            suspended = TrustedQueries.TrustedUsers.is_user_suspended_by_host(
                            host_uuid, rdbw)
        # If user is suspended, we don't need to restore any data.
        if not suspended:
            with self.open_state(for_update=True) as state:
                # Add this transaction to the list of restores-in-progress

                with db.RDB() as rdbw:
                    chunks_for_files_by_uuid = \
                        TrustedQueries.TrustedBlocks.get_chunks_for_files(
                            host_uuid,
                            state.ds_uuid,
                            state.file_paths_for_basedirs,
                            rdbw)

                    state.pending_chunk_uuids = \
                        set(chunks_for_files_by_uuid.iterkeys())

                logger.verbose('%s::Need to restore chunks to '
                                   'host %s (DS %s): (%i)\n%r',
                               self.uuid, host_uuid, state.ds_uuid,
                               len(state.pending_chunk_uuids),
                               state.pending_chunk_uuids)

                self.__recalc_pending_host_uuids_in_state(host_uuid, state)

                logger.verbose('%s::Now need to restore chunks '
                                   'to host %s: (%i) %r',
                               self.uuid, host_uuid,
                               len(state.pending_chunk_uuids),
                               state.pending_chunk_uuids)

                state.of = len(state.pending_chunk_uuids)
                state.of_bytes = \
                    sum(ch.size()
                            for ch in chunks_for_files_by_uuid.itervalues())

                logger.debug('For %r, total number of chunks is %i, '
                                 'total number of bytes is %i',
                             self, state.of, state.of_bytes)

                success = self.__request_restoring_more_chunks(state)

            if not success:
                self.__restore_ops_failed()  # outside the state context!


    def __recalc_pending_host_uuids_in_state(self, host_uuid, state):
        """Calculate/recalculate the C{pending_host_uuids} field in state.

        The calculation is performed upon the most actual situation in the DB,
        and upon the current transaction state (which may also be updated).
        The C{state.pending_host_uuids} must already be present!

        @note: C{state} must be opened for update.
        """
        cls = self.__class__

        with db.RDB() as rdbw:
            chunk_uuids_by_host_uuid = \
                cls.__get_chunk_uuids_by_host_uuid(
                    state.pending_chunk_uuids,
                    acceptor_host_uuid=host_uuid,
                    rdbw=rdbw)

        # What chunks are already present on the acceptor host?
        already_available = chunk_uuids_by_host_uuid.get(host_uuid,
                                                         set())
        logger.verbose('%s::But this chunks are already '
                           'available at %s: %r',
                       self.uuid, host_uuid, already_available)

        state.pending_chunk_uuids -= already_available

        # Explicitly create a sorted copy, and exclude the host_uuid.
        # state.pending_host_uuids = \
        #     sorted(u for u in chunk_uuids_by_host_uuid.iterkeys()
        #              if u != host_uuid)
        state.pending_host_uuids = \
            sorted(chunk_uuids_by_host_uuid.iterkeys())


    @classmethod
    @duration_logged('__refresh_progress()')
    def __refresh_progress(cls, uploaded_chunks, refreshed_state):
        """
        Force recalculation of progress for a transaction, and modify the state
        accordingly, after some chunks were considered uploaded.

        The function is expected to be called from inside the opened state
        context with the state that would be used for calculation.

        @note: this function should not take too long, because it may run
            inside the state context opened for updating.

        @param uploaded_chunks: the iterable of UUIDs of the chunks which were
            successfully uploaded.
        @type uploaded_chunks: col.Iterable

        @param refreshed_state: the state which will be updated
            with the latest information.
        @type refreshed_state: RestoreTransaction_Node.State

        @return: a newly updated state (C{refreshed_state}).
        @rtype: RestoreTransaction_Node.State
        """
        uploaded_chunks = set(uploaded_chunks)
        logger.debug('Recalculating progress, while considering these chunks '
                         'uploaded to %r within %r: (%i) %r',
                     refreshed_state.tr_dst_uuid,
                     refreshed_state.tr_uuid,
                     len(uploaded_chunks), uploaded_chunks)

        # Get the current set of pending chunks;
        # remove the chunks that are uploaded;
        # compare the length of the sets (of pending chunks) before and
        # after removal.
        refreshed_state.last_progress_recalc_time = datetime.utcnow()
        pending_count = len(refreshed_state.pending_chunk_uuids)
        refreshed_state.pending_chunk_uuids -= uploaded_chunks
        refreshed_state.num = \
            refreshed_state.of - len(refreshed_state.pending_chunk_uuids)

        logger.debug('Remaining chunks to upload within %r: (%i) %r',
                     refreshed_state.tr_uuid,
                     len(refreshed_state.pending_chunk_uuids),
                     refreshed_state.pending_chunk_uuids)

        if refreshed_state.pending_chunk_uuids:
            with db.RDB() as rdbw:
                pending_bytes = \
                    TrustedQueries.TrustedChunks.get_total_size_of_chunks(
                        refreshed_state.pending_chunk_uuids, rdbw)
        else:
            pending_bytes = 0

        _new_num = refreshed_state.of \
                   - len(refreshed_state.pending_chunk_uuids)
        _new_num_bytes = refreshed_state.of_bytes - pending_bytes
        _st = refreshed_state
        logger.debug('Recalculated progress: chunks - %i->%i of %i, '
                         'bytes - %i->%i of %i',
                     _st.num, _new_num, _st.of,
                     _st.num_bytes, _new_num_bytes, _st.of_bytes)

        refreshed_state.num = max(_new_num, 0)
        refreshed_state.num_bytes = max(_new_num_bytes, 0)

        logger.debug('Current progress %i/%i (%i/%i)',
                     refreshed_state.num, refreshed_state.of,
                     refreshed_state.num_bytes, refreshed_state.of_bytes)
        return refreshed_state


    @classmethod
    @contract_epydoc
    def refreshed_progress(cls, tr_manager, host_uuid, tr_uuid):
        """Get a refreshed (to be up-to-date) progress state for a transaction.

        @type tr_manager: AbstractTransactionManager
        @type host_uuid: PeerUUID
        @type tr_uuid: TransactionUUID

        @return: a newly updated state, or C{None} if the state could not
            already be found.
        @rtype: RestoreTransaction_Node.State, NoneType
        """
        logger.debug('Force progress recalculation for %r on %r',
                     tr_uuid, host_uuid)
        try:
            # What chunks are really present on the Host?
            with db.RDB() as rdbw:
                all_chunk_uuids_on_target_host = \
                    set(TrustedQueries.TrustedChunks.get_chunk_uuids_for_host(
                            host_uuid, rdbw))

            with tr_manager.open_tr_state(tr_uuid=tr_uuid,
                                          for_update=False) as state:
                return cls.__refresh_progress(all_chunk_uuids_on_target_host,
                                              refreshed_state=state)
        except:
            logger.exception('Could not recalculate progress for %r', tr_uuid)
            return None


    @classmethod
    @contract_epydoc
    def __get_chunk_uuids_by_host_uuid(cls,
                                       chunk_uuids, acceptor_host_uuid, rdbw):
        """Get the actual distribution of chunks to the hosts which have them.

        @todo: the complexity of the function depends on the complexity
            of C{TrustedQueries.TrustedChunks.get_host_uuids_for_chunks()}.

        @param chunk_uuids: the UUIDs of the chunks whose location (on the
            peers) we want to know.
        @type chunk_uuids: col.Iterable

        @param acceptor_host_uuid: the UUID of the host for which the restore
            is executed.
        @type acceptor_host_uuid: PeerUUID
        @type rdbw: DatabaseWrapperSQLAlchemy

        @return: the mapping from the host UUID to the chunks (ones from
            C{chunk_uuids}) which are stored on that host.
            If the "p2p_storage" feature is supported,
            all hosts which have these chunks are returned;
            otherwise, only the Trusted Hosts and the acceptor host
            are returned.
        @rtype: col.Mapping
        """
        # Get the hosts which carry these chunks, but differently
        # depending upon the availability of the P2P storage.
        _chunk_uuids_by_host_uuid = \
            TrustedQueries.TrustedChunks.get_host_uuids_for_chunks(
                chunk_uuids, rdbw)

        # Now filter the result depending on the system capabilities.

        feature_set = settings.get_feature_set()
        if feature_set.p2p_storage:
            # With "p2p_storage" feature, each host may serve some chunks
            # separately.
            result = _chunk_uuids_by_host_uuid

        else:
            # Without "p2p_storage" feature, let's consider that only
            # Trusted Hosts serve the chunks.
            _trusted_storage_host_uuids = \
                {h.uuid
                     for h in TrustedQueries.HostAtNode.get_all_trusted_hosts(
                                  for_storage=True, rdbw=rdbw)}
            _wanted_host_uuids = \
                _trusted_storage_host_uuids | {acceptor_host_uuid}
            result = {h_uuid: c_uuids
                          for h_uuid, c_uuids
                              in _chunk_uuids_by_host_uuid.iteritems()
                          if h_uuid in _wanted_host_uuids}

        return result


    @exceptions_logged(logger)
    def __on_restore_retry_delay_elapsed(self):
        assert not in_main_thread()

        with self.open_state(for_update=True) as state:
            success = self.__request_restoring_more_chunks(state)
        if not success:
            self.__restore_ops_failed()  # outside the state context!


    @duration_logged('__request_restoring_more_chunks()')
    @contract_epydoc
    def __request_restoring_more_chunks(self, state):
        """
        Try the next host among the available ones, and attempt
        to request it to send all the chunks it can.

        @todo: each call of C{__request_restoring_more_chunks()}
            should be pretty fast, less than a second, because it holds
            the state context open for writing during the whole call.
            If it ever happens to be that long, this needs to be refactored,
            so that it opens a state context for writing only for the duration
            of actual writing.

        @param state: transaction state already opened for update.
        @type state: AbstractTransaction.State

        @return: whether everything goes so far so good. In the opposite case,
            the caller must call C{self.__restore_ops_failed()}
            (and do that outside of the state context).
        @rtype: bool
        """
        _message = self.message
        _manager = self.manager
        me = _message.src
        host = _message.dst

        so_far_so_good = True

        logger.debug('%s::Pending hosts (%i), %r',
                     self.uuid,
                     len(state.pending_host_uuids),
                     state.pending_host_uuids)
        logger.debug('%s::Pending chunks for restore (%i), %r',
                     self.uuid,
                     len(state.pending_chunk_uuids),
                     state.pending_chunk_uuids)

        if not state.pending_chunk_uuids:
            logger.debug('%s::Seems done with restoring...', self.uuid)
            so_far_so_good = \
                self.__no_more_chunks_to_restore(state, success=True)
        else:
            # What host shall we use? Use only alive ones.
            host_uuids = \
                deque(ifilter(self.manager.app.known_hosts.is_peer_alive,
                              state.pending_host_uuids))

            if not host_uuids:
                # If no hosts now, they still may occur in some time.
                logger.warning('%s::Cannot restore: no more hosts '
                                   'on attempt %d!',
                               self.uuid, state.no_donors_retries)

                state.no_donors_retries += 1

                if state.no_donors_retries <= MAX_RETRIES_IF_NO_DONORS:
                    logger.debug('Pausing for %s...',
                                 RESTORE_RETRY_PERIOD_IF_NO_HOSTS)
                    callLaterInThread(
                        RESTORE_RETRY_PERIOD_IF_NO_HOSTS.total_seconds(),
                        self.__on_restore_retry_delay_elapsed)
                    so_far_so_good = True
                else:
                    logger.error('In %d attempts, '
                                     "couldn't find any donors, cancelling",
                                 MAX_RETRIES_IF_NO_DONORS)
                    so_far_so_good = \
                        self.__no_more_chunks_to_restore(state, success=False)

            else:
                # No matter how many retries we could've made, but now we found
                # some donors; reset the retries counter.
                state.no_donors_retries = 0

                # Let's use the first host in the list.
                restoring_from_host_uuid = host_uuids.popleft()
                # ... but in case of possible failure, move it
                # to the end of loop.
                host_uuids.append(restoring_from_host_uuid)

                restoring_from_host = \
                    self.manager.app.known_hosts[restoring_from_host_uuid]

                logger.debug("%s::Let's restore some chunks from %r to %r",
                             self.uuid, restoring_from_host, host)

                with db.RDB() as rdbw:
                    all_chunk_uuids_on_from_host = \
                        set(TrustedQueries.TrustedChunks
                                          .get_chunk_uuids_for_host(
                                               restoring_from_host_uuid, rdbw))

                # Among all the chunks on the SRC host,
                # we need only several ones, which are needed for restore
                available_chunk_uuids = \
                    all_chunk_uuids_on_from_host & state.pending_chunk_uuids
                del all_chunk_uuids_on_from_host  # help GC

                logger.verbose('%s::Chunks available at host %s are: %r',
                               self.uuid,
                               restoring_from_host_uuid,
                               available_chunk_uuids)

                with db.RDB() as rdbw:
                    restoring_chunks = \
                        list(Queries.Chunks.get_chunks_by_uuids(
                                 available_chunk_uuids, rdbw))

                if restoring_chunks:
                    logger.verbose('%s::Restoring chunks from %r to %r: %r',
                                   self.uuid,
                                   restoring_from_host,
                                   host,
                                   available_chunk_uuids)

                    # Start the nested RECEIVE_CHUNKS transaction.
                    rc_tr = _manager.create_new_transaction(
                                name='RECEIVE_CHUNKS',
                                src=me,
                                dst=host,
                                parent=self,
                                # RECEIVE_CHUNKS-specific
                                chunks_to_restore={
                                    restoring_from_host.uuid: restoring_chunks
                                })
                    rc_tr.completed.addCallbacks(
                        partial(self._on_receive_chunks_success,
                                what_chunks=restoring_chunks,
                                from_what_host=restoring_from_host),
                        partial(self._on_receive_chunks_error,
                                from_what_host=restoring_from_host))
                    so_far_so_good = True
                else:
                    logger.debug("%s::Host %s doesn't have any chunks for %s,"
                                     'removing it from the set',
                                 self.uuid, restoring_from_host_uuid, host)
                    state.pending_host_uuids.remove(restoring_from_host_uuid)

                    so_far_so_good = \
                        self.__request_restoring_more_chunks(state)

        return so_far_so_good


    @exceptions_logged(logger)
    @contract_epydoc
    def _on_receive_chunks_error(self, failure, from_what_host):
        """
        @type from_what_host: AbstractInhabitant
        """
        logger.error('%s::Chunk restoring from host %r, '
                         'notification issue: %r',
                     self.uuid, from_what_host, failure)

        with self.open_state(for_update=True) as state:
            success = self.__request_restoring_more_chunks(state)

        if not success:
            self.__restore_ops_failed()  # outside the state context!


    @exceptions_logged(logger)
    def _on_receive_chunks_success(self,
                                   rc_state, what_chunks, from_what_host):
        """
        This method is called after the RECEIVE_CHUNKS transaction
        has successfully completed.

        @type rc_state: ReceiveChunksTransaction_Node.State
        @type from_what_host: AbstractInhabitant
        """
        _manager = self.manager
        _message = self.message
        me = _message.src
        host = _message.dst

        logger.debug('%s::Notified %r about the upcoming replication '
                         'from %r for the following chunks: %r',
                     self.uuid, host, from_what_host, what_chunks)

        # Start the nested SEND_CHUNKS transaction.
        sc_tr = self.manager \
                    .create_new_transaction(name='SEND_CHUNKS',
                                            src=me,
                                            dst=from_what_host,
                                            parent=self,
                                            # SEND_CHUNKS-specific
                                            chunks_map={host: what_chunks})
        sc_tr.completed.addCallbacks(
            partial(self._on_send_chunks_success,
                    from_host_uuid=from_what_host.uuid,
                    what_chunks=what_chunks),
            partial(self._on_send_chunks_error,
                    what_chunks=what_chunks))


    @exceptions_logged(logger)
    @contract_epydoc
    def _on_send_chunks_success(self, sc_state, from_host_uuid, what_chunks):
        """
        This method is called after the SEND_CHUNKS transaction
        sending the chunk to the host being restored
        has successfully completed.

        @type sc_state: SendChunksTransaction_Node.State
        @type from_host_uuid: PeerUUID
        """
        cls = self.__class__
        logger.debug('Restored the chunks successfully: %r', what_chunks)

        with self.open_state(for_update=True) as state:
            # Result in sc_state is ignored.
            # TODO: but should it be ignored? if send_chunks failed,
            # this could mean the chunk haven't been actually requested
            # for upload;
            # they should be reuploaded rather than considered done.
            # Though maybe not, we'll notice it ourselves.
            cls.__refresh_progress((c.uuid for c in what_chunks),
                                   refreshed_state=state)

            state.pending_host_uuids.remove(from_host_uuid)
            success = self.__request_restoring_more_chunks(state)

        if not success:
            self.__restore_ops_failed()  # outside the state context!


    @exceptions_logged(logger)
    def _on_send_chunks_error(self, failure):
        """
        This method is called after the SEND_CHUNKS transaction
        sending the chunk to the host being restored
        has failed for some reason.
        """
        logger.error('Chunk restoring issue: %r', failure)

        with self.open_state(for_update=True) as state:
            success = self.__request_restoring_more_chunks(state)

        if not success:
            self.__restore_ops_failed()  # outside the state context!


    def __no_more_chunks_to_restore(self, state, success=True):
        """What to do when there are no more chunks to restore?

        @param state: transaction state already opened for update.
        @type state: AbstractTransaction.State

        @return: whether the restore transaction was successful.
        @rtype: bool
        """
        logger.debug('%s::No more chunks to restore! %s successful.',
                     self.uuid, 'Everything' if success else 'Not')

        state.success = success
        _ds_uuid = state.ds_uuid
        _wr_uuid = state.wr_uuid
        is_sync = state.is_sync

        if success:
            logger.debug('%s::All chunks are already on the host, '
                             "let's send out the RESTORE message",
                         self.uuid)

            _message = self.message
            host = _message.dst

            with db.RDB() as rdbw:
                ds = Queries.Datasets.get_dataset_by_uuid(_ds_uuid, host.uuid,
                                                          rdbw)
                ugroup = Queries.Inhabitants.get_ugroup_by_uuid(ds.ugroup_uuid,
                                                                rdbw)

            # Interesting fact: ds.sync doesn't mean that we are necessarily
            # syncing the dataset. Only state.is_sync knows that (since it is
            # possible, that we are restoring some separate files from a "sync"
            # dataset).

            _message.ugroup = ugroup
            _message.dataset = ds if is_sync else None
            _message.sync = is_sync

            # Re-get the files->blocks mapping information
            # to send to the host.
            with db.RDB() as rdbw:
                (files_by_rel_path,
                 blocks_by_file_rel_path) = \
                    TrustedQueries.TrustedBlocks.get_blocks_for_files(
                        host.uuid, state.ds_uuid,
                        state.file_paths_for_basedirs, rdbw)
            _message.files = \
                {f: blocks_by_file_rel_path[f.rel_path]
                     for f in files_by_rel_path.itervalues()}
            _message.wr_uuid = _wr_uuid

            logger.verbose(
                '%s::No more chunks for DS %r/WR %r at %r:'
                    '\n%r',
                self.uuid, _message.dataset, _message.wr_uuid, _message.ugroup,
                _message.files)

            # The transaction is still paused at the moment
            self.manager.post_message(_message)
            return True

        else:
            return False


    @unpauses
    def __restore_ops_failed(self):
        """
        @note: due to C{@unpauses}, should B{not} be called within a state
            context opened for update; call it via C{callLaterInThread()}!
        """
        logger.error('Some issue occured during restoring %r, '
                         'not sending the RESTORE message',
                     self)


    @defer.inlineCallbacks
    @exceptions_logged(logger)
    def on_end(self):
        assert self.is_outgoing()
        cls = self.__class__

        yield defer.succeed(None)  # to satisfy @inlineCallbacks

        with self.open_state(for_update=True) as state:
            if not state.success:
                logger.error("Couldn't restore %r", self)

            else:
                _ds_uuid = state.ds_uuid

                # Receiving the RESTORE_ACK from the host
                ack_result_code = self.message_ack.ack_result_code
                host_uuid = self.message_ack.src.uuid

                result_is_good = \
                    RestoreMessage.ResultCodes.is_good(ack_result_code)

                if result_is_good:
                    with db.RDB() as rdbw:
                        ds = Queries.Datasets.get_dataset_by_uuid(_ds_uuid,
                                                                  host_uuid,
                                                                  rdbw)

                    restore_is_sync = state.is_sync
                    logger.debug('%s:: %s restore succeeded for %s',
                                 'Whole-dataset' if restore_is_sync
                                                 else 'selective',
                                 'the whole dataset' if ds.sync
                                                     else 'some files',
                                 self.uuid)
                    if restore_is_sync:
                        # It doesn't necessarily mean that ds.sync is True!
                        logger.debug('%s::Marking whole dataset %r '
                                         'as synced to %r',
                                     self.uuid, _ds_uuid, host_uuid)
                        with db.RDB() as rdbw:
                            TrustedQueries.TrustedDatasets \
                                          .mark_dataset_as_synced_to_host(
                                               _ds_uuid, host_uuid, rdbw)
                else:
                    logger.debug('%s::Restore (DS %r) failed at %r '
                                     'with code %r',
                                 self.uuid, _ds_uuid, host_uuid,
                                 ack_result_code)

                state.ack_result_code = ack_result_code

            logger.debug('%s::on_end done!', self.uuid)
