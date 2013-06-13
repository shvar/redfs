#!/usr/bin/python
"""
BACKUP transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from datetime import datetime, timedelta
from functools import partial
from itertools import ifilter
from random import Random
from threading import Lock
from uuid import UUID
from weakref import WeakValueDictionary

from twisted.internet.defer import DeferredList
from twisted.python.failure import Failure

from contrib.dbc import consists_of, contract_epydoc

from common import limits
from common.abstractions import pauses, unpauses
from common.chunks import Chunk, ChunkFromFilesFinal, EncryptedChunkFromFiles
from common.crypto import AES_BLOCK_SIZE, Cryptographer
from common.datasets import DatasetOnChunks
from common.db import Queries
from common.inhabitants import Host
from common.typed_uuids import PeerUUID
from common.utils import pformat, exceptions_logged, gen_uuid, in_main_thread
from common.twisted_utils import callLaterInThread

from protocol import transactions
from protocol.messages import BackupMessage, ProvideBackupHostsMessage

from host import db
from host.db import HostQueries

from ._host import AbstractHostTransaction
from .chunks import ChunksTransaction_Host
from .need_info_from_node import NeedInfoFromNodeTransaction_Host
from .progress import ProgressTransaction_Host
from .provide_backup_hosts import ProvideBackupHostsTransaction_Host



#
# Constants
#

logger = logging.getLogger(__name__)
logger_status_backup = logging.getLogger('status.backup')
logger_status_backup_ch_upl = logging.getLogger('status.backup.progress')

# Whether we are allowed to send multiple chunks
# in a single CHUNKS transaction...
MERGE_CHUNKS_ON_BACKUP = True
# .. and how large (approximately) should be the CHUNKS message.
try:
    from host_magic import MAX_CHUNK_MESSAGE_SIZE  # pylint:disable=F0401
except ImportError:
    MAX_CHUNK_MESSAGE_SIZE = 16
# If error occured during backup, after how many minutes is it relaunched?
RELAUNCH_AFTER_MINUTES_ON_ERROR = 1  # minutes



#
# Classes
#

class BackupTransaction_Host(transactions.BackupTransaction,
                             AbstractHostTransaction):
    """BACKUP transaction on the Host.

    @ivar target_hosts: the dictionary mapping the Host object
        to the C{col.Counter} of how many chunks (of some size code)
        should be uploaded to that Host.
    @type target_hosts: col.Mapping

    @ivar __chunks_by_size_code: the mapping from the chunk size code
        to all the chunks of this size.
    @type __chunks_by_size_code: col.Mapping

    @ivar __all_chunks: the set of the chunks which are expected to be present
        in this dataset.
    @type __all_chunks: set
    @invariant: consists_of(__all_chunks, ChunkFromFilesFinal)

    @ivar __uploading_chunks: the list of the chunks in the dataset which
        were not yet uploaded at the beginning of this transaction.
    @type __uploading_chunks: set
    @invariant: consists_of(__uploading_chunks, ChunkFromFilesFinal)

    @ivar __uploaded_chunks: the list of the chunks in this dataset
        whose upload is already completed.
    @type __uploaded_chunks: set
    @invariant: consists_of(__uploaded_chunks, Chunk)

    @ivar __progress_notif_deferreds: the list of deferred objects, each
        related to one of the progress notifications being queued to send out.
    @type __progress_notif_deferreds: list
    @ivariant: consists_of(__progress_notif_deferreds, Deferred)

    @ivar paused: the variable for external controlling whether the backup
        transaction is temporarily suspended (but should be continued as soon
        as the variable is set back to C{False}).
    @type paused: bool

    @invariant: self.__all_chunks ==
                self.__uploading_chunks | self.__uploaded_chunks
    """

    __slots__ = ('paused', 'dataset', '__chunks_by_size_code', 'target_hosts',
                 '__all_chunks', '__uploading_chunks', '__uploaded_chunks',
                 '__progress_notif_deferreds', '__progress_notif_deferredlist',
                 '__random', '__cryptographer', 'ack_result_code')

    # Key: dataset uuid;
    # value: the BackupTransaction_Host transaction
    per_dataset_transactions = WeakValueDictionary()
    per_dataset_transactions_lock = Lock()



    class State(AbstractHostTransaction.State):
        """The state for the BACKUP transaction on the Host."""

        __slots__ = ()

        name = 'BACKUP'



    def __init__(self, *args, **kwargs):
        """Constructor."""
        super(BackupTransaction_Host, self).__init__(*args, **kwargs)
        self.__random = Random(42)

        with db.RDB() as rdbw:
            my_user = HostQueries.HostUsers.get_my_user(rdbw)

        self.__cryptographer = None

        self.__progress_notif_deferreds = []
        self.__progress_notif_deferredlist = None

        self.ack_result_code = BackupMessage.ResultCodes.OK
        self.paused = False
        self.dataset = None


    @classmethod
    def __create_dataset_from_incoming_message(self, _message):
        """
        When the incoming message is received, the dataset is created
        from its data; i.e. either the existing one is used,
        or a completely new one is generated.

        @rtype: DatasetOnChunks, NoneType
        """
        _my_host = _message.dst

        if _message.dataset_uuid is not None:
            # Using the prebuilt dataset.

            with db.RDB() as rdbw:
                _dataset = \
                    HostQueries.HostDatasets.get_my_ds_in_progress(
                        _my_host.uuid, _message.dataset_uuid, rdbw)

                if _dataset is not None:
                    assert _dataset.uuid == _message.dataset_uuid, \
                           (_dataset.uuid, _message.dataset_uuid)

        else:
            raise Exception('Not supported message: {!r}'.format(_message))

        return _dataset


    @pauses
    def on_begin(self):
        """
        @todo: Add errback too.
        """
        cls = self.__class__

        _message = self.message
        _host = _message.dst
        logger.debug('Starting backup...')

        _dataset = self.dataset \
                 = cls.__create_dataset_from_incoming_message(_message)

        if self.manager.app.feature_set.per_group_encryption:
            # Read group key from the user group
            with db.RDB() as rdbw:
                _ugroup = Queries.Inhabitants.get_ugroup_by_uuid(
                              _dataset.ugroup_uuid, rdbw)
            group_key = _ugroup.enc_key
        else:
            group_key = None

        self.__cryptographer = Cryptographer(group_key=group_key,
                                             key_generator=None)

        logger.debug('Created dataset %r.', _dataset)
        if _dataset is None:
            raise Exception('No dataset!')
        else:
            self.__notify_about_backup_started()
            self.__notify_about_backup_running()

            ds_uuid = _dataset.uuid

            with cls.per_dataset_transactions_lock:
                if ds_uuid in cls.per_dataset_transactions:
                    self.ack_result_code = BackupMessage.ResultCodes \
                                                        .GENERAL_FAILURE
                    raise Exception('The dataset {} is already being '
                                        'backed up'.format(ds_uuid))
                else:
                    cls.per_dataset_transactions[ds_uuid] = self

                    # Force copying it to dict, to don't cause
                    # race conditions during the logger message serialization.
                    logger.debug('Added backup %r, per dataset transactions '
                                     'are now %r',
                                 ds_uuid, dict(cls.per_dataset_transactions))

            if _dataset is None:
                raise Exception('The dataset {} is not found.'.format(ds_uuid))

            # Initialize chunks.
            # Please note that these chunks may include the ones
            # which are actually present in the cloud already
            # but under a different UUID.
            # This will be fixed later, after NEED_INFO_ACK is received.

            # All chunks, including the already uploaded ones;
            # contains ChunkFromFilesFinal objects.

            # _dataset is MyDatasetOnChunks.
            # dataset.__chunks is list of ChunkFromFilesFinal.

            self.__all_chunks = set(_dataset.chunks())
            assert consists_of(self.__all_chunks, ChunkFromFilesFinal), \
                   repr(self.__all_chunks)
            # Already uploaded chunks; contains Chunk objects.
            with db.RDB() as rdbw:
                self.__uploaded_chunks = \
                    set(HostQueries.HostChunks
                                   .get_uploaded_chunks(_dataset.uuid,
                                                        rdbw=rdbw))
            assert consists_of(self.__uploaded_chunks, Chunk), \
                   repr(self.__uploaded_chunks)
            # Only the pending chunks.
            self.__uploading_chunks = {ch for ch in self.__all_chunks
                                          if ch not in self.__uploaded_chunks}
            assert consists_of(self.__uploading_chunks, ChunkFromFilesFinal), \
                   repr(self.__uploading_chunks)

            #
            # Now create the NEED_INFO transaction.
            # But only if we have chunks to ask!
            #
            if self.__uploading_chunks:
                _query = {
                    'select': ('chunks.uuid', 'uuid'),
                    'from': 'chunks',
                    'where': {'["hash", "size", "uuid"]':
                                  [c for c in self.__uploading_chunks
                                     if c.hash is not None]}
                }

                nifn_tr = self.manager.create_new_transaction(
                              name='NEED_INFO_FROM_NODE',
                              src=_message.dst,
                              dst=self.manager.app.primary_node,
                              parent=self,
                              # NEED_INFO_FROM_NODE-specific
                              query=_query)

                nifn_tr.completed.addCallbacks(self._on_child_nifn_completed,
                                               partial(logger.error,
                                                       'NI issue: %r'))

            else:
                logger.debug('IMHO, no new chunks to upload. '
                             'Proceeding directly.')
                # Go to the next step directly.
                self._ask_for_backup_hosts()


    @exceptions_logged(logger)
    @contract_epydoc
    def _on_child_nifn_completed(self, ni_state):
        """
        This method is called after the child NEED_INFO_FROM_NODE transaction
        has succeeded.

        @type ni_state: NeedInfoFromNodeTransaction_Host.State
        """
        _message = self.message
        logger.debug('Received response to NEED_INFO_FROM_NODE')

        # This is a dictionary mapping local chunk UUID
        # to the cloud chunk UUID number which should be used instead.
        uuids_to_fix = ni_state.ack_result
        logger.debug('Need to fix: %r', uuids_to_fix)

        _all_chunks = self.__all_chunks

        if uuids_to_fix:
            assert isinstance(uuids_to_fix, col.Mapping), repr(uuids_to_fix)
            # Which chunks are present under a different name?
            misnamed_chunk_uuids = {k for k, v in uuids_to_fix.iteritems()
                                      if k != v}
            assert consists_of(misnamed_chunk_uuids, UUID), \
                   repr(misnamed_chunk_uuids)
            misnamed_chunks = {c for c in self.__all_chunks
                                 if c.uuid in misnamed_chunk_uuids}

            # 1. These chunks should be considered already uploaded,..
            self.__uploaded_chunks |= misnamed_chunks
            self.__uploading_chunks -= misnamed_chunks
            # 2. ... renamed in the database,..
            HostQueries.HostChunks.mark_chunks_as_already_existing(
                uuids_to_fix)
            # 3. ... and renamed in the state (while needed).
            for ch in self.__all_chunks:
                if ch.uuid in uuids_to_fix:
                    ch.uuid = uuids_to_fix[ch.uuid]

        # Now we finally know the real set of the chunks which need
        # to be uploaded. Go to the next step.
        self._ask_for_backup_hosts()


    def _ask_for_backup_hosts(self):
        """
        Next step of the procedure: ask for hosts which will accept out chunks.
        """
        _message = self.message

        if __debug__:
            logger.debug('Backing up: %i blocks, %i chunks',
                         sum(len(ch.blocks)
                                 for ch in self.__uploading_chunks),
                         len(self.__all_chunks))

        # Select only size codes with non-empty chunk lists
        self.__chunks_by_size_code = {}
        for chunk in self.__uploading_chunks:
            self.__chunks_by_size_code.setdefault(chunk.maxsize_code, []) \
                                      .append(chunk)

        logger.debug('%s with uploading dataset',
                     'Start' if self.__uploaded_chunks else 'Proceed')

        # Now let's start the nested PROVIDE_BACKUP_HOSTS transaction.
        _chunk_count_by_size_code = \
            {k: len(v)
                 for k, v in self.__chunks_by_size_code.iteritems()}

        pbh_tr = self.manager.create_new_transaction(
                     name='PROVIDE_BACKUP_HOSTS',
                     src=_message.dst,
                     dst=_message.src,
                     parent=self,
                     # PROVIDE_BACKUP_HOSTS-specific
                     chunk_count_by_size_code=_chunk_count_by_size_code)

        pbh_tr.completed.addCallbacks(self._on_child_pbh_completed,
                                      partial(logger.error, 'PBH issue: %r'))


    @exceptions_logged(logger)
    @contract_epydoc
    def _on_child_pbh_completed(self, pbh_state):
        """
        This method is called after the child PROVIDE_BACKUP_HOSTS transaction
        has succeeded.

        @type pbh_state: ProvideBackupHostsTransaction_Host.State
        """
        if not ProvideBackupHostsMessage.ResultCodes \
                                        .is_good(pbh_state.ack_result_code):
            # Our backup request was rejected!
            self.ack_result_code = \
                BackupMessage.ResultCodes.from_provide_backup_host_result_code(
                    pbh_state.ack_result_code)
            self.__complete_backup_transaction()

        else:
            # Proceed with the backup
            self.target_hosts = \
                _target_hosts = \
                    {Host(uuid=uuid,
                          urls=per_host.urls):
                             col.Counter(per_host.chunks_by_size)
                         for uuid, per_host in pbh_state.ack_hosts_to_use
                                                        .iteritems()}

            self.manager.app.known_peers.update(
                {host.uuid: host
                     for host in _target_hosts.iterkeys()})

            logger.debug('CHILD PBH COMPLETED (%r), using target hosts '
                             'for backup: %r',
                         pbh_state,
                         _target_hosts.keys())

            _message = self.message


            @exceptions_logged(logger)
            @contract_epydoc
            def _on_dataset_progress_success(p_state):
                """
                This method is called after the PROGRESS transaction
                reporting to the node about the dataset has succeeded.

                @type p_state: ProgressTransaction_Host.State
                """
                logger.info('Reported to the Node about the dataset %r '
                                'successfully.',
                            self.dataset)
                self.__notify_about_upload_progress()
                self.__upload_more_chunks()


            # Notify the node about the new dataset which started uploading.
            p1_tr = self.manager.create_new_transaction(name='PROGRESS',
                                                        src=_message.dst,
                                                        dst=_message.src,
                                                        parent=self,
                                                        # PROGRESS-specific
                                                        dataset=self.dataset)
            p1_tr.completed.addCallbacks(
                _on_dataset_progress_success,
                partial(logger.error, 'Dataset reporting issue: %r'))


    def __upload_more_chunks(self):
        """
        Try upload some more chunks. If no more chunks can be uploaded,
        unpause the transaction.
        """
        assert not in_main_thread()

        logger.debug('Should we upload more chunks? '
                         'Current ack_result_code is %r',
                     self.ack_result_code)
        if BackupMessage.ResultCodes.is_good(self.ack_result_code):
            logger.debug('So far so good...')
            if not self.paused:
                self.__notify_about_backup_running()

                if not self.__try_upload_next_chunks():
                    self._no_more_chunks_but_wait_for_progresses()
            else:
                logger.debug('%r paused, retry in a second', self)
                callLaterInThread(1.0, self.__upload_more_chunks)
        else:
            logger.debug('Must wait for progresses')
            self._no_more_chunks_but_wait_for_progresses()


    @exceptions_logged(logger)
    def _no_more_chunks_but_wait_for_progresses(self):
        """
        We are almost done with the transaction,
        but still probably need to wait for PROGRESS transaction
        before we can safely report backup as completed.

        @precondition: self.__progress_notif_deferredlist is None
        @postcondition: self.__progress_notif_deferredlist is not None
        """
        self.__progress_notif_deferredlist = \
            DeferredList(self.__progress_notif_deferreds)
        self.__progress_notif_deferredlist.addBoth(
            exceptions_logged(logger)(
                lambda ignore: self._no_more_chunks()))
        logger.debug('We have no more chunks to upload, '
                         'but waiting for the progresses: %r',
                     self.__progress_notif_deferreds)


    @exceptions_logged(logger)
    def _no_more_chunks(self):
        logger.debug('All nested PROGRESS messages completed.')

        if BackupMessage.ResultCodes.is_good(self.ack_result_code):
            logger.debug('No more chunks!')
            _message = self.message
            _my_host = self.manager.app.host

            self.dataset.time_completed = datetime.utcnow()


            @exceptions_logged(logger)
            @contract_epydoc
            def _on_final_dataset_progress_success(p_state):
                """
                This method is called after the PROGRESS transaction
                reporting to the node about the dataset has succeeded.

                @type p_state: ProgressTransaction_Host.State
                """
                logger.debug('Finalizing the %r backup.', self.dataset)

                with db.RDB() as rdbw:
                    # Mark the current dataset as completed
                    # only after the response from the node is received.
                    Queries.Datasets.update_dataset(_my_host.uuid,
                                                    self.dataset,
                                                    rdbw)


            # self.__all_chunks is a collection of ChunkFromFilesFinal

            # Notify the node about the dataset which was just uploaded.
            p2_tr = self.manager.create_new_transaction(
                        name='PROGRESS',
                        src=_my_host,
                        dst=_message.src,
                        parent=self,
                        # PROGRESS-specific
                        dataset=self.dataset,
                        chunks=self.__all_chunks)
            p2_tr.completed.addCallbacks(
                _on_final_dataset_progress_success,
                partial(logger.error, 'Dataset final reporting issue: %r'))


            @exceptions_logged(logger)
            def _complete_backup_on_progress_completed(ignore):
                self.__complete_backup_transaction()


            p2_tr.completed.addBoth(_complete_backup_on_progress_completed)

        else:
            self.__complete_backup_transaction()


    def __notify_about_backup_started(self):
        _dataset = self.dataset
        logger_status_backup.info('Backup started',
                                  extra={'status': 'started',
                                         'result_str': None,
                                         'ds_uuid': _dataset.uuid,
                                         'relaunch_in_mins': None})


    def __notify_about_backup_running(self):
        _dataset = self.dataset
        logger_status_backup.info('Backup is running',
                                  extra={'status': 'running',
                                         'result_str': None,
                                         'ds_uuid': _dataset.uuid,
                                         'relaunch_in_mins': None})


    def __notify_about_backup_end(self, is_good):
        _dataset = self.dataset
        _result_str = BackupMessage.ResultCodes.to_str(self.ack_result_code)

        logger_status_backup.info(
            'Backup ended',
            extra={
                'status': 'ok' if is_good else 'fail',
                'result_str': _result_str,
                'ds_uuid': _dataset.uuid,
                'relaunch_in_mins': None if is_good
                                         else RELAUNCH_AFTER_MINUTES_ON_ERROR
            })


    def __notify_about_upload_progress(self):
        """
        Report a status log about the current progress of chunks upload.
        """
        _dataset = self.dataset
        _uploaded_count = len(self.__uploaded_chunks)
        _total_count = len(self.__all_chunks)
        _uploaded_size = sum(chunk.size() for chunk in self.__uploaded_chunks)
        _total_size = sum(chunk.size() for chunk in self.__all_chunks)

        logger_status_backup_ch_upl.info(
            '-- Upload progress: %i of %i --', _uploaded_count, _total_count,
            extra={'num': _uploaded_count,
                   'of': _total_count,
                   'num_bytes': _uploaded_size,
                   'of_bytes': _total_size,
                   'ds_uuid': _dataset.uuid})


    def __take_next_chunks_to_upload(self, chunk_count_by_size):
        """
        Take out the next bunch of chunks to upload from the transaction state.

        @param chunk_count_by_size: the mapping from chunk size code
            to the required number of such chunks.
        @type chunk_count_by_size: col.Mapping
        """
        M = 1024 * 1024
        try_next_chunks = []
        add_more = True
        logger.debug('Take some chunks to upload: %r', chunk_count_by_size)
        while add_more:
            # What chunk size we are going to upload next?
            try_next_chunk_size = min(chunk_count_by_size.iterkeys())

            # How much more chunks with this chunk size we may upload
            # to this host?
            chunk_size_count_with_this_chunk_size = \
                chunk_count_by_size.get(try_next_chunk_size, 0)
            assert chunk_size_count_with_this_chunk_size > 0, \
                   repr(chunk_size_count_with_this_chunk_size)

            # Decrease the chunk size count, and remove completely if 0
            chunk_size_count_with_this_chunk_size -= 1
            if chunk_size_count_with_this_chunk_size:
                chunk_count_by_size[try_next_chunk_size] = \
                    chunk_size_count_with_this_chunk_size
            else:
                del chunk_count_by_size[try_next_chunk_size]

            assert self.__chunks_by_size_code, \
                   'We still have the chunks allowed to upload, '\
                       "but don't have the chunks available."

            # What next chunk we will use? First find the list
            # of the chunks...
            chunks_of_this_size = \
                self.__chunks_by_size_code.get(try_next_chunk_size, [])
            assert chunks_of_this_size
            # Then select the next chunk, remove if from the list
            # and wipe the list if this was the last chunk
            try_next_chunk = chunks_of_this_size.pop()
            if not chunks_of_this_size:
                del self.__chunks_by_size_code[try_next_chunk_size]

            try_next_chunks.append(try_next_chunk)

            # In what cases do we need to stop
            # forming the CHUNKS message?
            _current_total_size = sum(c.size()
                                          for c in try_next_chunks)
            add_more = (chunk_count_by_size and
                        MERGE_CHUNKS_ON_BACKUP and
                        _current_total_size < MAX_CHUNK_MESSAGE_SIZE * M)

        return try_next_chunks


    def __put_back_non_uploaded_chunks(self, host, chunks):
        """
        Put back (into the transaction state) some chunks which were not
        successfully uploaded.

        @type chunks: col.Iterable
        """
        # [1/2] Put back the host (if needed),
        # and increment the number of pending chunks.

        # [1a] Put back the hosts
        if host not in self.target_hosts:
            logger.debug('For %r, the non-uploaded counts were missing, '
                             'restoring',
                         host)
            self.target_hosts[host] = _target_hosts = col.Counter()

        # [1b] Increment the counts
        put_back_chunk_sizes = col.Counter(ch.maxsize_code for ch in chunks)
        logger.debug('Need to pend back the following chunk sizes: %r',
                     put_back_chunk_sizes)
        self.target_hosts[host] += put_back_chunk_sizes

        # [2/2] Put back the chunks.
        logger.debug('Putting back %i chunk(s): %r',
                     len(chunks), [ch.uuid for ch in chunks])
        for chunk in chunks:
            self.__chunks_by_size_code.setdefault(chunk.maxsize_code, []) \
                                      .append(chunk)


    def __try_upload_next_chunks(self):
        """
        We received the information which hosts we should use;
        now find the next host and try to upload the next chunk
        (or maybe multiple chunks) to it.

        @returns: Whether any chunk was in fact uploaded.
        @rtype: bool

        @todo: If the connection fails to the url, we must delete it
               from the list.
        """
        assert not in_main_thread()

        _dataset = self.dataset

        logger.debug('Trying to upload next chunk(s)...')

        # If we could not proceed with something on this iteration,
        # but there is still other information which may be used,
        # we just retry it. We could've just call the same function again
        # and again, but risk failing due to the limited stack.
        while True:
            if not self.target_hosts:
                if self.__chunks_by_size_code:
                    # We still have some chunks not uploaded:
                    # the backup transaction failed!
                    self.ack_result_code = BackupMessage.ResultCodes \
                                                        .GENERAL_FAILURE
                    logger.debug('Error: no target hosts, '
                                     'but still some chunks: %r',
                                 self.__chunks_by_size_code)

                return False

            else:
                logger.debug('Backup targets %r', self.target_hosts)
                # Select any next host, pseudo-randomly (but reproducibly)
                target_host = \
                    self.__random.choice(sorted(self.target_hosts.keys()))
                chunk_count_by_size = self.target_hosts[target_host]

                if not chunk_count_by_size:
                    logger.debug('Good! No more chunks allowed for %r!',
                                 target_host)
                    del self.target_hosts[target_host]
                    # Try again, probably with the other host.
                    logger.debug('Remaining %r', self.target_hosts)
                    continue

                assert 0 not in chunk_count_by_size.itervalues(), \
                       repr(chunk_count_by_size)

                # Shall we send a single chunk or all chunks altogether?
                # Analyze chunk(s) to send and put it/them
                # into the try_next_chunks variable.

                logger.debug('We need to upload such chunks to %r: %r',
                             target_host, chunk_count_by_size)

                try_next_chunks = self.__take_next_chunks_to_upload(
                                      chunk_count_by_size)

                # We've found the list of one (or maybe more) chunks to send,
                # let's encrypt and post them.
                try_next_chunks_encrypted = \
                    map(partial(EncryptedChunkFromFiles.from_non_encrypted,
                                self.__cryptographer),
                        try_next_chunks)

                # We collected the chunks to upload!
                # Start the nested CHUNKS transaction then.

                logger.debug('Sending %d chunk(s) in a single batch',
                             len(try_next_chunks_encrypted))

                _message = self.message

                assert target_host.uuid in self.manager.app.known_peers, \
                       u'Host {!r} is not in {!r}' \
                           .format(target_host,
                                   self.manager.app.known_peers.keys())

                # repr(try_next_chunks) might be pretty long,
                # so let's '.verbose()' it.
                logger.verbose('b. Will upload chunks %r to %r',
                               try_next_chunks, target_host.urls)

                c_tr = self.manager.create_new_transaction(
                           name='CHUNKS',
                           src=_message.dst,
                           dst=target_host,
                           parent=self,
                           # CHUNKS-specific
                           chunks=try_next_chunks_encrypted)

                # Do NOT change the following lines to "addCallbacks":
                # "__on_chunks_failure()" must be called even if the internals
                # of "__on_chunks_success" have failed!
                c_tr.completed.addCallback(
                    self.__on_chunks_success,
                    _message.dst, target_host, try_next_chunks)
                c_tr.completed.addErrback(
                    self.__on_chunks_failure,
                    target_host, try_next_chunks)

                c_tr.completed.addBoth(
                    exceptions_logged(logger)(
                        lambda ignore: self.__upload_more_chunks()))
                # Try again for the next chunk
                # only when the started transaction succeeds.
                return True


    @exceptions_logged(logger)
    @contract_epydoc
    def __on_chunks_success(self, c_state, from_host, to_host, chunks):
        """
        This method is called after the child CHUNKS transaction has succeeded.

        @type c_state: ChunksTransaction_Host.State
        @type from_host: Host
        @type to_host: Host
        @type chunks: col.Iterable
        """
        from_uuid, to_uuid = from_host.uuid, to_host.uuid

        uploaded_chunks = set(c_state.chunks)

        logger.debug('Uploaded the child chunks from %r to %r: %r',
                     from_host, to_host, uploaded_chunks)


        @exceptions_logged(logger)
        def _on_chunks_progress_success(p_state):
            """
            This method is called after the PROGRESS transaction
            reporting to the node about the uploaded chunks has succeeded.

            @type p_state: ProgressTransaction_Host.State
            """
            logger.debug('Reported progress successfully for %i chunks!',
                         len(uploaded_chunks))
            # Just uploaded the chunk(s), so mark it/them
            # as uploaded locally.
            for _ch in uploaded_chunks:
                HostQueries.HostChunks.mark_chunk_uploaded(_ch.uuid)


        @exceptions_logged(logger)
        def _on_chunks_progress_failure(failure):
            logger.error('Chunks reporting issue: %r', failure)
            # Unfortunately, we've already reported that
            # some chunks were uploaded; but the Node doesn't know that.
            # Let's "go back" in the progress logs.
            self.__uploaded_chunks -= uploaded_chunks
            self.__notify_about_upload_progress()


        # We've uploaded some more chunks.
        # They cannot be fully considered "safe" though
        # until the Node confirms it received the proper Progress message.
        # But for more actual progress report, we can generate the info log
        # right now.
        # In case if Progress fails later, we'll just rollback
        # some chunk counts (see _on_chunks_progress_failure()).
        self.__uploaded_chunks |= uploaded_chunks
        self.__notify_about_upload_progress()

        pr_def = c_state.out_progress_deferred
        if pr_def is None:
            assert not c_state.outgoing_success, repr(c_state)

            logger.debug('Outgoing CHUNKS transaction has actually failed '
                             'on the receiver side; we fail here manually')

            # Emulate the error
            failure = Failure(exc_value=Exception('CHUNKS failed on '
                                                  'the receiver side'))
            self.__on_chunks_failure(failure,
                                     target_host=to_host, failed_chunks=chunks)

        else:  # if pr_def is not None
            assert c_state.outgoing_success, repr(c_state)

            # Do NOT change to addCallbacks!
            pr_def.addCallback(_on_chunks_progress_success)
            pr_def.addErrback(_on_chunks_progress_failure)

            self.__progress_notif_deferreds.append(pr_def)
            logger.debug('Appending deferred for chunks %r: %r',
                         uploaded_chunks, pr_def)


    @exceptions_logged(logger)
    def __on_chunks_failure(self, failure, target_host, failed_chunks):
        """This method is called after the child CHUNKS transaction has failed.

        @type failure: Failure

        @param target_host: to what host the chunks were uploaded.
        @type target_host: Host
        """
        logger.error('Failed to upload chunks to %r (due to %r: %s), '
                         'retrying them again: %r',
                     target_host, failure, failure.getErrorMessage(),
                     [ch.uuid for ch in failed_chunks])

        self.__put_back_non_uploaded_chunks(target_host, failed_chunks)


    @unpauses
    def __complete_backup_transaction(self):
        """
        Just complete the backup transaction, no matter of success or error.
        """
        logger.debug('Completing the backup!')


    def on_end(self):
        """
        @note: In error case, .dataset may be None.
        """
        cls = self.__class__

        if self.dataset is not None:
            _ds_uuid = self.dataset.uuid
            with cls.per_dataset_transactions_lock:
                assert _ds_uuid in cls.per_dataset_transactions, \
                       (_ds_uuid, cls.per_dataset_transactions)
                del cls.per_dataset_transactions[_ds_uuid]
                logger.debug('Removed backup %r, per dataset transactions '
                                 'are now %r',
                             _ds_uuid, cls.per_dataset_transactions)

        self.message_ack = self.message.reply()
        self.message_ack.ack_result_code = self.ack_result_code

        self.manager.post_message(self.message_ack)

        _dataset = self.dataset

        if _dataset is not None:
            is_result_good = BackupMessage.ResultCodes.is_good(
                                 self.ack_result_code)
            self.__notify_about_backup_end(is_good=is_result_good)
            if not is_result_good:
                # Specially to be trackable by the user.
                logger.error('Backup failed: %r', self)

                self.manager.app \
                            .relaunch_backup(self.dataset.uuid,
                                             RELAUNCH_AFTER_MINUTES_ON_ERROR)
