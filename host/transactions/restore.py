#!/usr/bin/python
"""
RESTORE transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import os
import sys
import shutil
import time
import traceback
from datetime import datetime, timedelta
from os.path import abspath
from threading import RLock
from types import NoneType

import dateutil.tz

from contrib.dbc import contract_epydoc

from common.abstract_dataset import AbstractBasicDatasetInfo
from common.abstractions import unpauses, unpauses_incoming
from common.chunks import LocalPhysicalFileState
from common.crypto import (
    Cryptographer, co_crc32, crc32 as eager_crc32, co_fingerprint)
from common.datasets import DatasetWithDirectories
from common.db import Queries
from common.itertools_ex import consume, co_reshape_bytestream
from common.path_ex import sanitize_path
from common.typed_uuids import DatasetUUID, UserGroupUUID
from common.utils import coalesce, open_wb
from common.twisted_utils import callLater

from protocol import transactions
from protocol.messages import RestoreMessage

from host import db, settings as host_settings
from host.db import HostQueries

from ._host import AbstractHostTransaction



#
# Constants
#

logger = logging.getLogger(__name__)

TZ_LOCAL = dateutil.tz.tzlocal()
TZ_UTC = dateutil.tz.tzutc()


# timeout for path removal from ignore
REMOVAL_FROM_FS_IGNORE_TIMEOUT = timedelta(seconds=1)
MAX_TRIES_TO_GET_FILE_NAME_OVERRIDE = 10


#
# Classes
#

class ChecksumMismatch(Exception):
    pass


class CannotRestoreFile(Exception):
    """
    An exception to be raised when the file restoring cannot proceed
    successfully.
    """
    pass


class FileCorrupt(Exception):
    """
    An exception to be raised when the attempt to restore the file has failed
    leaving the file in corrupt state.
    """
    pass


class RestoreTransaction_Host(transactions.RestoreTransaction,
                              AbstractHostTransaction):
    """RESTORE transaction on the Host.

    @cvar restore_targets_by_wr_uuid: the Restore transaction performs the
        factual restoration at the end of processing; but to what
        directory should it restore the files?
        The answer is simple: if the Restore transaction was launched by the
        outer WantRestore (on the Node), it must use the directory WR knows;
        if the Restore transaction was initiated by the Node, then the files
        should be written to the default sync directory.
        C{restore_targets_by_wr_uuid} is a mapping of a WantRestore transaction
        UUID (known by WantRestore transaction here on the Host; and passed
        inside the Restore message from the Node, when WR creates child R
        transaction) to the desired target directory.
        If WR UUID is given in the R message, we must use the appropriate
        directory taken from the C{restore_targets_by_wr_uuid} mapping
        (or generate a warning if there is no any: this means that the host
        likely died for a moment during the restoration);
        If WR UUID is not given in the R message, we restore to the default
        sync dir.

    @type restore_targets_by_wr_uuid: col.MutableMapping
    """

    __slots__ = ()


    restore_targets_by_wr_uuid = {}

    _path_operation_lock = RLock()


    class State(AbstractHostTransaction.State):
        """The state for the RESTORE transaction on the Host."""

        __slots__ = ('ack_result_code',)

        name = 'RESTORE'


        def __init__(self, *args, **kwargs):
            super(RestoreTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.ack_result_code = RestoreMessage.ResultCodes.GENERAL_FAILURE


        def __str__(self):
            return u'ack_result_code={self.ack_result_code!r}' \
                       .format(self=self)



    @unpauses_incoming
    def on_begin(self):
        assert self.is_incoming()

        self.__restore_files()


    def __get_restore_directory(self):
        """
        Get the restore directory to use, or None if it cannot be found/used.

        The result is always normalized/absolute.
        """
        _message = self.message

        my_host = self.manager.app.host
        ugroup = _message.ugroup
        wr_uuid = _message.wr_uuid
        time_started = datetime.utcnow()

        target_dir = None  # we'll modify it and return

        # The dataset is considered "auto-sync" if it was invoked by the node,
        # i.e. it's wr_uuid is None
        #assert _message.sync == (wr_uuid is None) == (ds.uuid is not None), \
        #       (_message.sync, wr_uuid, ds.uuid)

        if _message.sync:
            # The Restore transaction was invoked by the Node itself.
            # For "sync" datasets, the restore directory
            # is the synced directory.
            logger.debug('RDB lookup for restore directory for %r', ugroup)
            with db.RDB() as rdbw:
                target_dir = \
                    HostQueries.HostFiles.get_base_directory_for_ugroup(
                        ugroup.uuid, rdbw)
            if target_dir is None:
                logger.debug('%r is not yet bound to the user', ugroup)
                self.manager.app.add_me_to_group(ugroup)
                target_dir = host_settings._get_ugroup_default_sync_dir(
                                 my_host.uuid, ugroup)
        elif host_settings.get_feature_set().web_initiated_restore:
            # 1. New-style logic:
            #    the directory is made of dataset.time_started
            target_dir = host_settings.get_default_restore_dir(my_host.uuid)
            subdir_name = time_started.strftime('%d_%B_%Y_%H-%M-%S')
            target_dir = os.path.join(target_dir, subdir_name)
        else:
            # 2. Old-style logic:
            #    the directory was saved before, in WantRestore.
            target_dir = \
                RestoreTransaction_Host.restore_targets_by_wr_uuid \
                                       .get(wr_uuid)
            if target_dir is None:
                logger.debug('No target directory stored for %r', wr_uuid)

        return abspath(target_dir)


    @contract_epydoc
    def __delete_single_file(self, file_, file_path):
        """Actual delete procedure for a single file.

        @param file_: the file to restore.
        @type file_: LocalPhysicalFileState

        @param file_path: what is the real path for the file.
        @type file_path: basestring
        """

        # If it doesn't exist, we don't even need to remove it!
        if os.path.exists(file_path):
            try:
                if os.path.isdir(file_path):
                    # TODO: ticket:183, part 2 - probably, rmtree()
                    # is an overkill and can destroy needed files;
                    # also, ordering of "delete" and "create" operations
                    # must be maintained.
                    shutil.rmtree(file_path)
                else:
                    os.remove(file_path)
            except:
                logger.exception('Cannot delete %r at %r', file_, file_path)
                raise


    def __file_blocks_generator(self, file_, file_blocks, cryptographer):
        """Provides the blocks of a file, ready to be written.

        @returns: a generator of blocks for the file.

        @raises ChecksumMismatch: if some checksum failed during
            the calculation. May be raised during the iteration, or in the end.
        """
        success = True

        # In parallel with generating the contents,
        # let's calculate its CRC32,...
        crc32_gen = co_crc32()
        cur_file_crc32 = crc32_gen.next()
        # ... and fingerprint
        _fp_gen_noreshape = co_fingerprint(cryptographer)
        # Fingerprint calculation needs reshaping to some large round number...
        # let it be 16384 for performance.
        # Though probably, 2048 would be good as well.
        fp_gen = co_reshape_bytestream(_fp_gen_noreshape, 16384)
        cur_file_fp_func = fp_gen.next()

        written_bytes = 0
        while written_bytes < file_.size and success:
            next_blocks = [bl
                               for bl, ch in file_blocks
                                   if bl.offset_in_file == written_bytes]
            if not next_blocks:
                logger.error('Error during writing file %r',
                             file_path)
                logger.debug('File blocks are: %r',
                             file_blocks)
                success = False
            else:
                next_block = next_blocks[0]

                _next_block_chunk = next_block.chunk

                # Read and decrypt the next chunk
                chunk_decrypted = \
                    self.manager.app.chunk_storage.read_and_decrypt_chunk(
                        _next_block_chunk.uuid,
                        _next_block_chunk.size(),
                        _next_block_chunk.crc32,
                        cryptographer)

                # Check the CRC32 of the chunk
                _chunk_crc32 = eager_crc32(chunk_decrypted)
                if _chunk_crc32 != _next_block_chunk.crc32:
                    logger.error('CRC failed for chunk %s: %08X != %08X',
                                 _next_block_chunk.uuid,
                                 _chunk_crc32,
                                 _next_block_chunk.crc32)

                # Get the body of the next block
                _start = next_block.offset_in_chunk
                _end = next_block.offset_in_chunk + next_block.size
                _block_data = chunk_decrypted[_start:_end]

                # Add the body of the block to the whole-file
                # CRC32/Fingerprint calculators,
                # and also write it to the file
                cur_file_crc32 = crc32_gen.send(_block_data)
                cur_file_fp_func = fp_gen.send(_block_data)
                yield _block_data
                written_bytes += next_block.size

        # Loop done, now finalize.

        # Check CRC32
        if cur_file_crc32 != file_.crc32:
            raise ChecksumMismatch(
                      u'CRC mismatch for file {:s} at maybe {!r}: '
                      u'{:08X} != {:08X}'
                          .format(file_.uuid, file_.full_path,
                                  cur_file_crc32, file_.crc32))
        else:
            # If CRC32 matches, additionally check fingerprint:
            cur_file_fp = cur_file_fp_func()

            # Check Fingerprint
            if cur_file_fp != file_.fingerprint:
                logger.error('Cannot recalculate from the file anymore '
                                 '(%r ! %r for %r/%r)',
                             cur_file_fp, file_.fingerprint,
                             file_.uuid, file_.full_path)

            # Check Fingerprint, really.
            if cur_file_fp != file_.fingerprint:
                raise ChecksumMismatch(
                          u'Fingerprint mismatch for file {:s} at maybe {!r}:'
                          u' {!r} != {!r}'
                              .format(file_.uuid, file_.full_path,
                                      cur_file_fp, file_.fingerprint))


    @contract_epydoc
    def __restore_single_file(self,
                              file_path, file_, file_blocks, cryptographer):
        """Actual restore procedure for a single file.

        @param file_: the file to restore.
        @type file_: LocalPhysicalFileState

        @param file_path: what is the real path for the file.
        @type file_path: basestring

        @param file_blocks: the iterable of blocks used to restore the file
            (if needed).
        @type file_blocks: col.Iterable

        @param cryptographer: C{Cryptographer} object to use for decrypting.
        @type cryptographer: Cryptographer

        @raises CannotRestoreFile: if checksum or fingerprint of restored file
            is not correct, and that was spotted before corrupting
            the actual file.
        @raises FileCorrupt: if checksum or fingerprint of restored file
            is not correct, and that caused the corruption of a file.
        """
        cls = self.__class__

        file_blocks = list(file_blocks)  # for reiterability

        # 1. Do a "dry run" to try to ensure nothing will be broken
        #    during the restore.
        try:
            consume(self.__file_blocks_generator(file_, file_blocks,
                                                 cryptographer))
        except Exception as e:
            logger.debug('Cannot restore: %s', traceback.format_exc())
            raise CannotRestoreFile('{:s} at {!r}'.format(file_.uuid,
                                                          file_path))
        else:
            # 2. Do a real run of restoration.

            # TODO: do not write if the existing file is newer,
            # but write to a "non-merged copy".
            # In more details:
            # * if the existing file is newer AND "hot" (i.e., not backed up,
            #   or yet buffered to be backed up), this should be treated
            #   as a name conflict.
            # * if the existing file is newer AND "cold" (i.e. in the cloud
            #   already), just think that we've received some too old file
            #   for some reason, and don't write anything.
            try:
                with open_wb(file_path) as fh:
                    for block in self.__file_blocks_generator(file_,
                                                              file_blocks,
                                                              cryptographer):
                        fh.write(block)
                    # Finished the loop over blocks and writing the file.
                    fh.flush()
                    fh.truncate()  # what if we were rewriting a larger file?
                    os.fsync(fh.fileno())
            except Exception as e:
                logger.debug('File corrupt: %s', traceback.format_exc())
                raise FileCorrupt('{:s} at {!r}'.format(file_.uuid,
                                                        file_path))
            else:
                _new_dt = file_.time_changed
                _new_dt_local = _new_dt.replace(tzinfo=TZ_UTC) \
                                       .astimezone(TZ_LOCAL)
                logger.debug('Converted update time from %s to %s',
                             _new_dt, _new_dt_local)
                _new_ts = time.mktime(_new_dt_local.timetuple())
                # _new_ts must be integer
                os.utime(file_path, (_new_ts, _new_ts))
                logger.debug("%r file's mtime force set to %s",
                             file_path, _new_dt)


    def __bind_single_file_state_to_file_if_needed(self,
                                                   ds, base_dir_id, file_):
        """
        @param base_dir_id: the ID of the base directory in the DB,
            to improve performance of queries.
        @type base_dir_id: int, NoneType
        """
        if ds is not None:
            _file_states = [file_]
            logger.verbose('Binding file state %r to %r (base dir %r)',
                           file_, ds, base_dir_id)
            with db.RDB() as rdbw:
                HostQueries.HostFiles.add_file_states(base_dir_id,
                                                      _file_states,
                                                      rdbw)
                HostQueries.HostFiles.bind_file_states_to_files(ds.uuid,
                                                                _file_states,
                                                                rdbw)


    @classmethod
    def suggest_override_path(cls, path, ts,
                              counter=0, _tz_local_override=TZ_LOCAL):
        r"""
        Given the base path and extra information, suggest the filename
        to resolve a file name conflict.

        >>> f = RestoreTransaction_Host.suggest_override_path

        >>> ts1 = datetime(2010, 1, 5, 16, 4, 49)
        >>> ts2 = datetime(2010, 7, 19, 16, 4, 49)
        >>> tz_moscow = dateutil.tz.gettz('Europe/Moscow')
        >>> tz_la = dateutil.tz.gettz('America/Los_Angeles')

        >>> # Properly works with timezones
        >>> f('abc', ts1, _tz_local_override=tz_moscow)
        u'abc (conflict on 2010-01-05 19:04)'
        >>> f('abc', ts1, _tz_local_override=tz_la)
        u'abc (conflict on 2010-01-05 08:04)'
        >>> f('abc', ts2, _tz_local_override=tz_moscow)
        u'abc (conflict on 2010-07-19 20:04)'
        >>> f('abc', ts2, _tz_local_override=tz_la)
        u'abc (conflict on 2010-07-19 09:04)'

        >>> # Properly works with extensions
        >>> f('abc.def.ghi', ts1, _tz_local_override=tz_moscow)
        u'abc.def (conflict on 2010-01-05 19:04).ghi'

        >>> # Properly works with counter
        >>> f('abc.def.ghi', ts1, counter=5, _tz_local_override=tz_moscow)
        u'abc.def (conflict 5 on 2010-01-05 19:04).ghi'

        @param ts: timestamp of the file; must be in UTC.
        @type ts: datetime

        @rtype: basestring
        """
        _root, _ext = os.path.splitext(path)
        ts_local = ts.replace(tzinfo=TZ_UTC).astimezone(_tz_local_override)

        return u'{root} (conflict{opt_counter} on {ts}){ext}'.format(
                   root=_root,
                   ts=ts_local.strftime('%Y-%m-%d %H:%M'),
                   opt_counter=' {:d}'.format(counter) if counter else '',
                   ext=_ext)


    @contract_epydoc
    def __restore_op_for_path(self, file_,
                                    file_blocks,
                                    is_whole_dataset_restored,
                                    base_dir_id,
                                    restore_directory,
                                    cryptographer,
                                    ds):
        """
        Perform the restore-specific operation for a single file
        from the message.

        The actual operation may cause restoring the file (i.e. writing),
        or deleting.

        @param file_: file to run the restore operation on.
        @type file_: LocalPhysicalFileState

        @param file_blocks: the iterable of blocks used to restore the file
            (if needed).
        @type file_blocks: col.Iterable

        @param is_whole_dataset_restored: whether restoring the whole dataset
            (as a part of sync operation, maybe) or just some single files.

        @param base_dir_id: the ID of the base directory in the DB,
            to improve performance of queries.
        @type base_dir_id: int, NoneType

        @param restore_directory: to what directory should the file
            be restored.
        @type restore_directory: basestring

        @param cryptographer: C{Cryptographer} object to use for decrypting.
        @type cryptographer: Cryptographer

        @param ds: the dataset to bind the states to
            (or C{None} if not needed).
        @type ds: NoneType, AbstractBasicDatasetInfo
        """
        _app = self.manager.app
        # What file paths should we use as a basis?
        # Store whole path if a selected files were requested
        # (for compatibility), but write the rel_path if whole dataset
        # was restored
        _base_file_name = \
            '.' + os.sep + (file_.rel_path if is_whole_dataset_restored
                                           else file_.full_path)
        # This path can be overrided later
        file_path = sanitize_path(abspath(os.path.join(restore_directory,
                                                       _base_file_name)))

        try:
            logger.verbose('Ignoring FS changes in %r', file_path)
            _app.ignore_fs_events_for_path(file_path)

            if file_.fingerprint is None:
                logger.debug('Removing the file %r at %r',
                             file_, file_path)
                # Deleting the file.
                # UPD: not just deleting, but also may be adding
                # a directory.
                # TODO: ticket:141 - fix accordingly.
                assert file_.crc32 is None and not file_blocks, \
                       (file_, file_.crc32, file_blocks)
                with RestoreTransaction_Host._path_operation_lock:
                    self.__delete_single_file(file_, file_path)

            else:
                logger.debug('Restoring the file %r at %r (%s bytes)',
                             file_, file_path, file_.size)
                logger.verbose('In RESTORE, receiving file %r: %r',
                               file_, file_blocks)
                if _app.is_path_hot(file_path):
                    do_path_override = True

                # do not try to write file over dir
                try:
                    do_path_override = os.path.isdir(file_path)
                except OSError:
                    # can not access path
                    do_path_override = True

                _file_path = file_path
                if do_path_override:
                    ts = datetime.utcnow() if ds is None \
                                            else ds.time_started
                    counter = 0
                    while (os.path.exists(_file_path) or
                           _app.is_path_hot(_file_path)):
                        _file_path = self.suggest_override_path(
                                         _file_path, ts, counter)
                        counter += 1
                        if counter >= \
                                MAX_TRIES_TO_GET_FILE_NAME_OVERRIDE:
                            raise Exception('Too many tries to get override '
                                            'for path {!r}'
                                                .format(file_path))
                    logger.info('Restore conflict, writing file %r '
                                    'to %r instead of %r',
                                file_, _file_path, file_path)
                    file_path = _file_path

                # TODO: between this point and the actual writing,
                # the overriding _file_path may already become
                # inappropriate; in the best case, we need to do this
                # as close to actual writing as possible,
                # ideally atomically find name/open for writing.

                # Writing/updating the file
                assert file_.crc32 is not None, \
                       (file_, file_.crc32, file_blocks)
                # @todo: better solution for preventing cuncurrent
                # restore
                with RestoreTransaction_Host._path_operation_lock:
                    self.__restore_single_file(
                        file_path, file_, file_blocks, cryptographer)

        except CannotRestoreFile as e:  # from __restore_single_file
            logger.error('Cannot restore the file, but nothing is broken: %s',
                         e)

        except FileCorrupt as e:  # from __restore_single_file
            logger.error('Cannot restore the file, and it was corrupted: %s',
                         e)

        except Exception:
            # We don't even know what the problem was!
            # Possibly, at this point, if there were error during
            # writing file (partial write), we should restore
            # previous version of file...
            logger.exception('Some error has occured during restore '
                                'of the file %r at path %r',
                             file_, file_path)

        else:
            # We've wrote the file successfully, can now mark it
            # as already backed up: add the state and bind the dataset
            # to it.
            self.__bind_single_file_state_to_file_if_needed(
                ds, base_dir_id, file_)

        finally:
            # by now, we've finished writing the file
            ts = datetime.utcnow()

            # Ignore all events for file_path occured before this
            # moment.
            _app.ignore_fs_events_for_path(file_path, ts)

            # Delay removal from ignore
            # it required to drop events from fs notify, that happened
            # during restore (they can be received after restore end..)
            callLater(REMOVAL_FROM_FS_IGNORE_TIMEOUT.total_seconds(),
                    _app.stop_ignoring_fs_events_for_path,
                    file_path, ts)
            logger.verbose('Delaying remove from ignore, '
                            'path: %r, ts: %s', file_path, ts)


    @unpauses
    def __restore_files(self):
        """Internal procedure which actually restores the files.

        @todo: the Fingerprint calculation should be turned into
            "file is read by blocks and then repacked into 16KiB segments";
            then recalculation of the fingerprint in case of FP mismatch
            won't be needed.
        """
        _message = self.message
        my_host = self.manager.app.host
        feature_set = self.manager.app.feature_set
        ds = _message.dataset
        wr_uuid = _message.wr_uuid
        ugroup = _message.ugroup

        restore_directory = self.__get_restore_directory()
        assert _message.sync == (wr_uuid is None) == (ds.uuid is not None), \
               (_message.sync, wr_uuid, ds)

        base_dir_id = None  # will be used later

        if restore_directory is None:
            logger.error('Do not know the restore directory')
        else:
            logger.debug('Going to restore dataset %r for %r to %r',
                         ds, ugroup, restore_directory)
            if not os.path.exists(restore_directory):
                os.makedirs(restore_directory)

            group_key = ugroup.enc_key if feature_set.per_group_encryption \
                                       else None
            cryptographer = Cryptographer(group_key=group_key,
                                          key_generator=None)

            is_whole_dataset_restored = _message.sync

            logger.debug('Restoring %s files from dataset: %r',
                         'all' if is_whole_dataset_restored else 'selected',
                         coalesce(ds, 'N/A'))

            # TODO: use the "delete time" from the LocalPhysicalFileState!
            _now = datetime.utcnow()

            # If we are syncing-in the whole dataset, we should write it
            # into the DB as a whole. The files/file_locals will be bound to it
            # so that after restore, we'll know on this Host that these states
            # are fully synced to the cloud already (in fact, they came
            # from the cloud).
            if _message.sync:
                # Let's hack into the files and substitute the base_dir.
                # TODO: do it better!
                for f in _message.files.iterkeys():
                    f.base_dir = restore_directory

                # Write the whole dataset to the DB
                _small_files = _message.files.keys()  # not iterkeys(0 for now!
                _dirs = {restore_directory: (_small_files, [])}

                # Given the information in the inbound message about
                # the whole dataset, store this dataset in the DB.
                dataset = DatasetWithDirectories(
                              name=ds.name,
                              sync=ds.sync,
                              directories=_dirs,
                              # TODO: transport real data
                              # from the node
                              uuid=DatasetUUID.safe_cast_uuid(ds.uuid),
                              ugroup_uuid=UserGroupUUID.safe_cast_uuid(
                                              ugroup.uuid),
                              time_started=ds.time_started,
                              time_completed=_now)

                with db.RDB() as rdbw:
                    # Do we already have the dataset?
                    _ds_in_progress = \
                        HostQueries.HostDatasets.get_my_ds_in_progress(
                            host_uuid=my_host.uuid,
                            ds_uuid=dataset.uuid,
                            rdbw=rdbw)

                    if _ds_in_progress is None:
                        # We don't have it, insert.
                        dummy_ds_uuid = \
                            HostQueries.HostDatasets.create_dataset_for_backup(
                                my_host.uuid, dataset, rdbw)
                        assert dummy_ds_uuid == dataset.uuid, \
                               (dummy_ds_uuid, dataset.uuid)

                    base_dir_id = \
                        HostQueries.HostFiles.add_or_get_base_directory(
                            restore_directory, ugroup.uuid, rdbw)

            error_in_any_file_occured = False

            #
            # Finally, loop over the files and restore each one
            #
            for file_, file_blocks in _message.files.iteritems():
                self.__restore_op_for_path(file_, file_blocks,
                                           is_whole_dataset_restored,
                                           base_dir_id,
                                           restore_directory,
                                           cryptographer,
                                           ds)

            # Loop over the files completed
            if is_whole_dataset_restored:
                logger.debug('Restoring %r completed, there were %s issues.',
                             ds,
                             'some' if error_in_any_file_occured else 'no')
                if not error_in_any_file_occured:
                    with db.RDB() as rdbw:
                        logger.debug('Updating %r at host %s...',
                                     ds, my_host.uuid)
                        ds_to_finish = \
                            Queries.Datasets.get_dataset_by_uuid(ds.uuid,
                                                                 my_host.uuid,
                                                                 rdbw)

                        ds_to_finish.time_completed = datetime.utcnow()
                        logger.debug('Updating %r as completed', dataset)

                        # Mark the current dataset as completed
                        # only after the response from the node is received.
                        Queries.Datasets.update_dataset(my_host.uuid,
                                                        ds_to_finish,
                                                        rdbw)

            # Everything seems ok to this moment
            with self.open_state(for_update=True) as state:
                state.ack_result_code = RestoreMessage.ResultCodes.OK


    def on_end(self):
        assert self.is_incoming()

        self.message_ack = self.message.reply()
        with self.open_state(for_update=True) as state:
            self.message_ack.ack_result_code = state.ack_result_code

        self.manager.post_message(self.message_ack)
