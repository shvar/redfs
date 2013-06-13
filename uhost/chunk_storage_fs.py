#!/usr/bin/python
"""
The manager performing all the operations on the chunks
storing them on the local filesystem.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import os
import logging
import numbers
import traceback
from glob import glob
from itertools import islice
from threading import RLock
from uuid import UUID

from twisted.application.internet import TimerService
from twisted.internet import reactor

from contrib.dbc import contract_epydoc

from common.chunk_storage import IChunkStorage
from common.chunks import (AbstractChunkWithContents,
                           ChunkWithEncryptedBodyMixin)
from common.crypto import Cryptographer
from common.db import Queries
from common.itertools_ex import ilen, take
from common.system import get_free_space_at_path, fallocate
from common.utils import (
    coalesce, duration_logged, exceptions_logged, gen_uuid, open_wb,
    round_up_to_multiply)

from host import db
from host.db import HostQueries



#
# Logging
#

logger = logging.getLogger(__name__)
logger_status_chunks_op = logging.getLogger('status.chunks_op')
logger_status_chunks_op_error = logging.getLogger('status.chunks_op_error')



#
# Classes
#

class ChunkFromStorage(ChunkWithEncryptedBodyMixin,
                       AbstractChunkWithContents):
    """
    A single data chunk, which body is stored in the storage directory.
    Note the maxsize_code is always None!

    @ivar file_path: The path to the file with the chunk.
    @type file_path: basestring

    @invariant: self.max_code is None
    """
    __slots__ = ('file_path', '__size_cache', '__crc32')


    @contract_epydoc
    def __init__(self, file_path, crc32, *args, **kwargs):
        """
        @type file_path: basestring

        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF
        """
        self.file_path = file_path
        self.__size_cache = None
        self.__crc32 = crc32
        super(ChunkFromStorage, self).__init__(*args, **kwargs)


    def __str__(self):
        return u'{}, from {!r}'.format(super(ChunkFromStorage, self).__str__(),
                                       self.file_path)


    def get_bytes(self):
        """
        @returns: The byte stream for this chunk, as taken from
                  the storage file.
        @rtype: str
        """
        with open(self.file_path, 'rb') as fh:
            return fh.read()


    def phys_size(self):
        """
        The chunk size may be read once from the disk,
        but then cached in the memory.
        """
        if self.__size_cache is None:
            self.__size_cache = os.stat(self.file_path).st_size
        return self.__size_cache


    @property
    @contract_epydoc
    def crc32(self):
        """
        @rtype: numbers.Integral
        @postcondition: 0 <= result <= 0xFFFFFFFF
        """
        return self.__crc32



class ChunkStorageFS(IChunkStorage):
    """
    This class is responsible for storing the chunks on the local filesystem,
    considering such features like filling the place with the dummy chunks
    if needed.

    @todo: Likely, C{__chunk_op_lock} should be changed from RLock to RWLock
           when RWLock is in the standard library.
           All users should be changed too to declare
           what access level (R or W) do they need.
    """
    __slots__ = ('__chunk_dir', '__chunk_op_lock')

    __SINGLETON_INITIALIZED = False


    __CHUNK_DIGIT_TEMPLATE = '[0-9a-f]'
    __CHUNK_FILENAME_TEMPLATE = '{0}-{1}-{2}-{3}-{4}.chunk' \
                                    .format(*['{d:}' * i
                                                  for i in (8, 4, 4, 4, 12)]) \
                                    .format(d=__CHUNK_DIGIT_TEMPLATE)


    def __init__(self, chunk_dir):
        assert not ChunkStorageFS.__SINGLETON_INITIALIZED
        ChunkStorageFS.__SINGLETON_INITIALIZED = True

        self.__chunk_dir = chunk_dir
        self.__chunk_op_lock = RLock()

        logger.debug('Set up ChunkStorageFS at %r successfully',
                     self.__chunk_dir)


    @contract_epydoc
    def __get_chunk_file_path(self, chunk_uuid, is_dummy, dir_path=None):
        """
        For a given uuid (of running host) and the chunk_uuid,
        return the appropriate path to store the chunk files.

        On release:
            '~/.freebrie/chunks/.../00000000-1111-2222-3333-123456789012.chunk'

        @type chunk_uuid: UUID

        @param is_dummy: whether the chunk is dummy.
        @type is_dummy: bool

        @param dir_path: Use this path to override the path for the directory
                         to the chunks; if omitted, the real one is used.
        @type dir_path: basestring
        """
        _ch_hex = chunk_uuid.hex
        subdir1, subdir2 = _ch_hex[7], _ch_hex[6]

        return os.path.join(coalesce(dir_path, self.__chunk_dir),
                            subdir1,
                            subdir2,
                            '{}.chunk'.format(chunk_uuid))


    @contract_epydoc
    def write_chunk(self, chunk, is_dummy=False):
        """
        Given a new chunk, write it to the storage.

        @type chunk: AbstractChunkWithContents

        @type is_dummy: bool
        """
        # For clarity only
        super(ChunkStorageFS, self).write_chunk(chunk, is_dummy)
        file_path = self.__get_chunk_file_path(chunk.uuid, is_dummy=is_dummy)
        logger.debug('Chunk body %s being written to %s',
                     chunk.uuid, file_path)

        with self.__chunk_op_lock:
            if os.path.exists(file_path):
                logger.warning('Attempting to write the chunk %s, '
                                   'but it exists already!',
                               chunk.uuid)
            else:
                bytes = chunk.get_bytes()
                logger.debug('Writing chunk: %r (%i/%i/%i: %s)',
                             chunk,
                             len(bytes),
                             chunk.size(),
                             chunk.phys_size(),
                             chunk.phys_size() == len(bytes))
                with open_wb(file_path) as fh:
                    fh.write(bytes)
                    fh.flush()
                    os.fsync(fh.fileno())


    @contract_epydoc
    def read_chunk(self, chunk_uuid, exp_crc32, exp_size):
        """
        Given a chunk UUID, read it from the storage.

        @type chunk_uuid: UUID

        @type exp_crc32: int

        @param exp_size: expected size of the chunk.
        """
        # For clarity only
        super(ChunkStorageFS, self).read_chunk(chunk_uuid, exp_crc32, exp_size)

        file_path = self.__get_chunk_file_path(chunk_uuid, is_dummy=False)
        logger.debug('Reading chunk %s from %s',
                     chunk_uuid, file_path)

        with self.__chunk_op_lock:
            return ChunkFromStorage(  # ChunkFromStorageFS-specific
                                    file_path=file_path,
                                    crc32=exp_crc32,
                                      # AbstractChunkWithContents-specific
                                      # ---
                                      # Chunk-specific
                                    uuid=chunk_uuid,
                                    size=exp_size)


    @contract_epydoc
    def read_and_decrypt_chunk(self, chunk_uuid, size, crc32, cryptographer):
        """
        Given a chunk UUID, read it from the storage and decrypt.

        @type chunk_uuid: UUID

        @param size: expected size of the chunk (may be lower than the one
            being read due to alignment needed for the encryption).
        @precondition: size > 0

        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @type cryptographer: Cryptographer
        """
        # For clarity only
        super(ChunkStorageFS, self).read_and_decrypt_chunk(chunk_uuid,
                                                           size,
                                                           crc32,
                                                           cryptographer)

        path = self.__get_chunk_file_path(chunk_uuid, is_dummy=False)
        with self.__chunk_op_lock:
            with open(path, 'rb') as fh_read:
                decrypted = cryptographer.decrypt(fh_read.read(), crc32)
                assert len(decrypted) >= size, \
                       (chunk_uuid, size, len(decrypted))
                return decrypted[:size]


    @classmethod
    @contract_epydoc
    def __convert_chunk_filenames_to_uuids(cls, chunk_filenames):
        """
        Given the iterable of filenames with the chunk files,
        return the appropriate list of the chunk UUIDs.

        @rtype: col.Iterable
        """
        # The UUID, in text form, is the name of the file, without extension.
        return (UUID(os.path.splitext(os.path.basename(f))[0])
                    for f in chunk_filenames)


    @duration_logged()
    @contract_epydoc
    def __get_chunk_filenames_on_fs(self,
                                    chunk_dir, max_depth=2):
        """
        For a given host UUID, return the list of the files
        for all the chunks which are present on the file system for this host.

        @param chunk_dir: what directory to examine for chunks.
        @type chunk_dir: basestring

        @result: the (non-reiterable) iterable of the file names
                 for the chunks.
        @rtype: col.Iterable
        """
        cls = self.__class__

        with self.__chunk_op_lock:
            # Accessing the directory with the chunks might be error-prone,
            # so just ignore the file system errors if any, and reflect that
            # in the result accordingly.
            try:
                if max_depth:  # we need to go deeper
                    # Do we have subdirectories?
                    _glob_dirs = glob(os.path.join(chunk_dir,
                                                   cls.__CHUNK_DIGIT_TEMPLATE))
                    chunk_subdirs = (d
                                         for d in _glob_dirs
                                         if os.path.isdir(d))
                    del _glob_dirs  # help GC
                    filenames = \
                        (self.__get_chunk_filenames_on_fs(d,
                                                          max_depth - 1)
                             for d in chunk_subdirs)
                    for per_subdir in filenames:
                        for filename in per_subdir:
                            yield filename

                # Do we have files looking like they are chunks?
                _glob_files = glob(os.path.join(chunk_dir,
                                                cls.__CHUNK_FILENAME_TEMPLATE))
                chunk_files = (f
                                   for f in _glob_files
                                   if os.path.isfile(f))
                del _glob_files  # help GC
                for chunk_file in chunk_files:
                    yield chunk_file

            except Exception as e:
                pass


    @duration_logged()
    @contract_epydoc
    def __get_chunk_uuids_on_fs(self):
        """
        For a given host UUID, return the list of the UUIDs for the chunks
        which are present on the file system for this host.

        Basically, this is just
        __convert_chunk_filenames_to_uuids . __get_chunk_filenames_on_fs

        @result: the (non-reiterable) Iterable of the host UUIDs
                 (contains C{UUID} objects).
        @rtype: col.Iterable
        """
        cls = self.__class__
        return cls.__convert_chunk_filenames_to_uuids(
                   self.__get_chunk_filenames_on_fs(self.__chunk_dir))


    @duration_logged()
    @contract_epydoc
    def __get_dummy_chunk_uuids_on_fs(self):
        """
        For a given host UUID, return the list of the chunk UUIDS
        for all the (dummy-only) chunks which are actually present
        on the file system for this host.

        As a convenient side effect, remove from the DB
        all the dummy chunks which are actually missing from the FS.

        @return: the (possibly non-reiterable) Iterable of the UUIDs
                 for the dummy chunks available on the FS.
        @rtype: col.Iterable
        """
        with self.__chunk_op_lock:
            present_chunk_uuids = frozenset(self.__get_chunk_uuids_on_fs())

            with db.RDB() as rdbw:
                dummy_chunk_uuids_in_db = \
                    set(HostQueries.HostChunks
                                   .get_all_dummy_chunk_uuids(rdbw=rdbw))

            # Do we have dummy chunks in DB actually absent from FS?
            # Wipe them.
            in_db_not_on_fs = dummy_chunk_uuids_in_db - present_chunk_uuids
            dummy_chunk_uuids_in_db -= in_db_not_on_fs
            assert dummy_chunk_uuids_in_db <= present_chunk_uuids, \
                   (dummy_chunk_uuids_in_db, present_chunk_uuids)
            if in_db_not_on_fs:
                logger.debug('%i chunks in DB but not on FS, '
                                 'deleting from DB:\n%r',
                             len(in_db_not_on_fs), in_db_not_on_fs)
                HostQueries.HostChunks.delete_dummy_chunks(in_db_not_on_fs)

            # The actual dummy chunks is the intersection of "present chunks"
            # and "chunks marked as dummy in DB".
            return present_chunk_uuids & dummy_chunk_uuids_in_db


    @contract_epydoc
    def get_real_stored_chunk_uuids(self):
        """
        @returns: the (possibly non-reiterable) Iterable over the chunk UUIDs
                  for the Chunks (real, non-dummy ones) which are stored
                  on this system.
        @rtype: col.Iterable
        """
        # For clarity only
        super(ChunkStorageFS, self).get_real_stored_chunk_uuids()

        # Calculate dummy first, as the calculation
        # also removes the wrong chunks.
        with self.__chunk_op_lock:
            dummy_uuids = frozenset(self.__get_dummy_chunk_uuids_on_fs())
            all_uuids = frozenset(self.__get_chunk_uuids_on_fs())
        return all_uuids - dummy_uuids


    @duration_logged()
    def __create_some_dummy_chunks(self, how_many):
        """
        Create some dummy chunks (in FS and in DB).
        Yields the UUIDs of dummy chunks after creation of each one.

        @param how_many: How many dummy chunks to create.

        @rtype: col.Iterable
        """
        with self.__chunk_op_lock:
            logger.debug('Creating %i new dummy chunk(s)', how_many)

            # One by one, adding the dummy chunks
            for i in xrange(how_many):
                dummy_chunk_uuid = gen_uuid()
                HostQueries.HostChunks.create_dummy_chunk(dummy_chunk_uuid)
                fallocate(self.__get_chunk_file_path(dummy_chunk_uuid,
                                                     is_dummy=True),
                          0x100000)
                yield dummy_chunk_uuid
            logger.debug('Created %i dummy chunk(s)', how_many)


    @duration_logged()
    def delete_some_dummy_chunks(self, how_many, dummy_chunk_uuids=None):
        """
        Delete some dummy chunks (from FS and from DB).
        Yields the UUIDs of dummy chunks after removing of each one.

        @param how_many: How many dummy chunks to delete.
        @type how_many: numbers.Integral

        @param dummy_chunk_uuids: (Optional) set of the dummy chunk UUIDs
                                  (if we know it already).
                                  If None, calculated automatically.
        @type dummy_chunk_uuids: (set, NoneType)
        """
        # For clarity only
        super(ChunkStorageFS, self).delete_some_dummy_chunks(how_many,
                                                             dummy_chunk_uuids)

        with self.__chunk_op_lock:
            _dummy_chunk_uuids_iter = \
                self.__get_dummy_chunk_uuids_on_fs() \
                    if dummy_chunk_uuids is None \
                    else dummy_chunk_uuids

            # list needed so it can be used three times
            removing_dummy_chunks = list(take(how_many,
                                              _dummy_chunk_uuids_iter))

            # not "how_many", but len(removing_dummy_chunks), cause
            # we may probably remove less than required.
            logger.debug('Removing %i dummy chunks',
                         len(removing_dummy_chunks))

            for dummy_chunk_uuid in removing_dummy_chunks:
                try:
                    _path = self.__get_chunk_file_path(dummy_chunk_uuid,
                                                       is_dummy=True)
                    os.unlink(_path)
                except Exception as e:
                    logger.exception('Problem during removing chunk %s: %r',
                                     dummy_chunk_uuid, e)
                yield dummy_chunk_uuid  # even if failed
                logger.debug('Deleted dummy chunk %r', dummy_chunk_uuid)
            HostQueries.HostChunks.delete_dummy_chunks(removing_dummy_chunks)


    @duration_logged()
    def update_dummy_chunks_size(self,
                                 old_limit_mib=None, new_limit_mib=None):
        """
        Whenever the situation with the chunks could have been changed,
        update the dummy chunks: create new ones if there is a lack of them,
        or remove unneeded ones if there is an excess of them.

        @param old_limit_mib: The previous total amount of chunks to keep
                              in storage, assumed 0 if None.
        @type old_limit_mib: numbers.Integral, NoneType
        @param new_limit_mib: The new total amount of chunks to keep
                              in storage, taken from the settings if None.
        @type new_limit_mib: numbers.Integral, NoneType
        """
        # For clarity only
        super(ChunkStorageFS, self).update_dummy_chunks_size(old_limit_mib,
                                                             new_limit_mib)

        # TODO! TODO! TODO! we don't bother with dummy chunks for now
        logger.warn('update_dummy_chunks_size() disabled')
        return

        if new_limit_mib is None:
            assert old_limit_mib is None
            new_limit_mib = \
                HostQueries.HostSettings.get(Queries.Settings
                                                    .MAX_STORAGE_SIZE_MIB)

        # This two variables will be used to specify the progress of the task.
        num, of = 0, 0
        _operation = '<generic operation>'


        @exceptions_logged(logger)
        def timercb():
            """
            Callback function called on timer firing.
            """
            if (num, of) != (0, 0):
                logger_status_chunks_op.info(
                    'The chunk reallocation takes too long, completed %i/%i',
                    num, of,
                    extra={'_type': 'chunks_allocation.progress',
                           'num': num,
                           'of': of})


        timer_service = TimerService(1.0, timercb)

        # If the task takes more than 3 seconds,
        # start notifying about the progress
        _callLater = reactor.callLater  # pylint:disable=E1101,C0103
        # Won't worry about deferToThread here, cause it is very fast.
        long_task_timer = _callLater(3.0, timer_service.startService)

        logger.debug('Resizing dummy chunk set from %s to %s',
                     old_limit_mib, new_limit_mib)

        with self.__chunk_op_lock:
            try:
                # Check for dummy chunks before the check for present files,
                # as the check for dummy chunks also may remove some
                # of the files.

                # What dummy chunks are available?
                # list, so it can be used twice.
                # TODO: do we need to use it twice?
                dummy_chunk_uuids = list(self.__get_dummy_chunk_uuids_on_fs())
                how_many_dummy_chunks = len(dummy_chunk_uuids)

                # What chunk files are present on the FS,...
                present_chunk_filenames_iter = \
                    self.__get_chunk_filenames_on_fs(self.__chunk_dir)
                # ... and what are the chunk UUIDs?
                # present_chunk_uuids = \
                #     self.__class__.convert_chunk_filenames_to_uuids(
                #         present_chunk_filenames)

                # How many bytes/MiB do we need to have preallocated?
                reserved_mib = long(new_limit_mib)
                reserved_size = reserved_mib * 0x100000

                # How many bytes/MiB is preallocated already?
                present_chunk_size = \
                    sum(os.stat(f).st_size
                            for f in present_chunk_filenames_iter)
                del present_chunk_filenames_iter  # help GC
                present_chunks_in_mib = \
                    round_up_to_multiply(present_chunk_size,
                                         0x100000) // 0x100000

                if reserved_mib > present_chunks_in_mib:
                    # Add new chunks
                    how_many_new_chunks = reserved_mib - present_chunks_in_mib
                    of = how_many_new_chunks
                    _operation = 'allocation'

                    for u in self.__create_some_dummy_chunks(
                                 how_many_new_chunks):
                        num += 1

                elif reserved_mib < present_chunks_in_mib:
                    # Try to remove some dummy chunks...
                    how_many_chunks_try_to_remove = \
                        present_chunks_in_mib - reserved_mib

                    # But we cannot remove more than len(dummy_chunk_uuids)!
                    if how_many_dummy_chunks < how_many_chunks_try_to_remove:
                        logger.debug('Trying to remove %i chunks, '
                                         'but only %i dummy chunks available!',
                                     how_many_chunks_try_to_remove,
                                     how_many_dummy_chunks)

                    how_many_chunks_to_remove = \
                        min(how_many_chunks_try_to_remove,
                            how_many_dummy_chunks)
                    of = how_many_chunks_to_remove
                    _operation = 'removing'

                    chunk_uuids_to_delete = take(how_many_chunks_to_remove,
                                                 dummy_chunk_uuids)

                    for u in self.delete_some_dummy_chunks(
                                 how_many_chunks_to_remove,
                                 chunk_uuids_to_delete):
                        num += 1

            except Exception as e:
                logger_status_chunks_op_error.error(
                    'The chunk %s failed: %r',
                    _operation, e,
                    extra={'_type': 'chunks_allocation.error',
                           '_exc': e,
                           '_tb': traceback.format_exc()})

            finally:
                # We've done with the chunks allocation.
                # Now stop the timer, and manually report that 100%
                # of the work is done.
                if (not long_task_timer.called and
                    not long_task_timer.cancelled):
                    long_task_timer.cancel()

                if timer_service.running:
                    timer_service.stopService()

                timercb()


    def migrate_chunks(self, old_path, new_path):
        """
        Migrate the chunks from their previous path to the new one.

        @note: Only non-dummy chunks are migrated;
               dummy chunks are removed from the old place and
               not regenerated at the new place,
               please call update_dummy_chunks_size() manually for that.
        """
        assert old_path != new_path, (old_path, new_path)

        # This two variables will be used to specify the progress of the task.
        num, of = 0, 0


        @exceptions_logged(logger)
        def timercb():
            """
            Callback function called on timer firing.
            """
            if (num, of) != (0, 0):
                logger_status_chunks_op.info(
                    'The chunk migration takes too long, completed %i/%i',
                    num, of,
                    extra={'_type': 'chunks_migration.progress',
                           'num': num,
                           'of': of})


        timer_service = TimerService(1.0, timercb)

        # If the task takes more than 3 seconds, start notifying
        # about the progress
        _callLater = reactor.callLater  # pylint:disable=E1101,C0103
        # Won't worry about deferToThread here, cause it is very fast.
        long_task_timer = _callLater(3.0, timer_service.startService)

        with self.__chunk_op_lock:
            try:
                # What chunk files are present on the FS,
                # and what are the chunk UUIDs?
                present_chunk_uuids_iter = self.__get_chunk_uuids_on_fs()

                with db.RDB() as rdbw:
                    dummy_chunks_in_db = \
                        frozenset(HostQueries.HostChunks
                                             .get_all_dummy_chunk_uuids(
                                                  rdbw=rdbw))

                # First, remove all the dummy chunks
                removed_dummy_chunks = []
                for dummy_chunk_uuid in dummy_chunks_in_db:
                    try:
                        assert self.__get_chunk_file_path(dummy_chunk_uuid,
                                                          is_dummy=True,
                                                          dir_path=old_path) \
                               == self.__get_chunk_file_path(dummy_chunk_uuid,
                                                             is_dummy=True)

                        _path = self.__get_chunk_file_path(dummy_chunk_uuid,
                                                           is_dummy=True)
                        if os.path.exists(_path):
                            os.unlink(_path)
                        # If we removed the file successfully, let's append it
                        # to the list of the chunks which are to be removed
                        # from the DB.
                        removed_dummy_chunks.append(dummy_chunk_uuid)
                    except Exception as e:
                        logger.error('Cannot remove dummy chunk %s: %s',
                                     dummy_chunk_uuid, e)
                HostQueries.HostChunks \
                           .delete_dummy_chunks(removed_dummy_chunks)

                # This dictionary maps the chunk UUID
                # to a tuple of the old filename and the new filename.
                #
                # Btw, no need to convert present_chunk_uuids_iter to set
                # and do the set difference, as it is the same complexity
                # as for ... if not in.
                uuid_to_filenames = \
                    {u: (self.__get_chunk_file_path(u,
                                                    is_dummy=False,
                                                    dir_path=old_path),
                         self.__get_chunk_file_path(u,
                                                    is_dummy=False,
                                                    dir_path=new_path))
                         for u in present_chunk_uuids_iter
                         if u not in dummy_chunks_in_db}

                # Now, move the files to the new directory.
                of = len(uuid_to_filenames)
                for u, (old_filename, new_filename) \
                        in uuid_to_filenames.iteritems():

                    logger.debug('Moving chunk %s from %s to %s',
                                 u, old_filename, new_filename)

                    try:
                        with open(old_filename, 'rb') as rfh:
                            with open_wb(new_filename) as wfh:
                                wfh.write(rfh.read())
                    except Exception:
                        logger.error('Cannot move chunk %s from %s to %s',
                                     u, old_filename, new_filename)
                    else:
                        try:
                            os.unlink(old_filename)
                        except Exception:
                            logger.error('Cannot remove chunk file %s',
                                         old_filename)

                    num += 1

            except Exception as e:
                logger_status_chunks_op_error.error(
                    'The chunks migration failed: %r',
                    e,
                    extra={'_type': 'chunks_migration.error',
                           '_exc': e,
                           '_tb': traceback.format_exc()})

            finally:
                if (not long_task_timer.called and
                    not long_task_timer.cancelled):
                    long_task_timer.cancel()

                if timer_service.running:
                    timer_service.stopService()


    def __get_total_dummy_chunks_size(self):
        """
        @return: The total size of all the dummy chunks on the FS and in DB.
        """
        return 0x100000 * ilen(self.__get_dummy_chunk_uuids_on_fs())


    def get_free_chunk_space_at_path(self, path):
        """
        Given a path on the file system, return the (best approximation of the)
        free space at the filesystem mounted at that path.

        Note that the result does not include just the "free" space at the FS,
        but also the free space which may be reclaimed by cleaning
        the dummy chunks.

        In total, the result of this function is the absolute maximum of space
        which can be used for the chunks

        @param path: The path which filesystem needs to be checked
                     for free space.

        @rtype: numbers.Integral
        """
        return get_free_space_at_path(path) + \
               self.__get_total_dummy_chunks_size()


    @duration_logged()
    def delete_chunks(self, chunk_uuids_to_delete):
        """
        Delete some (real, non-dummy!) chunks (from FS and from DB).
        Yields the UUIDs of chunks after removing of each one.

        @note: For now, you need to call update_dummy_chunks_size()
               manually after deleting the chunks.

        @param chunk_uuids_to_delete: the UUIDs of the chunks to be deleted.
        @type chunk_uuids_to_delete: col.Iterable
        """
        # For clarity only
        super(ChunkStorageFS, self).delete_chunks(chunk_uuids_to_delete)

        chunk_uuids_to_delete = list(chunk_uuids_to_delete)
        with self.__chunk_op_lock:
            logger.debug('Removing %i chunks', len(chunk_uuids_to_delete))

            # First remove all the chunks from the DB altogether,..
            with db.RDB() as rdbw:
                Queries.Chunks.delete_chunks(chunk_uuids_to_delete, rdbw)

            # ... then remove them from FS one by one.
            for chunk_uuid in chunk_uuids_to_delete:
                try:
                    os.unlink(self.__get_chunk_file_path(chunk_uuid,
                                                         is_dummy=False))
                except Exception as e:
                    logger.warning('Problem during removing chunk %s: %s',
                                   chunk_uuid, e)
                # Let's consider the chunk deleted even if an exception occured
                # during the unlink() call. Anyway there is
                # no such chunk anymore.
                yield chunk_uuid
                logger.debug('Deleted chunk %s', chunk_uuid)
