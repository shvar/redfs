#!/usr/bin/python
"""
Advanced data operations over the data from
Relational DB, Fast DB and Big DB simultaneously.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import gc
import logging
import os
import posixpath
import stat
from collections import namedtuple
from datetime import datetime
from functools import partial
from itertools import chain
from operator import attrgetter

from contrib.dbc import contract_epydoc

from common import editions as common_editions, limits
from common.data_operations import *  # cheat
from common.chunk_storage import IChunkStorage
from common.chunks import EncryptedChunkFromFiles
from common.crypto import Cryptographer
from common.datasets import DatasetOnVirtualFiles
from common.datatypes import ProgressNotificationPerHost
from common.db import Queries, DatabaseWrapperSQLAlchemy
from common.itertools_ex import sorted_groupby
from common.os_ex import fake_stat, stat_get_mtime
from common.typed_uuids import DatasetUUID, UserGroupUUID
from common.utils import gen_uuid, NULL_UUID
from common.vfs import RelVirtualFile

from . import data_queries
from .db import TrustedQueries
from .docstore import abstract_docstorewrapper
from .docstore.fdbqueries import FDBQueries
from .docstore.bdbqueries import BDBQueries



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Functions
#

@contract_epydoc
def get_file_of_blocks_gen(blocks, cryptographer, bdbw):
    """Get the contents of a file in the cloud, in a generator way.

    @param blocks: the (possibly, non-reiterable) Iterable over the blocks
        of some specific file to be restored.
        Each item of the iterable is C{common.chunks.Chunk.Block}.
    @type blocks: col.Iterable

    @type cryptographer: Cryptographer

    @param bdbw: Big DataBase wrapper.
    @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

    @raise BDBQueries.Chunks.NoChunkException:
        if some chunk cannot be read from BigDB.

    @return: the iterable over the pieces of file, which, when concatenated,
        form the whole file.
    """
    file_size = 0
    for block in blocks:
        assert file_size == block.offset_in_file, \
               (file_size, block.offset_in_file)
        gfile = BDBQueries.Chunks.get_chunk(chunk_uuid=block.chunk.uuid,
                                            bdbw=bdbw)
        chunk_crc32 = block.chunk.crc32
        assert chunk_crc32 == gfile.crc32, \
               (chunk_crc32, gfile.crc32, block.chunk, block)
        end = block.offset_in_chunk + block.size
        decrypted = cryptographer.decrypt(gfile.read(), block.chunk.crc32)
        logger.verbose('yielding block %r with [%d:%d] to %d',
                       block, block.offset_in_chunk, end, file_size)
        yield decrypted[block.offset_in_chunk:end]
        file_size += block.size


@contract_epydoc
def download_file_of_blocks_gen(blocks, cryptographer, magnet_code,
                                fdbw, bdbw):
    """Download the file (given by its blocks) from the cloud, as a generator.

    After downloading, mark it appropriately (increment counters, etc).

    @param blocks: the (possibly, non-reiterable) Iterable over the blocks
        of some specific file to be restored.
        Each item of the iterable is C{common.chunks.Chunk.Block}.
    @type blocks: col.Iterable

    @type cryptographer: Cryptographer

    @param fdbw: FastDB wrapper.
    @type fdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

    @param bdbw: BigDB wrapper.
    @type bdbw: abstract_docstorewrapper.AbstractDocStoreWrapper

    @return: the iterable over the pieces of file, which, when concatenated,
        form the whole file.
    """
    for i in get_file_of_blocks_gen(blocks, cryptographer, bdbw):
        yield i

    FDBQueries.MagnetLinks.inc_downloads(magnet_code, fdbw)


FileSizeAndContents = namedtuple('FileSizeAndContents', ('size', 'blocks'))


def download_file(ds_uuid, base_dir, rel_path, edition, rdbw):
    """Get the information for file streaming.

    @param ds_uuid: dataset UUID

    @param base_dir: base directory of the file

    @param rel_path: relative path of the file

    @type edition: the edition where the procedure is executed
        (affects encryption).

    @type rdbw: DatabaseWrapperSQLAlchemy

    @return: a tuple with C{size} (expected file size) and the iterator over
        file blocks (an Iterable over C{common.chunks.Chunk.Block} objects).
        If not available, C{None} is returned.
    @rtype: FileSizeAndContents, NoneType
    """
    bd_mapping = {base_dir: [rel_path]}
    file_map, block_map = TrustedQueries.TrustedBlocks.get_blocks_for_files(
                              NULL_UUID, ds_uuid, bd_mapping, rdbw)

    # Here can be KeyError exception if there are no valid files.
    try:
        logger.debug('File %r, dataset %s, %d block(s), expected size: %d',
                     rel_path, ds_uuid,
                     len(block_map[rel_path]), file_map[rel_path].size)
    except KeyError:
        return None
    else:
        blocks_iter = find_file_restore_solution(block_map[rel_path],
                                                 file_map[rel_path])
        return FileSizeAndContents(size=file_map[rel_path].size,
                                   blocks=blocks_iter)


def get_cryptographer(ds_uuid, edition, rdbw):
    """Get a cryptographer for some particular dataset.

    @type ds_uuid: UUID

    @rtype: Cryptographer
    """
    feature_set = common_editions.EDITIONS[edition]

    if feature_set.per_group_encryption:
        # Read group key from the user group
        group_key = Queries.Datasets.get_enc_key_for_dataset(ds_uuid, rdbw)
    else:
        group_key = None

    return Cryptographer(group_key=group_key, key_generator=None)



class NoSolutionException(Exception):

    def __init__(self, bad_file, *args, **kwargs):
        self.__bad_file = bad_file


    def __str__(self):
        return u'No solution for {!r}'.format(self.__bad_file)



@contract_epydoc
def find_file_restore_solution(blocks, target_file):
    r"""
    Find a solution that definitely restores the requested file from a given
    sequence of blocks, or raises an exception if such solution does not exist.

    >>> # Prepare variables for tests
    >>> from common import chunks
    >>> from common.crypto import Fingerprint
    >>> from common.test.utils import gen_uuid
    >>> gen_uuid('find_file_restore_solution')
    UUID('deebaaf6-1702-ca23-ed0b-a6a8b6f41b5a')
    >>> gen_uuid('find_file_restore_solution')
    UUID('09ced4c0-17c4-002b-2c3f-74cb5d695999')

    >>> fp6 = Fingerprint(size=6, hash=0L)
    >>> file6 = chunks.File(fingerprint=fp6, crc32=None)
    >>> dummychunk = chunks.ChunkInfo(crc32=0, uuid=gen_uuid('solution'),
    ...                               hash=None, size=4*1024*1024)
    >>> Block = chunks.Chunk.Block

    >>> # Test just the blocks for a dummy file
    >>> _test = lambda blocks, f: list(find_file_restore_solution(blocks, f))

    >>> #
    >>> # Real tests
    >>> #

    >>> # Test 1: simple solution
    >>> _test([
    ...     Block(offset_in_file=0, size=3,
    ...           offset_in_chunk=0, chunk=dummychunk, file=file6),
    ...     Block(offset_in_file=0, size=2,
    ...           offset_in_chunk=0, chunk=dummychunk, file=file6),
    ...     Block(offset_in_file=2, size=2,
    ...           offset_in_chunk=0, chunk=dummychunk, file=file6),
    ...     Block(offset_in_file=4, size=2,
    ...           offset_in_chunk=0, chunk=dummychunk, file=file6),
    ...     Block(offset_in_file=3, size=3,
    ...           offset_in_chunk=0, chunk=dummychunk, file=file6),
    ... ], file6)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
    [<Block: File(000000...000000: 6 bytes), #0, 3 bytes,
             chunk 9aee4540-bac4-3d87-2404-2383ff90bfff>,
     <Block: File(000000...000000: 6 bytes), #0, 3 bytes,
             chunk 9aee4540-bac4-3d87-2404-2383ff90bfff>]

    >>> _test([
    ...     Block(offset_in_file=0, size=2,
    ...           offset_in_chunk=0, chunk=dummychunk, file=file6),
    ...     Block(offset_in_file=4, size=2,
    ...           offset_in_chunk=0, chunk=dummychunk, file=file6),
    ... ], file6)  # doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
      ...
    NoSolutionException: No solution for File(000000...000000: 6 bytes)

    >>> # _test([
    >>> #     Block(offset_in_file=0, size=2),
    >>> #     Block(offset_in_file=4, size=2),
    >>> #     Block(offset_in_file=0, size=3),
    >>> #     Block(offset_in_file=3, size=3)
    >>> # ], file6)  # raises Exception for now, but must be fixed

    @param blocks: the (possibly, non-reiterable) Iterable over the blocks
        of some specific file to be restored.
        Each item of the iterable is C{common.chunks.Chunk.Block}.
    @type blocks: col.Iterable

    @returns: a (possibly non-reiterable) iterable of C{Chunk.Block}-s
        which should be used to create the file.
    @rtype: col.Iterable

    @raise NoSolutionException: if the solution does not exist.
    """
    # TODO: Major: To make smart!
    blocks_by_offset = {}
    offset_in_file_counter = blocks_counter = 0

    for block in blocks:
        if block.size > 0:
            blocks_by_offset.setdefault(block.offset_in_file, []).append(block)
        else:
            logger.error('find_file_restore_solution: block %r has zero size!',
                         block)

    logger.debug('blocks_by_offset for %r are %r',
                 target_file, blocks_by_offset)

    while offset_in_file_counter < target_file.size:
        try:
            block_to_yield = blocks_by_offset[offset_in_file_counter][0]
        except KeyError:
            logger.warning('Broken solution for %r! Failure at %d'
                               '(size: %d), target file size: %d',
                           target_file, blocks_counter,
                           offset_in_file_counter,
                           target_file.size)
            raise NoSolutionException(target_file)
        else:
            logger.verbose('Solution for %r so far so good: '
                               'block %r with size %d added to %d',
                           target_file, block_to_yield,
                           block_to_yield.size, offset_in_file_counter)
            offset_in_file_counter += block_to_yield.size
            blocks_counter += 1
            yield block_to_yield

    # Job is done, returning blocks for requested file
    if offset_in_file_counter == target_file.size:
        logger.debug('Successful solution for %r in %d blocks',
                     target_file, blocks_counter)
        return
    else:
        # Ooops, filesize mismatch!
        logger.warning('Broken solution for %r! Failure at %d (size: %d), '
                           'target file size: %d',
                       target_file, blocks_counter,
                       offset_in_file_counter,
                       target_file.size)
        raise NoSolutionException(target_file)


FileToUpload = namedtuple('FileToUpload',
                          ('base_dir', 'rel_path', 'size', 'file_getter'))
"""A record about a file being uploaded.

It is a definite file, not a directory!

If C{size} field is C{None}, the file is considered deleted
(and C{file_getter} field is unneeded then).
"""

FileStateAndFakeStat = namedtuple('FileStateAndFakeStat',
                                  ('file_state', 'stat'))


@contract_epydoc
def upload_delete_files(base_dir, files, ds_name, group_uuid, sync,
                        cryptographer, rdbw, chunk_storage, ospath=posixpath):
    """Upload several files to the cloud.

    Note: these should be indeed the uploaded files, or deleted files,
    but not the deleted files.

    @note: all the paths should be in POSIX format.

    @param base_dir: the base directory path (in the dataset) where to upload
        the file.
    @type base_dir: basestring

    @param files: the iterable of files which should be uploaded.
        Contains instances of C{FileToUpload}.
    @type files: col.Iterable

    @param group_uuid: the UUID of the user group, for which the file
        should be bound.
    @type group_uuid: UserGroupUUID

    @param sync: whether the created dataset should be considered a
        "sync dataset".
    @type sync: bool

    @param rdbw: RelDB access wrapper.
    @type rdbw: DatabaseWrapperSQLAlchemy

    @param chunk_storage: the chunk storage object.
    @type chunk_storage: IChunkStorage

    @return: the UUID of newly created dataset.
    @rtype: DatasetUUID
    """
    upload_time = datetime.utcnow()

    # For each FileToUpload, create fake stat for the files,
    # either uploaded or deleted.
    _files_to_upload_with_stat = \
        ((ftu, fake_stat(atime=upload_time,
                         mtime=upload_time,
                         ctime=upload_time,
                         # Is deleted? In FTU, "deleted" file is marked
                         # via .size=None, but in stat - via st_mode=None.
                         st_mode=0777 if ftu.size is not None else None,
                         size=ftu.size))
             for ftu in files)  # isinstance(ftu, FileToUpload)

    # Turn the original FileToUpload's to RelVirtualFile's
    _vfiles = (RelVirtualFile(rel_dir=ospath.dirname(ftu.rel_path),
                              filename=ospath.basename(ftu.rel_path),
                              stat=fstat,
                              stat_getter=lambda fstat=fstat: fstat,
                              file_getter=ftu.file_getter)
                   for ftu, fstat in _files_to_upload_with_stat)
    # consists_of(_vfiles, RelVirtualFile)

    # Group RelVirtualFile's by rel_dir
    _files_grouped_by_rel_dir = \
        ((rvf for rvf in per_rel_dir)
             for rel_dir, per_rel_dir
                 in sorted_groupby(_vfiles, attrgetter('rel_dir')))

    paths_map = {base_dir: {'ifiles': _files_grouped_by_rel_dir,
                            'stat': fake_stat(isdir=True,
                                              atime=upload_time,
                                              mtime=upload_time,
                                              ctime=upload_time)}}

    ds_uuid = DatasetUUID.safe_cast_uuid(gen_uuid())
    dataset = DatasetOnVirtualFiles.from_paths(
                  ds_name, ds_uuid, group_uuid, sync, paths_map, upload_time,
                  cryptographer)

    assert dataset is not None

    thosts = list(TrustedQueries.HostAtNode.get_all_trusted_hosts(
                      for_storage=True,
                      rdbw=rdbw))

    logger.debug('Uploading dataset %r,... like, to %r',
                 dataset, thosts)

    # Use group_uuid as host_uuid
    fake_host_uuid = None

    dummy_ds_uuid = Queries.Datasets.create_dataset_for_backup(
                        fake_host_uuid, dataset, rdbw)

    # Commit this session to get latest files in web
    # with status "Processing" when they are encrypting.
    rdbw.commit()

    dataset.time_completed = datetime.utcnow()

    # We have the original chunks. Let's encrypt them.
    encrypted_chunks = \
        [EncryptedChunkFromFiles.from_non_encrypted(cryptographer,
                                                    non_encr_ch)
             for non_encr_ch in dataset.chunks()]

    # Precalculate the chunk hashes.
    total_chunks_bytesize = sum(chunk.size()
                                    for chunk in encrypted_chunks)
    calculated_hashes = 0

    for chunk in encrypted_chunks:
        assert isinstance(chunk, EncryptedChunkFromFiles), \
               repr(chunk)

        # dummy = chunk.get_bytes()
        dummy = chunk.hash

        calculated_hashes += chunk.size()
        logger.verbose('Hashes calculated for %i byte(s) of %i.',
                       calculated_hashes, total_chunks_bytesize)
        # GC collection is forced, because otherwise,
        # after the massive chunks rereading,
        # the memory is clogged very soon.
        gc.collect()

    # Do we already have the duplicate chunks for our chunks?
    # If yes, we must use their UUIDs.
    duplicates = TrustedQueries.TrustedChunks.save_chunks_and_get_duplicates(
                     encrypted_chunks, rdbw) if encrypted_chunks else {}
    logger.debug('Duplicates for the file: %d', len(duplicates))
    for k, v in duplicates.iteritems():
        logger.verbose('  chunk %s -> %s', k, v)

    # Also, if we already have the duplicates, we can find if all of them
    # are indeed present.
    if duplicates:
        _hosts_for_chunks = \
            TrustedQueries.TrustedChunks.get_host_uuids_for_chunks(
                duplicates.itervalues(), rdbw)
        if _hosts_for_chunks:
            logger.verbose('For this file, some chunks are already present:')
            for h in _hosts_for_chunks.iterkeys():
                _chunks_on_host = _hosts_for_chunks[h]
                logger.verbose('  At host %s (%d chunks):',
                               h, len(_chunks_on_host))
                for ch in _chunks_on_host:
                    logger.verbose('    chunk %s', ch)

        # The chunks that are already present on some hosts.
        already_available_chunk_uuids = \
            set(chain.from_iterable(_hosts_for_chunks.itervalues()))
        logger.debug('Really present chunks: %d',
                     len(already_available_chunk_uuids))
        for cu in already_available_chunk_uuids:
            logger.verbose('  chunk %s', cu)

        # _hosts_for_chunks not needed anymore
        del _hosts_for_chunks  # help GC
    else:
        already_available_chunk_uuids = set()

    #
    # Duplicate fixes: phase 1
    #

    # Fix the duplicates which match the existing chunks.
    for chunk in encrypted_chunks:
        if chunk.uuid in duplicates:
            old_uuid = chunk.uuid
            new_uuid = duplicates[chunk.uuid]
            # Fix the UUID of the chunk...
            logger.verbose('Fixing the DB duplication for chunk uuid: '
                               '%s to %s',
                           old_uuid, new_uuid)
            chunk.uuid = new_uuid
            # ... and fix the UUIDs of the chunk referred from the blocks
            for bl in chunk.blocks:
                # I wonder, is "bl.chunk" the same as the "chunk"?
                # If it is, it's already modified;
                # if it is a copy, it still needs to be modified.
                if bl.chunk.uuid != new_uuid:
                    assert bl.chunk.uuid == old_uuid, \
                           (old_uuid, new_uuid, bl, bl.chunk.uuid)
                    bl.chunk.uuid = new_uuid

    #
    # Duplicate fixes: phase 2
    #

    # But now let's check if we have duplicates within the stream itself...
    inner_duplicates = {(c.hash, c.size()): c.uuid
                            for c in encrypted_chunks}
    # Second phase of fixing the duplicates
    for chunk in encrypted_chunks:
        old_uuid = chunk.uuid
        new_uuid = inner_duplicates[(chunk.hash, chunk.size())]
        if new_uuid != old_uuid:
            # Fix the UUID of the chunk...
            logger.verbose('Fixing the inner duplication for chunk uuid: '
                               '%s to %s',
                           old_uuid, new_uuid)
            chunk.uuid = new_uuid
            # ... and fix the UUIDs of the chunk referred from the blocks
            for bl in chunk.blocks:
                # I wonder, is "bl.chunk" the same as the "chunk"?
                # If it is, it's already modified;
                # if it is a copy, it still needs to be modified.
                if bl.chunk.uuid != new_uuid:
                    assert bl.chunk.uuid == old_uuid, \
                           (old_uuid, new_uuid, bl, bl.chunk.uuid)
                    bl.chunk.uuid = new_uuid

    #
    # Duplicate fixes: done
    #

    # Finally, add the chunks to the DB
    if encrypted_chunks:
        logger.debug('Now, writing %i (%i unique) chunks to DB',
                     len(encrypted_chunks), len(inner_duplicates))
        Queries.Chunks.add_chunks(encrypted_chunks, rdbw)
    else:
        logger.debug('No chunks to add')

    logger.debug('Kind-of-upload the chunks to the trusted hosts: %r', thosts)

    just_uploaded_chunks = set()

    for chunk in encrypted_chunks:
        if chunk.uuid in already_available_chunk_uuids:
            logger.debug('Ignoring %r, already present', chunk)
        else:
            already_available_chunk_uuids.add(chunk.uuid)
            just_uploaded_chunks.add(chunk)
            logger.debug('Kind-of-uploading %r to storage', chunk)
            chunk_storage.write_chunk(chunk, is_dummy=False)

        # GC collection is forced, because otherwise,
        # after the massive chunks rereading,
        # the memory is clogged very soon.
        gc.collect()

    blocks_map = [(block, chunk)
                      for chunk in encrypted_chunks
                      for block in chunk.blocks]

    logger.debug('Binding blocks...')

    Queries.Blocks.bind_blocks_to_files(fake_host_uuid, ds_uuid,
                                        blocks_map, rdbw)

    # Now let's mark that these chunks are uploaded to all (available)
    # Trusted Hosts.
    if not just_uploaded_chunks:
        logger.debug('Actually, no new chunks were uploaded to storage')
    else:
        notifications = [ProgressNotificationPerHost(
                             chunks=just_uploaded_chunks)]

        # just_uploaded_chunks can be too large, let's log just their number
        logger.debug('Kind-of-uploading %i chunk(s)',
                     len(just_uploaded_chunks))

        for thost in thosts:
            logger.debug('Kind-of-upload the chunks to the trusted host: %r',
                          thost)
            TrustedQueries.TrustedChunks.chunks_are_uploaded_to_the_host(
                fake_host_uuid, thost.uuid, notifications, rdbw)

    # That's all, folks!
    Queries.Datasets.update_dataset(fake_host_uuid, dataset, rdbw)

    return ds_uuid


@contract_epydoc
def create_directory(base_dir, rel_path, ds_name, group_uuid, sync,
                     cryptographer, rdbw, ospath=posixpath):
    """Create new directory in the cloud.

    @note: all the paths should be in POSIX format.

    @param base_dir: the base directory path (in the dataset) where to upload
        the file.
    @type base_dir: basestring

    @param rel_path: the name of directory which should be created.
    @type rel_path: basestring

    @param group_uuid: the UUID of the user group, for which the file
        should be bound.
    @type group_uuid: UserGroupUUID

    @param sync: whether the created dataset should be considered a
        "sync dataset".
    @type sync: bool

    @param rdbw: RelDB wrapper.
    @type rdbw: DatabaseWrapperSQLAlchemy

    @return: the UUID of newly created dataset.
    @rtype: DatasetUUID
    """
    upload_time = datetime.utcnow()

    # For each FileToUpload, create fake stat
    dir_fake_stat = fake_stat(isdir=True,
                              atime=upload_time,
                              mtime=upload_time,
                              ctime=upload_time,
                              size=None)

    # Turn the original FileToUpload's to RelVirtualFile's
    _vfile = RelVirtualFile(rel_dir=ospath.dirname(rel_path),
                            filename=ospath.basename(rel_path),
                            stat=dir_fake_stat,
                            stat_getter=lambda dir_fake_stat=dir_fake_stat:
                                            dir_fake_stat)
    # isinstance(ftu, FileToUpload)

    # Group RelVirtualFile's by rel_dir
    # _files_grouped_by_rel_dir = \
    #     ((rvf for rvf in per_rel_dir)
    #          for rel_dir, per_rel_dir
    #          in sorted_groupby(_vfiles, attrgetter('rel_dir')))

    paths_map = {base_dir: {'ifiles': [[_vfile]],
                            'stat': fake_stat(isdir=True,
                                              atime=upload_time,
                                              mtime=upload_time,
                                              ctime=upload_time)}}

    ds_uuid = DatasetUUID.safe_cast_uuid(gen_uuid())
    dataset = DatasetOnVirtualFiles.from_paths(
                  ds_name, ds_uuid, group_uuid, sync, paths_map, upload_time,
                  cryptographer)

    assert dataset is not None

    # Use group_uuid as host_uuid
    fake_host_uuid = None

    dummy_ds_uuid = Queries.Datasets.create_dataset_for_backup(
                        fake_host_uuid, dataset, rdbw)

    dataset.time_completed = datetime.utcnow()

    # That's all, folks!
    Queries.Datasets.update_dataset(fake_host_uuid, dataset, rdbw)

    return ds_uuid


PerGroupSpaceInfo = namedtuple('PerGroupSpaceInfo',
                               ('group_uuid', 'group_name', 'max_size',
                                'file_hold_size', 'file_uniq_size',
                                'available', 'avg_replica', 'used_percents'))

GroupSpaceInfoOverall = namedtuple('GroupSpaceInfoOverall',
                                   ('max_size', 'used_size', 'available'))

GetGroupSpaceInfoResult = namedtuple('GetGroupSpaceInfoResult',
                                     ('per_group', 'overall'))


def get_group_space_info(rdbw):
    """
    Get the information about the space in the cloud utilized by every group.

    @param rdbw: RelDB wrapper.
    @type rdbw: DatabaseWrapperSQLAlchemy

    @rtype: GetGroupSpaceInfoResult
    """
    per_groupspace_stat = \
        TrustedQueries.SpaceStat.get_per_group_space_stat(rdbw)

    pg_info = []  # per_group_info
    max_size_total, used_size_total = 0, 0

    for pg_stat in per_groupspace_stat:
        _max, _used, _uniq = pg_stat.max_size, \
                             pg_stat.file_hold_size,\
                             pg_stat.file_uniq_size
        max_size_total += _max
        used_size_total += _used

        res_row = PerGroupSpaceInfo(group_uuid=pg_stat.group_uuid,
                                    group_name=pg_stat.group_name,
                                    max_size=_max,
                                    file_hold_size=_used,
                                    file_uniq_size=_uniq,
                                    available=_max - _used,
                                    avg_replica=_used / _uniq if _uniq > 0
                                                              else 0,
                                    used_percents=_used / _max * 100
                                                      if _max > 0
                                                      else 0)
        pg_info.append(res_row)

    overall_info = GroupSpaceInfoOverall(
                       max_size=max_size_total,
                       used_size=used_size_total,
                       available=max_size_total - used_size_total)

    return GetGroupSpaceInfoResult(per_group=pg_info,
                                   overall=overall_info)
