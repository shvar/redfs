#!/usr/bin/python
"""The manager performing all the operations on the chunks stored on BigDB."""

#
# Imports
#

from __future__ import absolute_import
import logging
import numbers

import gridfs

from contrib.dbc import contract_epydoc

from common.chunk_storage import IChunkStorage
from common.chunks import (
    AbstractChunkWithContents, ChunkWithEncryptedBodyMixin, Chunk)
from common.typed_uuids import ChunkUUID

from trusted.docstore.bdbqueries import BDBQueries



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ChunkFromBigDB(ChunkWithEncryptedBodyMixin,
                     AbstractChunkWithContents):
    """
    A single data chunk, which body is stored in the BigDB.
    Note the maxsize_code is always None!

    @invariant: self.max_code is None
    """
    __slots__ = ('__gridout', '__crc32',)


    @contract_epydoc
    def __init__(self, gridout, crc32, *args, **kwargs):
        """Constructor.

        @type gridout: gridfs.grid_file.GridOut
        @type crc32: numbers.Integral
        @precondition: 0 <= crc32 <= 0xFFFFFFFF
        """
        self.__gridout = gridout
        self.__crc32 = crc32
        super(ChunkFromBigDB, self).__init__(*args, **kwargs)


    def get_bytes(self):
        """
        @returns: The byte stream for this chunk, as taken from
                  the storage file.
        @rtype: str
        """
        self.__gridout.seek(0)
        return self.__gridout.read()


    def _hash_read(self):
        return self.__gridout.hash

    hash = property(_hash_read, Chunk._hash_write)


    def phys_size(self):
        """
        The chunk size may be read once from the disk,
        but then cached in the memory.
        """
        return self.__gridout.length


    @property
    @contract_epydoc
    def crc32(self):
        """
        @rtype: numbers.Integral
        @postcondition: 0 <= result <= 0xFFFFFFFF
        """
        return self.__crc32



class ChunkStorageBigDB(IChunkStorage):
    """This class is responsible for storing the chunks on BigDB."""

    __slots__ = ('__bdbw_factory',)


    def __init__(self, bdbw_factory, *args, **kwargs):
        """Constructor."""
        self.__bdbw_factory = bdbw_factory


    def write_chunk(self, chunk, is_dummy=False):
        """Given a new chunk, write it to the storage.

        @type chunk: AbstractChunkWithContents

        @type is_dummy: bool
        """
        super(ChunkStorageBigDB, self).write_chunk(chunk, is_dummy)
        with self.__bdbw_factory() as bdbw:
            BDBQueries.Chunks.write_chunk(chunk=chunk, bdbw=bdbw)


    def read_chunk(self, chunk_uuid, exp_crc32, exp_size):
        """Given a chunk UUID, read it from the storage.

        @type chunk_uuid: UUID

        @type exp_crc32: int

        @param exp_size: expected size of the chunk.

        @rtype: AbstractChunkWithContents
        """
        super(ChunkStorageBigDB, self).read_chunk(chunk_uuid,
                                                  exp_crc32,
                                                  exp_size)

        with self.__bdbw_factory() as bdbw:
            gfile = BDBQueries.Chunks.get_chunk(chunk_uuid=chunk_uuid,
                                                bdbw=bdbw)
        assert exp_crc32 == gfile.crc32, (exp_crc32, gfile.crc32)

        return ChunkFromBigDB(  # ChunkFromBigDB-specific
                              gridout=gfile,
                              crc32=gfile.crc32,
                                # AbstractChunkWithContents-specific
                                # ---
                                # Chunk-specific
                              uuid=chunk_uuid,
                              size=exp_size)


    def delete_chunks(self, chunk_uuids_to_delete):
        """
        Delete some (real, non-dummy!) chunks (from FS and from DB).
        Yields the UUIDs of chunks after removing of each one.

        @note: For now, you need to call update_dummy_chunks_size()
               manually after deleting the chunks.

        @param chunk_uuids_to_delete: the UUIDs of the chunks to be deleted.
        @type chunk_uuids_to_delete: col.Iterable
        """
        with self.__bdbw_factory() as bdbw:
            BDBQueries.Chunks.delete_chunks(chunk_uuids=chunk_uuids_to_delete,
                                            bdbw=bdbw)


    def read_and_decrypt_chunk(self, chunk_uuid, size, crc32, cryptographer):
        """Given a chunk UUID, read it from the storage and decrypt.

        @type chunk_uuid: UUID

        @param size: expected size of the chunk (may be lower than the one
            being read due to alignment needed for the encryption).
        @precondition: size > 0

        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @type cryptographer: Cryptographer
        """
        # Implement when the trusted host becomes able to decrypt the data.
        raise NotImplementedError('Not needed')


    def get_real_stored_chunk_uuids(self):
        """Implement C{IChunkStorage} interface.

        @returns: the (possibly non-reiterable) Iterable over the chunk UUIDs
                  for the Chunks (real, non-dummy ones) which are stored
                  on this system.
        @rtype: col.Iterable
        """
        with self.__bdbw_factory() as bdbw:
            # map() rather than imap(), so that the bdbw is released early
            return map(ChunkUUID.safe_cast_uuid,
                       BDBQueries.Chunks.get_all_stored_chunk_uuids(
                           bdbw=bdbw))


    def delete_some_dummy_chunks(self, how_many, dummy_chunk_uuids=None):
        """Delete some dummy chunks (from FS and from DB).

        Yields the UUIDs of dummy chunks after removing of each one.

        @param how_many: How many dummy chunks to delete.
        @type how_many: numbers.Integral

        @param dummy_chunk_uuids: (Optional) set of the dummy chunk UUIDs
                                  (if we know it already).
                                  If None, calculated automatically.
        @type dummy_chunk_uuids: (set, NoneType)
        """
        # Not needed on Trusted Host.
        pass


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
        # Not needed on Trusted Host.
        pass
