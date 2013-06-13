#!/usr/bin/python
"""
ChunkStorage interfaces.
"""

#
# Imports
#

from __future__ import absolute_import
import numbers
from abc import ABCMeta, abstractmethod



#
# Classes
#

class IChunkStorage(object):
    """
    The interface which every ChunkStorage implementation must satisfy.
    """

    __metaclass__ = ABCMeta

    __slots__ = ()


    @abstractmethod
    def write_chunk(self, chunk, is_dummy):
        """Given a new chunk, write it to the storage.

        @type chunk: AbstractChunkWithContents

        @type is_dummy: bool
        """
        pass


    @abstractmethod
    def read_chunk(self, chunk_uuid, exp_crc32, exp_size):
        """Given a chunk UUID, read it from the storage.

        @type chunk_uuid: UUID

        @type exp_crc32: int

        @param exp_size: expected size of the chunk.
        """
        pass


    @abstractmethod
    def delete_chunks(self, chunk_uuids_to_delete):
        """
        Delete some (real, non-dummy!) chunks (from FS and from DB).
        Yields the UUIDs of chunks after removing of each one.

        @note: For now, you need to call update_dummy_chunks_size()
               manually after deleting the chunks.

        @param chunk_uuids_to_delete: the UUIDs of the chunks to be deleted.
        @type chunk_uuids_to_delete: col.Iterable
        """
        pass


    @abstractmethod
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
        pass


    @abstractmethod
    def get_real_stored_chunk_uuids(self):
        """Get the UUIDs of (non-dummy) chunks actually stored in the storage.

        @returns: the (possibly non-reiterable) Iterable over the chunk UUIDs
            for the Chunks (real, non-dummy ones) which are stored
            on this system.
        @rtype: col.Iterable
        """
        pass


    @abstractmethod
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
        pass


    @abstractmethod
    def update_dummy_chunks_size(self,
                                 old_limit_mib, new_limit_mib):
        """
        Whenever the situation with the chunks could have been changed,
        update the dummy chunks: create new ones if there is a lack of them,
        or remove unneeded ones if there is an excess of them.

        @param old_limit_mib: The previous total amount of chunks to keep
                              in storage, assumed 0 if None.
        @type old_limit_mib: numbers.Integral, NoneType
        @param new_limit_mib: The new total amount of chunks to keep
                              in storage, taken from the settings if None.
        @type new_limit_mib: (numbers.Integral, NoneType)
        """
        pass
