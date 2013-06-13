#!/usr/bin/python
"""CHUNKS message implementation.

CHUNK message contains a bunch of one or several chunks which are sent
from the donor Host to the acceptor Host.
"""

#
# Imports
#

from __future__ import absolute_import, division
from datetime import timedelta
import logging
import numbers
from struct import pack, unpack, calcsize
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.abstractions import AbstractMessage
from common.chunks import AbstractChunkWithContents, ChunkFromContents
from common.utils import round_up_to_multiply



#
# Constants
#

logger = logging.getLogger(__name__)

PACKING_VERSION = 0

# 1. [1 byte] PACKING_VERSION
# 2. [8 bytes] count
HEADER_FORMAT = '!BQ'

# 1. [16 bytes] UUID: self.chunk_uuid.bytes
# 2. [64 bytes] hash
# 3. [4 bytes] CRC32 (of the non-encrypted contents)
# 4. [4 bytes] padded size
# 5. (probably encrypted) contents
CHUNK_HEADER_FORMAT = '!16s64sLL'



#
# Classes
#

class ChunksMessageBody(object):
    """
    The wrapper around the chunk message body; it supports both C{__len__}
    operator which returns the total byte size of the body and C{__iter__}
    which yields the contents one chunk by one.
    """
    __slots__ = ('chunks',)


    def __init__(self, chunks):
        self.chunks = chunks


    def __len__(self):
        """
        Get total size of the message
        """
        _header_length = calcsize(HEADER_FORMAT)
        _chunk_header_length = calcsize(CHUNK_HEADER_FORMAT)

        return _header_length + sum(_chunk_header_length + c.phys_size()
                                        for c in self.chunks)


    def __iter__(self):
        # First the overall header...
        yield pack(HEADER_FORMAT,
                   PACKING_VERSION,
                   len(self.chunks))

        if __debug__:
            # We are yielding the chunks bodies,
            # but also calculating how much bytes have we yielded already,
            # to double-check it with len(self).
            _header_length = calcsize(HEADER_FORMAT)
            _chunk_header_length = calcsize(CHUNK_HEADER_FORMAT)
            total = _header_length

        # Then the chunks themselves
        for chunk in self.chunks:
            assert isinstance(chunk, AbstractChunkWithContents), repr(chunk)

            # For each chunk:
            _bytes = chunk.get_bytes()
            _size = chunk.phys_size()  # != chunk.size() due to the
                                       # encryption alignment

            # More than 4 GiB in a chunk? Crazy!
            assert _size <= 0xFFFFFFFF, repr(_size)
            yield pack(CHUNK_HEADER_FORMAT,
                       chunk.uuid.bytes,
                       str(chunk.hash),
                       chunk.crc32,
                       _size)
            yield _bytes

            if __debug__:
                total += _chunk_header_length + len(_bytes)

        if __debug__:
            # Finally, let's double-check the amount of yielded bytes.
            assert total == len(self), (total, len(self))



class ChunksMessage(AbstractMessage):
    """CHUNKS message.

    @invariant: consists_of(self.chunks, AbstractChunkWithContents)
    """

    name = 'CHUNKS'
    version = 0

    __slots__ = ('chunks',)


    def __init__(self, *args, **kwargs):
        super(ChunksMessage, self).__init__(*args, **kwargs)
        self.chunks = []


    @classmethod
    @contract_epydoc
    def _timeout_for_bytesize(cls, bytes):
        r"""Calculate the expected timeout depending on the message length.

        >>> f = ChunksMessage._timeout_for_bytesize
        >>> f(0)  # 0 bytes - 1 minute
        datetime.timedelta(0, 60)
        >>> f(1024)  # 1 kb - ~1 minute
        datetime.timedelta(0, 60, 62500)
        >>> f(1024 * 1024)  # 1 Mb ~ 2 minutes 4 seconds
        datetime.timedelta(0, 124)
        >>> f(1024 * 1024 * 63)  # 63 Mb ~ 1 hour 8 minutes 12 seconds
        datetime.timedelta(0, 4092)

        @param bytes: the length (in bytes).
        @type bytes: numbers.Integral

        @rtype: timedelta
        """
        total_size_kb = round_up_to_multiply(bytes, 1024) // 1024
        # On 128 kbit/s, the upload speed will be...
        # let's think it will be 16 kbytes/s.
        return timedelta(minutes=1) \
               + timedelta(seconds=1) * total_size_kb // 16

    @property
    def maximum_timeout(self):
        """Implement the interface from C{AbstractMessage}."""
        cls = self.__class__

        if not self.chunks:
            logger.warning('Why do we send empty CHUNKS message %r?', self)

        return cls._timeout_for_bytesize(sum(ch.phys_size()
                                                 for ch in self.chunks))


    def __str__(self):
        return '{}: {}'.format(
            super(ChunksMessage, self).__str__(),
            '{:d} chunks'.format(len(self.chunks)) if self.chunks is not None
                                                   else 'nodata')


    def _get_body(self):
        """
        @note: the chunks are binary encoded rather than JSON (as usual).

        @todo: Do we need to transport the chunk original size?
        """
        return ChunksMessageBody(self.chunks)


    @contract_epydoc
    def _init_from_body(self, body):
        """
        @type body: str

        @todo: Do we need to transport the chunk original size?
        """
        _header_length = calcsize(HEADER_FORMAT)
        _chunk_header_length = calcsize(CHUNK_HEADER_FORMAT)

        (packing_version, count), tail = unpack(HEADER_FORMAT,
                                                body[:_header_length]), \
                                         body[_header_length:]
        assert packing_version == PACKING_VERSION, \
               (packing_version, PACKING_VERSION)


        for c in xrange(count):
            (_uuid_bytes, _hash, _crc32, _size), tail = \
                unpack(CHUNK_HEADER_FORMAT, tail[:_chunk_header_length]), \
                tail[_chunk_header_length:]

            _bytes, tail = tail[:_size], tail[_size:]
            chunk = ChunkFromContents(  # Chunk-specific
                                      uuid=UUID(bytes=_uuid_bytes),
                                      hash=_hash,
                                      crc32=_crc32,
                                      size=_size,  # It may differ
                                                   # from the real one
                                        # AbstractChunkWithContents-specific
                                      # ---
                                        # ChunkFromContents-specific
                                      bytes=_bytes)

            self.chunks.append(chunk)

        assert not tail, repr(tail)
