#!/usr/bin/python
"""The models of various useful types."""

#
# Imports
#

from __future__ import absolute_import
import numbers
from functools import partial
from types import NoneType
from uuid import UUID

from bson.binary import Binary

from contrib.dbc import contract_epydoc

from common import chunks as common_chunks

from trusted.ibsonable import IBSONable

from .idocstoredocument import IDocStoreAnyDocument



#
# Classes
#

class ChunkInfo(IDocStoreAnyDocument, IBSONable, common_chunks.ChunkInfo):
    """The information about some chunk."""

    # Can other arguments (besides maxsize_code) be optional too?
    bson_schema = {
        'maxsize_code': (int, NoneType),  # optional
        'uuid': UUID,
        'hash': lambda s: isinstance(s, str) and len(s) == 64,
        'size': int,
        'crc32': numbers.Integral
    }

    def __init__(self, *args, **kwargs):
        r"""Constructor.

        >>> # No optional arguments
        >>> ChunkInfo(uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...           hash='\x00\x01\x02\x03\xfc\xfd\xfe\xff' * 8,
        ...           size=2097152,
        ...           crc32=0x07FD7A5B
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        models.types.ChunkInfo(uuid=ChunkUUID('030f417b-...-847e7ab886a2'),
            hash=unhexlify('00010203fcfdfeff00010203fc...eff00010203fcfdfeff'),
            size=2097152,
            crc32=0x07FD7A5B)

        >>> # With optional arguments
        >>> ChunkInfo(maxsize_code=1,
        ...           uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...           hash='\x00\x01\x02\x03\xfc\xfd\xfe\xff' * 8,
        ...           size=2097152,
        ...           crc32=0x07FD7A5B
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        models.types.ChunkInfo(uuid=ChunkUUID('030f417b-...-847e7ab886a2'),
            maxsize_code=1,
            hash=unhexlify('00010203fcfdfeff00010203fc...eff00010203fcfdfeff'),
            size=2097152,
            crc32=0x07FD7A5B)
        """
        super(ChunkInfo, self).__init__(*args, **kwargs)

        assert self.is_valid_bsonable(), repr(self)


    def __repr__(self):
        r"""Hacked version of C{IPrintable.__repr__()}.

        >>> ChunkInfo(maxsize_code=1,
        ...           uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...           hash='abcdefgh' * 8,
        ...           size=2097152,
        ...           crc32=0x07FD7A5B
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        models.types.ChunkInfo(uuid=ChunkUUID('030f417b-...-847e7ab886a2'),
            maxsize_code=1,
            hash=unhexlify('61626364656667686162...4656667686162636465666768'),
            size=2097152,
            crc32=0x07FD7A5B)
        """
        return u'models.types.{}({})'.format(self.__class__.__name__, self)


    def __str__(self):
        """Force using an C{common_chunks.ChunkInfo.__str__}."""
        return common_chunks.ChunkInfo.__str__(self)


    def to_bson(self):
        r"""
        >>> # No optional arguments
        >>> ChunkInfo(uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...           hash='\x00\x01\x02\x03\xfc\xfd\xfe\xff' * 8,
        ...           size=2097152,
        ...           crc32=0x07FD7A5B
        ... ).to_bson()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'crc32': 134052443,
         'hash': Binary('\x00\x01\x02\x03\xfc\xfd\xfe\xff...\xfd\xfe\xff', 0),
         'uuid': ChunkUUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
         'size': 2097152}

        >>> # All optional arguments
        >>> ChunkInfo(maxsize_code=1,
        ...           uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...           hash='\x00\x01\x02\x03\xfc\xfd\xfe\xff' * 8,
        ...           size=2097152,
        ...           crc32=0x07FD7A5B
        ... ).to_bson()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'crc32': 134052443,
         'maxsize_code': 1,
         'hash': Binary('\x00\x01\x02\x03\xfc\xfd\xfe\xff...\xfd\xfe\xff', 0),
         'uuid': ChunkUUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
         'size': 2097152}
        """
        cls = self.__class__

        doc = super(ChunkInfo, self).to_bson()

        # Mandatory
        doc.update({
            'uuid': self.uuid,
            'hash': Binary(self.hash),
            'size': self.size(),
            'crc32': self.crc32,
        })
        # Optional
        if self.maxsize_code is not None:
            doc.update({'maxsize_code': self.maxsize_code})

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> # No optional arguments
        >>> ChunkInfo.from_bson({
        ...     'crc32': 134052443,
        ...     'hash': Binary('\x00\x01\x02\x03\xfc\xfd\xfe\xff' * 8),
        ...     'uuid': UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...     'size': 2097152
        ... })()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        models.types.ChunkInfo(uuid=ChunkUUID('030f417b-...-847e7ab886a2'),
            hash=unhexlify('00010203fcfdfeff0001...3fcfdfeff00010203fcfdfeff'),
            size=2097152, crc32=0x07FD7A5B)

        >>> # All optional arguments
        >>> ChunkInfo.from_bson({
        ...     'crc32': 134052443,
        ...     'maxsize_code': 1,
        ...     'hash': Binary('\x00\x01\x02\x03\xfc\xfd\xfe\xff' * 8),
        ...     'uuid': UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...     'size': 2097152
        ... })()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        models.types.ChunkInfo(uuid=ChunkUUID('030f417b-...-847e7ab886a2'),
            maxsize_code=1,
            hash=unhexlify('00010203fcfdfeff0001...3fcfdfeff00010203fcfdfeff'),
            size=2097152, crc32=0x07FD7A5B)
        """
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(ChunkInfo, cls).from_bson(doc),
                       maxsize_code=doc.get('maxsize_code'),
                       uuid=doc['uuid'],
                       hash=doc['hash'],
                       size=doc['size'],
                       crc32=int(doc['crc32']))


    @classmethod
    @contract_epydoc
    def from_common(cls, common_chunkinfo):
        r"""
        Create an C{ChunkInfo} model from the more common implementation,
        C{common.chunks.ChunkInfo}.

        >>> common_chunkinfo = \
        ...     common_chunks.ChunkInfo(
        ...         crc32=0x07FD7A5B,
        ...         uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'),
        ...         maxsize_code=1,
        ...         hash='abcdefgh' * 8,
        ...         size=2097152)
        >>> ChunkInfo.from_common(
        ...     common_chunkinfo
        ... ) # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        models.types.ChunkInfo(uuid=ChunkUUID('030f417b-...-847e7ab886a2'),
            maxsize_code=1,
            hash=unhexlify('61626364656667...1626364656667686162636465666768'),
            size=2097152,
            crc32=0x07FD7A5B)

        @type common_chunkinfo: common_chunks.ChunkInfo

        @rtype: ChunkInfo
        """
        return cls(maxsize_code=common_chunkinfo.maxsize_code,
                   uuid=common_chunkinfo.uuid,
                   hash=common_chunkinfo.hash,
                   size=common_chunkinfo.size(),
                   crc32=common_chunkinfo.crc32)
