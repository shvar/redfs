#!/usr/bin/python
"""
RECEIVE_CHUNKS transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from functools import partial
from types import NoneType

from contrib.dbc import consists_of, contract_epydoc

from common.chunks import Chunk
from common.utils import coalesce
from common.typed_uuids import PeerUUID

from ..types import ChunkInfo as model_ChunkInfo

from ._common import NodeTransactionState



#
# Classes
#

class ReceiveChunksTransactionState_Node(NodeTransactionState):
    """The state for the RECEIVE_CHUNKS transaction on the Node.

    @type chunks_to_replicate: dict
    @type chunks_to_restore: dict
    """

    __slots__ = ('chunks_to_replicate', 'chunks_to_restore')

    name = 'RECEIVE_CHUNKS'
    bson_schema = {
        'chunks_to_replicate': (dict, NoneType),
        'chunks_to_restore': (dict, NoneType),
    }


    @contract_epydoc
    def __init__(self,
                 chunks_to_replicate=None, chunks_to_restore=None,
                 *args, **kwargs):
        r"""Constructor.

        Either of the two fields, C{chunks_to_replicate}
        or C{chunks_to_restore}, is likely created.

        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> from common.chunks import ChunkInfo
        >>> from common.typed_uuids import ChunkUUID

        >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
        >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
        >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
        >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        >>> u1, u2, u3, u4 = \
        ...     (ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...      ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...      ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...      ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'))
        >>> host_uuid_1, host_uuid_2 = \
        ...     (PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
        ...      PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'))

        >>> # No optional arguments.
        >>> ReceiveChunksTransactionState_Node(
        ...     tr_start_time=tr_start_time,
        ...     tr_uuid=tr_uuid,
        ...     tr_src_uuid=tr_src_uuid,
        ...     tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ReceiveCh...State_Node(tr_start_time=datetime.datetime(2012, 9, 26, 14,
                                                               29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))

        >>> # All optional arguments.
        >>> ReceiveChunksTransactionState_Node(
        ...     chunks_to_replicate={
        ...         host_uuid_1:
        ...             [ChunkInfo(crc32=0x2A5FE875, uuid=u4,
        ...                        maxsize_code=1,
        ...                        hash='abcdabcd' * 8, size=73819)],
        ...         host_uuid_2:
        ...             [ChunkInfo(crc32=0x07FD7A5B, uuid=u1,
        ...                        maxsize_code=1,
        ...                        hash='abcdefgh' * 8, size=2097152),
        ...              ChunkInfo(crc32=0x7E5CE7AD, uuid=u2,
        ...                        maxsize_code=0,
        ...                        hash='01234567' * 8, size=143941),
        ...              ChunkInfo(crc32=0xDCC847D8, uuid=u3,
        ...                        maxsize_code=1,
        ...                        hash='76543210' * 8, size=2097151)]
        ...     },
        ...     chunks_to_restore={
        ...         host_uuid_1:
        ...             [ChunkInfo(crc32=0x07FD7A5B, uuid=u1,
        ...                        maxsize_code=1,
        ...                        hash='abcdefgh' * 8, size=2097152),
        ...              ChunkInfo(crc32=0x7E5CE7AD, uuid=u2,
        ...                        maxsize_code=0,
        ...                        hash='01234567' * 8, size=143941),
        ...              ChunkInfo(crc32=0xDCC847D8, uuid=u3,
        ...                        maxsize_code=1,
        ...                        hash='76543210' * 8, size=2097151)],
        ...         host_uuid_2:
        ...             [ChunkInfo(crc32=0x2A5FE875, uuid=u1,
        ...                        maxsize_code=1,
        ...                        hash='abcdabcd' * 8, size=73819)]
        ...     },
        ...     tr_start_time=tr_start_time,
        ...     tr_uuid=tr_uuid,
        ...     tr_src_uuid=tr_src_uuid,
        ...     tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ReceiveCh...State_Node(tr_start_time=datetime.datetime(2012, 9, 26, 14,
                                                               29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            chunks_to_replicate={PeerUUID('e96a073b-...fb04c221a9c'):
                    [ChunkInfo(uuid=ChunkUUID('5b237ceb-300d...-6331cb14b5b4'),
                               maxsize_code=1,
                               hash=unhexlify('6162636465...6162636465666768'),
                               size=2097152, crc32=0x07FD7A5B),
                     ChunkInfo(uuid=ChunkUUID('940f0711-52d7...-818580f432dc'),
                               maxsize_code=0,
                               hash=unhexlify('3031323334...3031323334353637'),
                               size=143941, crc32=0x7E5CE7AD),
                     ChunkInfo(uuid=ChunkUUID('a5b605f2-6ea5...-d217b7e8e784'),
                               maxsize_code=1,
                               hash=unhexlify('3736353433...3736353433323130'),
                               size=2097151, crc32=0xDCC847D8)],
                PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'):
                    [ChunkInfo(uuid=ChunkUUID('0a7064b3-bef6...-e9f9a40dfcf3'),
                               maxsize_code=1,
                               hash=unhexlify('6162636461...6162636461626364'),
                               size=73819, crc32=0x2A5FE875)]},
            chunks_to_restore={PeerUUID('e96a073b-...fb04c221a9c'):
                    [ChunkInfo(uuid=ChunkUUID('5b237ceb-300d...-6331cb14b5b4'),
                               maxsize_code=1,
                               hash=unhexlify('6162636461...6162636461626364'),
                               size=73819, crc32=0x2A5FE875)],
                PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'):
                    [ChunkInfo(uuid=ChunkUUID('5b237ceb-300d...-6331cb14b5b4'),
                               maxsize_code=1,
                               hash=unhexlify('6162636465...6162636465666768'),
                               size=2097152, crc32=0x07FD7A5B),
                     ChunkInfo(uuid=ChunkUUID('940f0711-52d7...-818580f432dc'),
                               maxsize_code=0,
                               hash=unhexlify('3031323334...3031323334353637'),
                               size=143941, crc32=0x7E5CE7AD),
                     ChunkInfo(uuid=ChunkUUID('a5b605f2-6ea5...-d217b7e8e784'),
                               maxsize_code=1,
                               hash=unhexlify('3736353433...3736353433323130'),
                               size=2097151, crc32=0xDCC847D8)]})

        @type chunks_to_replicate: NoneType, col.Mapping
        @type chunks_to_restore: NoneType, col.Mapping
        """
        super(ReceiveChunksTransactionState_Node, self).__init__(*args,
                                                                 **kwargs)
        self.chunks_to_replicate = dict(coalesce(chunks_to_replicate, {}))
        self.chunks_to_restore = dict(coalesce(chunks_to_restore, {}))

        if __debug__:
            for d in (self.chunks_to_replicate,
                      self.chunks_to_restore):
                assert consists_of(d.iterkeys(), PeerUUID), \
                       repr(d.keys())
                assert consists_of(d.itervalues(), list), repr(d.values())
                assert all(consists_of(per_inh_chunks, Chunk)
                               for per_inh_chunks in d.itervalues()), \
                       repr(d.values())

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return (u'{super}'
                 '{opt_chunks_to_replicate}{opt_chunks_to_restore}'.format(
                    super=super(ReceiveChunksTransactionState_Node, self)
                              .__str__(),
                    opt_chunks_to_replicate=
                        '' if not self.chunks_to_replicate
                           else u', chunks_to_replicate={!r}'
                                    .format(self.chunks_to_replicate),
                    opt_chunks_to_restore=
                        '' if not self.chunks_to_restore
                           else u', chunks_to_restore={!r}'
                                    .format(self.chunks_to_restore)))


    def to_bson(self):
        r"""
        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> from common.chunks import ChunkInfo
        >>> from common.typed_uuids import ChunkUUID

        >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
        >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
        >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
        >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        >>> u1, u2, u3, u4 = \
        ...     (ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...      ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...      ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...      ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'))
        >>> host_uuid_1, host_uuid_2 = \
        ...     (PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
        ...      PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'))

        >>> # No optional arguments.
        >>> ReceiveChunksTransactionState_Node(
        ...     tr_start_time=tr_start_time,
        ...     tr_uuid=tr_uuid,
        ...     tr_src_uuid=tr_src_uuid,
        ...     tr_dst_uuid=tr_dst_uuid
        ... ).to_bson()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {}

        >>> # All optional arguments.
        >>> ReceiveChunksTransactionState_Node(
        ...     chunks_to_replicate={
        ...         host_uuid_1: [ChunkInfo(crc32=0x2A5FE875, uuid=u4,
        ...                                 maxsize_code=1,
        ...                                 hash='abcdabcd' * 8,
        ...                                 size=73819)],
        ...         host_uuid_2: [ChunkInfo(crc32=0x07FD7A5B, uuid=u1,
        ...                                 maxsize_code=1,
        ...                                 hash='abcdefgh' * 8,
        ...                                 size=2097152),
        ...                       ChunkInfo(crc32=0x7E5CE7AD, uuid=u2,
        ...                                 maxsize_code=0,
        ...                                 hash='01234567' * 8,
        ...                                 size=143941),
        ...                       ChunkInfo(crc32=0xDCC847D8, uuid=u3,
        ...                                 maxsize_code=1,
        ...                                 hash='76543210' * 8,
        ...                                 size=2097151)]
        ...     },
        ...     chunks_to_restore={
        ...         host_uuid_1: [ChunkInfo(crc32=0x07FD7A5B, uuid=u1,
        ...                                 maxsize_code=1,
        ...                                 hash='abcdefgh' * 8,
        ...                                 size=2097152),
        ...                       ChunkInfo(crc32=0x7E5CE7AD, uuid=u2,
        ...                                 maxsize_code=0,
        ...                                 hash='01234567' * 8,
        ...                                 size=143941),
        ...                       ChunkInfo(crc32=0xDCC847D8, uuid=u3,
        ...                                 maxsize_code=1,
        ...                                 hash='76543210' * 8,
        ...                                 size=2097151)],
        ...         host_uuid_2: [ChunkInfo(crc32=0x2A5FE875, uuid=u1,
        ...                                 maxsize_code=1,
        ...                                 hash='abcdabcd' * 8,
        ...                                 size=73819)]
        ...     },
        ...     tr_start_time=tr_start_time,
        ...     tr_uuid=tr_uuid,
        ...     tr_src_uuid=tr_src_uuid,
        ...     tr_dst_uuid=tr_dst_uuid
        ... ).to_bson()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'chunks_to_restore':
             {'233ad9c2268f4506ab0f4c71461c5d88':
                  [{'crc32': 134052443, 'maxsize_code': 1,
                    'hash': Binary('abcdefghabcdefgh...abcdefghabcdefgh', 0),
                    'uuid': ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
                    'size': 2097152},
                   {'crc32': 2120017837, 'maxsize_code': 0,
                    'hash': Binary('0123456701234567...0123456701234567', 0),
                    'uuid': ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
                    'size': 143941},
                   {'crc32': 3704113112, 'maxsize_code': 1,
                    'hash': Binary('7654321076543210...7654321076543210', 0),
                    'uuid': ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
                    'size': 2097151}],
              'e96a073b3cd049a6b14a1fb04c221a9c':
                  [{'crc32': 710928501, 'maxsize_code': 1,
                    'hash': Binary('abcdabcdabcdabcd...abcdabcdabcdabcd', 0),
                    'uuid': ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
                    'size': 73819}]},
         'chunks_to_replicate':
             {'233ad9c2268f4506ab0f4c71461c5d88':
                  [{'crc32': 710928501, 'maxsize_code': 1,
                    'hash': Binary('abcdabcdabcdabcd...abcdabcdabcdabcd', 0),
                    'uuid': ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'),
                    'size': 73819}],
              'e96a073b3cd049a6b14a1fb04c221a9c':
                  [{'crc32': 134052443, 'maxsize_code': 1,
                    'hash': Binary('abcdefghabcdefgh...abcdefghabcdefgh', 0),
                    'uuid': ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
                    'size': 2097152},
                   {'crc32': 2120017837, 'maxsize_code': 0,
                    'hash': Binary('0123456701234567...0123456701234567', 0),
                    'uuid': ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
                    'size': 143941},
                   {'crc32': 3704113112, 'maxsize_code': 1,
                    'hash': Binary('7654321076543210...7654321076543210', 0),
                    'uuid': ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
                    'size': 2097151}]}}
        """
        cls = self.__class__

        doc = super(ReceiveChunksTransactionState_Node, self).to_bson()

        # Optional fields
        if self.chunks_to_replicate:
            doc.update({'chunks_to_replicate':
                            cls._prepare_chunk_maps_for_docstore(
                                self.chunks_to_replicate)})
        if self.chunks_to_restore:
            doc.update({'chunks_to_restore':
                            cls._prepare_chunk_maps_for_docstore(
                                self.chunks_to_restore)})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @staticmethod
    def _prepare_chunk_maps_for_docstore(chunk_map):
        r"""
        Convert a chunk map (like, in C{self.chunks_to_replicate}) or
        in C{self.chunks_to_restore} to the form sufficient to store
        in the FastDB.

        >>> from common.chunks import ChunkInfo
        >>> from common.typed_uuids import ChunkUUID

        >>> u1, u2, u3, u4 = \
        ...     (ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...      ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...      ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...      ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'))

        >>> host_uuid_1, host_uuid_2 = \
        ...     (PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
        ...      PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'))

        >>> cls = ReceiveChunksTransactionState_Node
        >>> cls._prepare_chunk_maps_for_docstore({
        ...     host_uuid_1:
        ...         [ChunkInfo(crc32=0x2A5FE875, uuid=u4,
        ...                    maxsize_code=1,
        ...                    hash='abcdabcd' * 8, size=73819)],
        ...     host_uuid_2:
        ...         [ChunkInfo(crc32=0x07FD7A5B, uuid=u1,
        ...                    maxsize_code=1,
        ...                    hash='abcdefgh' * 8, size=2097152),
        ...          ChunkInfo(crc32=0x7E5CE7AD, uuid=u2,
        ...                    maxsize_code=0,
        ...                    hash='01234567' * 8, size=143941),
        ...          ChunkInfo(crc32=0xDCC847D8, uuid=u3,
        ...                    maxsize_code=1,
        ...                    hash='76543210' * 8, size=2097151)]
        ...     })  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'233ad9c2268f4506ab0f4c71461c5d88':
             [{'crc32': 710928501,
               'maxsize_code': 1,
               'hash': Binary('abcdabcdabcdabcd...abcdabcdabcdabcd', 0),
               'uuid': ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'),
               'size': 73819}],
         'e96a073b3cd049a6b14a1fb04c221a9c':
             [{'crc32': 134052443,
               'maxsize_code': 1,
               'hash': Binary('abcdefghabcdefgh...abcdefghabcdefgh', 0),
               'uuid': ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
               'size': 2097152},
              {'crc32': 2120017837,
               'maxsize_code': 0,
               'hash': Binary('0123456701234567...0123456701234567', 0),
               'uuid': ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
               'size': 143941},
              {'crc32': 3704113112,
               'maxsize_code': 1,
               'hash': Binary('7654321076543210...7654321076543210', 0),
               'uuid': ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
               'size': 2097151}]}

        @type chunk_map: dict
        @rtype: dict
        """
        return {host_uuid.hex: [model_ChunkInfo.from_common(c).to_bson()
                                    for c in chunks]
                    for host_uuid, chunks in chunk_map.iteritems()}


    @staticmethod
    def _prepare_chunk_maps_from_docstore(chunk_map):
        r"""
        Convert a chunk map (like, in C{self.chunks_to_replicate}) or
        in C{self.chunks_to_restore} from the form stored in the FastDB
        to the proper classes.

        >>> from uuid import UUID

        >>> from bson.binary import Binary

        >>> cls = ReceiveChunksTransactionState_Node
        >>> cls._prepare_chunk_maps_from_docstore({
        ...     'e96a073b3cd049a6b14a1fb04c221a9c':
        ...         [{'crc32': 710928501, 'maxsize_code': 1,
        ...           'hash': Binary('abcdabcd' * 8, 0),
        ...           'uuid':UUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...           'size': 73819}],
        ...     '233ad9c2268f4506ab0f4c71461c5d88':
        ...         [{'crc32': 134052443, 'maxsize_code': 1,
        ...           'hash': Binary('abcdefgh' * 8, 0),
        ...           'uuid':UUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...           'size': 2097152},
        ...          {'crc32': 2120017837, 'maxsize_code': 0,
        ...           'hash': Binary('01234567' * 8, 0),
        ...           'uuid':UUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...           'size': 143941},
        ...          {'crc32': 3704113112, 'maxsize_code': 1,
        ...           'hash': Binary('76543210' * 8, 0),
        ...           'uuid':UUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...           'size': 2097151}]
        ... })  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'):
             [models.types.ChunkInfo(uuid=ChunkUUID('5b237ceb-...331cb14b5b4'),
                                     maxsize_code=1,
                                     hash=unhexlify('616263646...36461626364'),
                                     size=73819, crc32=0x2A5FE875)],
         PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'):
             [models.types.ChunkInfo(uuid=ChunkUUID('5b237ceb-...331cb14b5b4'),
                                     maxsize_code=1,
                                     hash=unhexlify('616263646...36465666768'),
                                     size=2097152, crc32=0x07FD7A5B),
              models.types.ChunkInfo(uuid=ChunkUUID('940f0711-...18580f432dc'),
                                     maxsize_code=0,
                                     hash=unhexlify('303132333...23334353637'),
                                     size=143941, crc32=0x7E5CE7AD),
              models.types.ChunkInfo(uuid=ChunkUUID('a5b605f2-...217b7e8e784'),
                                     maxsize_code=1,
                                     hash=unhexlify('373635343...53433323130'),
                                     size=2097151, crc32=0xDCC847D8)]}

        @type chunk_map: dict
        @rtype: dict
        """
        return {PeerUUID(host_uuid_str):
                    [model_ChunkInfo.from_bson(chunk_doc)()
                         for chunk_doc in chunk_docs]
                    for host_uuid_str, chunk_docs in chunk_map.iteritems()}


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> from bson.binary import Binary

        >>> from common.chunks import ChunkInfo

        >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
        >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
        >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
        >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')

        >>> # Deserialize with no optional aruments
        >>> ReceiveChunksTransactionState_Node.from_bson({
        ... })(tr_start_time=tr_start_time,
        ...    tr_uuid=tr_uuid,
        ...    tr_src_uuid=tr_src_uuid,
        ...    tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ReceiveChunks...State_Node(tr_start_time=datetime.datetime(2012, 9, 26,
                                                           14, 29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))
        >>> # Deserialize with all optional arguments
        >>> ReceiveChunksTransactionState_Node.from_bson({
        ...     'chunks_to_restore':
        ...         {'e96a073b3cd049a6b14a1fb04c221a9c':
        ...              [{'crc32': 710928501, 'maxsize_code': 1,
        ...                'hash': Binary('abcdabcd' * 8, 0),
        ...                'uuid':UUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...                'size': 73819}],
        ...          '233ad9c2268f4506ab0f4c71461c5d88':
        ...              [{'crc32': 134052443, 'maxsize_code': 1,
        ...                'hash': Binary('abcdefgh' * 8, 0),
        ...                'uuid':UUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...                'size': 2097152},
        ...               {'crc32': 2120017837, 'maxsize_code': 0,
        ...                'hash': Binary('01234567' * 8, 0),
        ...                'uuid':UUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...                'size': 143941},
        ...               {'crc32': 3704113112, 'maxsize_code': 1,
        ...                'hash': Binary('76543210' * 8, 0),
        ...                'uuid':UUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...                'size': 2097151}]},
        ...     'chunks_to_replicate':
        ...         {'e96a073b3cd049a6b14a1fb04c221a9c':
        ...              [{'crc32': 134052443, 'maxsize_code': 1,
        ...                'hash': Binary('abcdefgh' * 8, 0),
        ...                'uuid':UUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...                'size': 2097152},
        ...               {'crc32': 2120017837, 'maxsize_code': 0,
        ...                'hash': Binary('01234567' * 8, 0),
        ...                'uuid':UUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...                'size': 143941},
        ...               {'crc32': 3704113112, 'maxsize_code': 1,
        ...                'hash': Binary('76543210' * 8, 0),
        ...                'uuid':UUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...                'size': 2097151}],
        ...          '233ad9c2268f4506ab0f4c71461c5d88':
        ...              [{'crc32': 710928501, 'maxsize_code': 1,
        ...                'hash': Binary('abcdabcd' * 8, 0),
        ...                'uuid':UUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'),
        ...                'size': 73819}]}
        ... })(tr_start_time=tr_start_time,
        ...    tr_uuid=tr_uuid,
        ...    tr_src_uuid=tr_src_uuid,
        ...    tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ReceiveCh...State_Node(tr_start_time=datetime.datetime(2012, 9, 26, 14,
                                                               29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            chunks_to_replicate={PeerUUID('e96a073b-...b04c221a9c'):
                    [models.types.ChunkInfo(uuid=ChunkUUID('5b237ceb-...b5b4'),
                                            maxsize_code=1,
                                            hash=unhexlify('616263646...6768'),
                                            size=2097152, crc32=0x07FD7A5B),
                     models.types.ChunkInfo(uuid=ChunkUUID('940f0711-...32dc'),
                                            maxsize_code=0,
                                            hash=unhexlify('303132333...3637'),
                                            size=143941, crc32=0x7E5CE7AD),
                     models.types.ChunkInfo(uuid=ChunkUUID('a5b605f2-...e784'),
                                            maxsize_code=1,
                                            hash=unhexlify('373635343...3130'),
                                            size=2097151, crc32=0xDCC847D8)],
                PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'):
                    [models.types.ChunkInfo(uuid=ChunkUUID('0a7064b3-...fcf3'),
                                            maxsize_code=1,
                                            hash=unhexlify('616263646...6364'),
                                            size=73819, crc32=0x2A5FE875)]},
            chunks_to_restore={PeerUUID('e96a073b-...1fb04c221a9c'):
                    [models.types.ChunkInfo(uuid=ChunkUUID('5b237ceb-...b5b4'),
                                            maxsize_code=1,
                                            hash=unhexlify('616263646...6364'),
                                            size=73819, crc32=0x2A5FE875)],
                PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'):
                    [models.types.ChunkInfo(uuid=ChunkUUID('5b237ceb-...b5b4'),
                                            maxsize_code=1,
                                            hash=unhexlify('616263646...6768'),
                                            size=2097152, crc32=0x07FD7A5B),
                     models.types.ChunkInfo(uuid=ChunkUUID('940f0711-...32dc'),
                                            maxsize_code=0,
                                            hash=unhexlify('303132333...3637'),
                                            size=143941, crc32=0x7E5CE7AD),
                     models.types.ChunkInfo(uuid=ChunkUUID('a5b605f2-...e784'),
                                            maxsize_code=1,
                                            hash=unhexlify('373635343...3130'),
                                            size=2097151, crc32=0xDCC847D8)]})
        """
        assert cls.validate_schema(doc), repr(doc)

        raw_chunks_to_replicate = doc.get('chunks_to_replicate')
        raw_chunks_to_restore = doc.get('chunks_to_restore')
        chunks_to_replicate = \
            None if raw_chunks_to_replicate is None \
                 else cls._prepare_chunk_maps_from_docstore(
                          raw_chunks_to_replicate)
        chunks_to_restore = \
            None if raw_chunks_to_restore is None \
                 else cls._prepare_chunk_maps_from_docstore(
                          raw_chunks_to_restore)

        return partial(super(ReceiveChunksTransactionState_Node, cls)
                           .from_bson(doc),
                         # Optional
                       chunks_to_replicate=chunks_to_replicate,
                       chunks_to_restore=chunks_to_restore)
