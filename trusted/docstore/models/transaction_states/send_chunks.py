#!/usr/bin/python
"""
SEND_CHUNKS transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from functools import partial

from contrib.dbc import consists_of, contract_epydoc

from common.inhabitants import Host
from common.chunks import Chunk
from common.typed_uuids import PeerUUID

from ._common import NodeTransactionState
from ..types import ChunkInfo as model_ChunkInfo



#
# Classes
#

class SendChunksTransactionState_Node(NodeTransactionState):
    """The state for the SEND_CHUNKS transaction on the Node.

    @type chunks_map: dict

    @todo: the C{Host} objects omit the original name and may contain
        non-actual URLs.
    """

    __slots__ = ('chunks_map',)

    name = 'SEND_CHUNKS'


    @contract_epydoc
    def __init__(self, chunks_map, *args, **kwargs):
        r"""Constructor.

        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> from common.chunks import ChunkInfo
        >>> from common.inhabitants import User
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
        >>> host1 = Host(
        ...     name="SecretHost1",
        ...     user=User(name="SecretUser1",
        ...               digest="1a73bf8f3e54a5c5e3cecfe38d25fdf82587b868"),
        ...     uuid=PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
        ...     urls=["https://192.168.1.2:1234", "https://127.0.0.1:1234"]
        ... )
        >>> host2 = Host(
        ...     name="SecretHost2",
        ...     user=User(name="SecretUser2",
        ...               digest="e5e503e5197792ec4d4bac2dd4d3d1c870efbdbb"),
        ...     uuid=PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
        ...     urls=["https://192.168.2.3:4242"]
        ... )

        >>> SendChunksTransactionState_Node(
        ...     chunks_map={
        ...         host1: [ChunkInfo(crc32=0x2A5FE875, uuid=u4,
        ...                           maxsize_code=1,
        ...                           hash='abcdabcd' * 8, size=73819)],
        ...         host2: [ChunkInfo(crc32=0x07FD7A5B, uuid=u1,
        ...                           maxsize_code=1,
        ...                           hash='abcdefgh' * 8, size=2097152),
        ...                 ChunkInfo(crc32=0x7E5CE7AD, uuid=u2,
        ...                           maxsize_code=0,
        ...                           hash='01234567' * 8, size=143941),
        ...                 ChunkInfo(crc32=0xDCC847D8, uuid=u3,
        ...                           maxsize_code=1,
        ...                           hash='76543210' * 8, size=2097151)]
        ...     },
        ...     tr_start_time=tr_start_time,
        ...     tr_uuid=tr_uuid,
        ...     tr_src_uuid=tr_src_uuid,
        ...     tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        SendCh...State_Node(chunks_map={Host(uuid=PeerUUID('e96a073b-...1a9c'),
                    urls=['https://192.168.2.3:4242'],
                    name='SecretHost2',
                    user=User(name='SecretUser2',
                              digest='e5e503e5197792ec4...dd4d3d1c870efbdbb')):
                [ChunkInfo(uuid=ChunkUUID('5b237ceb-...-6331cb14b5b4'),
                           maxsize_code=1,
                           hash=unhexlify('616263646...667686162636465666768'),
                           size=2097152, crc32=0x07FD7A5B),
                 ChunkInfo(uuid=ChunkUUID('940f0711-...-818580f432dc'),
                           maxsize_code=0,
                           hash=unhexlify('303132333...536373031323334353637'),
                           size=143941, crc32=0x7E5CE7AD),
                 ChunkInfo(uuid=ChunkUUID('a5b605f2-...-d217b7e8e784'),
                           maxsize_code=1,
                           hash=unhexlify('373635343...231303736353433323130'),
                           size=2097151, crc32=0xDCC847D8)],
            Host(uuid=PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
                 urls=['https://192.168.1.2:1234',
                       'https://127.0.0.1:1234'],
                 name='SecretHost1',
                 user=User(name='SecretUser1',
                           digest='1a73bf8f3e54a5c5e3cecfe38d25fdf82587b868')):
                [ChunkInfo(uuid=ChunkUUID('0a7064b3-...-e9f9a40dfcf3'),
                           maxsize_code=1,
                           hash=unhexlify('616263646...263646162636461626364'),
                           size=73819, crc32=0x2A5FE875)]})

        @type chunks_map: col.Mapping
        """
        super(SendChunksTransactionState_Node, self).__init__(*args, **kwargs)
        self.chunks_map = dict(chunks_map)

        # Validate date
        assert consists_of(self.chunks_map.iterkeys(), Host), \
               repr(chunks_map)
        assert consists_of((ch
                                for ch_list in chunks_map.itervalues()
                                for ch in ch_list),
                               Chunk), \
               repr(chunks_map)

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return u'chunks_map={self.chunks_map!r}'.format(self=self)


    def to_bson(self):
        r"""
        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> from common.chunks import ChunkInfo
        >>> from common.inhabitants import User
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
        >>> host1 = Host(
        ...     name="SecretHost1",
        ...     user=User(name="SecretUser1",
        ...               digest="1a73bf8f3e54a5c5e3cecfe38d25fdf82587b868"),
        ...     uuid=PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
        ...     urls=["https://192.168.1.2:1234", "https://127.0.0.1:1234"]
        ... )
        >>> host2 = Host(
        ...     name="SecretHost2",
        ...     user=User(name="SecretUser2",
        ...               digest="e5e503e5197792ec4d4bac2dd4d3d1c870efbdbb"),
        ...     uuid=PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
        ...     urls=["https://192.168.2.3:4242"]
        ... )

        >>> SendChunksTransactionState_Node(
        ...     chunks_map={
        ...         host1: [ChunkInfo(crc32=0x2A5FE875, uuid=u4,
        ...                           maxsize_code=1,
        ...                           hash='abcdabcd' * 8, size=73819)],
        ...         host2: [ChunkInfo(crc32=0x07FD7A5B, uuid=u1,
        ...                           maxsize_code=1,
        ...                           hash='abcdefgh' * 8, size=2097152),
        ...                 ChunkInfo(crc32=0x7E5CE7AD, uuid=u2,
        ...                           maxsize_code=0,
        ...                           hash='01234567' * 8, size=143941),
        ...                 ChunkInfo(crc32=0xDCC847D8, uuid=u3,
        ...                           maxsize_code=1,
        ...                           hash='76543210' * 8, size=2097151)]
        ...     },
        ...     tr_start_time=tr_start_time,
        ...     tr_uuid=tr_uuid,
        ...     tr_src_uuid=tr_src_uuid,
        ...     tr_dst_uuid=tr_dst_uuid
        ... ).to_bson()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'chunks_map':
             {'233ad9c2268f4506ab0f4c71461c5d88':
                  {'chunks': [{'crc32': 710928501, 'maxsize_code': 1,
                               'hash': Binary('abcdabcd...bcdabcdabcdabcd', 0),
                               'uuid': ChunkUUID('0a7064b3-...-e9f9a40dfcf3'),
                               'size': 73819}],
                   'urls': ['https://192.168.1.2:1234',
                            'https://127.0.0.1:1234']},
              'e96a073b3cd049a6b14a1fb04c221a9c':
                  {'chunks': [{'crc32': 134052443, 'maxsize_code': 1,
                               'hash': Binary('abcdefgh...bcdefghabcdefgh', 0),
                               'uuid': ChunkUUID('5b237ceb-...-6331cb14b5b4'),
                               'size': 2097152},
                              {'crc32': 2120017837, 'maxsize_code': 0,
                               'hash': Binary('01234567...123456701234567', 0),
                               'uuid': ChunkUUID('940f0711-...-818580f432dc'),
                               'size': 143941},
                              {'crc32': 3704113112, 'maxsize_code': 1,
                               'hash': Binary('76543210...654321076543210', 0),
                               'uuid': ChunkUUID('a5b605f2-...-d217b7e8e784'),
                               'size': 2097151}],
                   'urls': ['https://192.168.2.3:4242']}}}
        """
        cls = self.__class__

        doc = super(SendChunksTransactionState_Node, self).to_bson()

        chunks_map_preprocessed = \
            {host.uuid.hex:
                 {'urls': host.urls,
                  'chunks': [model_ChunkInfo.from_common(c).to_bson()
                                 for c in chunks]}
                 for host, chunks in self.chunks_map.iteritems()}

        # Mandatory fields
        doc.update({'chunks_map': chunks_map_preprocessed})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> from bson.binary import Binary

        >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
        >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
        >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
        >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')

        >>> SendChunksTransactionState_Node.from_bson({
        ...     'chunks_map': {
        ...         '233ad9c2268f4506ab0f4c71461c5d88':
        ...             {'chunks':
        ...                  [{'crc32': 710928501, 'maxsize_code': 1,
        ...                    'hash': Binary('abcdabcd' * 8, 0),
        ...                    'uuid':
        ...                       UUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'),
        ...                    'size': 73819}],
        ...              'urls': ['https://192.168.1.2:1234',
        ...                       'https://127.0.0.1:1234']},
        ...         'e96a073b3cd049a6b14a1fb04c221a9c':
        ...             {'chunks':
        ...                  [{'crc32': 134052443, 'maxsize_code': 1,
        ...                    'hash': Binary('abcdefgh' * 8, 0),
        ...                    'uuid':
        ...                       UUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...                    'size': 2097152},
        ...                   {'crc32': 2120017837, 'maxsize_code': 0,
        ...                    'hash': Binary('01234567' * 8, 0),
        ...                    'uuid':
        ...                       UUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...                    'size': 143941},
        ...                   {'crc32': 3704113112, 'maxsize_code': 1,
        ...                    'hash': Binary('76543210' * 8, 0),
        ...                    'uuid':
        ...                       UUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...                    'size': 2097151}],
        ...              'urls': ['https://192.168.2.3:4242']}
        ...     }
        ... })(tr_start_time=tr_start_time,
        ...    tr_uuid=tr_uuid,
        ...    tr_src_uuid=tr_src_uuid,
        ...    tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        SendCh...State_Node(chunks_map={Host(uuid=PeerUUID('e96a073b...1a9c'),
                                             urls=['https://192...3:4242']):
                [models.t...ChunkInfo(uuid=ChunkUUID('5b237ceb-...31cb14b5b4'),
                                      maxsize_code=1,
                                      hash=unhexlify('616263646...6465666768'),
                                      size=2097152, crc32=0x07FD7A5B),
                 models.t...ChunkInfo(uuid=ChunkUUID('940f0711-...8580f432dc'),
                                      maxsize_code=0,
                                      hash=unhexlify('303132333...3334353637'),
                                      size=143941, crc32=0x7E5CE7AD),
                 models.t...ChunkInfo(uuid=ChunkUUID('a5b605f2-...17b7e8e784'),
                                      maxsize_code=1,
                                      hash=unhexlify('373635343...3433323130'),
                                      size=2097151, crc32=0xDCC847D8)],
            Host(uuid=PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
                 urls=['https://192.168.1.2:1234', 'https://127.0.0.1:1234']):
                [models.t...ChunkInfo(uuid=ChunkUUID('0a7064b3-...f9a40dfcf3'),
                                      maxsize_code=1,
                                      hash=unhexlify('616263646...6461626364'),
                                      size=73819, crc32=0x2A5FE875)]})
        """
        assert cls.validate_schema(doc), repr(doc)

        chunks_map_preprocessed = \
            {Host(uuid=PeerUUID(host_uuid_str),
                  urls=per_chunk_data['urls']):
                 [model_ChunkInfo.from_bson(doc)()
                      for doc in per_chunk_data['chunks']]
                 for host_uuid_str, per_chunk_data
                     in doc['chunks_map'].iteritems()}

        return partial(super(SendChunksTransactionState_Node, cls)
                           .from_bson(doc),
                         # Mandatory
                       chunks_map=chunks_map_preprocessed)
