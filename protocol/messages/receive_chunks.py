#!/usr/bin/python
"""RECEIVE_CHUNKS message implementation.

RECEIVE_CHUNKS notifies an acceptor Host that some chunks are going to be sent
to it soon.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from common.abstractions import AbstractMessage

from common.chunks import Chunk, ChunkInfo
from common.inhabitants import Host
from common.typed_uuids import HostUUID, PeerUUID

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class ReceiveChunksMessage(AbstractMessage):
    """RECEIVE_CHUNKS message."""

    name = 'RECEIVE_CHUNKS'
    version = 0

    __slots__ = ('expect_replication', 'expect_restore',
                 'ack_ready')


    def __init__(self, *args, **kwargs):
        super(ReceiveChunksMessage, self).__init__(*args, **kwargs)
        self.expect_replication = {}
        self.expect_restore = {}
        self.ack_ready = False


    if __debug__:
        def __invariant(self):
            """Verify the class invariants."""
            for d in (self.expect_replication,
                      self.expect_restore,):
                assert consists_of(d.iterkeys(), PeerUUID), repr(d.keys())
                assert consists_of(d.itervalues(), list), repr(d.values())
                assert all(consists_of(per_inh_list, Chunk)
                               for per_inh_list in d.itervalues()), \
                       repr(d.values())
            return True


    @staticmethod
    @contract_epydoc
    def _expect_map_encode(exp_dict):
        r"""
        Encode the dictionary mapping the UUID of the sending host
        to the chunk list into the form ready for transportation.

        >>> from common.abstractions import PeerUUID
        >>> from common.chunks import ChunkInfo
        >>> from common.typed_uuids import ChunkUUID

        >>> u1, u2, u3, u4 = \
        ...     (ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...      ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...      ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
        ...      ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'))

        >>> mapping = {
        ...     PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'):
        ...         [ChunkInfo(crc32=0x2A5FE875,
        ...                    uuid=u4,
        ...                    maxsize_code=1,
        ...                    hash='abcdabcd' * 8, size=73819)],
        ...     PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'):
        ...         [ChunkInfo(crc32=0x07FD7A5B,
        ...                    uuid=u1,
        ...                    maxsize_code=1,
        ...                    hash='abcdefgh' * 8, size=2097152),
        ...             ChunkInfo(crc32=0x7E5CE7AD,
        ...                    uuid=u2,
        ...                    maxsize_code=0,
        ...                    hash='01234567' * 8, size=143941),
        ...             ChunkInfo(crc32=0xDCC847D8,
        ...                    uuid=u3,
        ...                    maxsize_code=1,
        ...                    hash='76543210' * 8, size=2097151)]
        ... }

        >>> ReceiveChunksMessage ._expect_map_encode(
        ...     mapping)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'233ad9c2268f4506ab0f4c71461c5d88':
             [{'crc32': 710928501,
               'maxsize_code': 1,
               'hash': 'YWJjZGFiY2RhYmNkYWJjZG...mNkYWJjZGFiY2RhYmNkYWJjZA==',
               'uuid': '0a7064b3bef645c09e82e9f9a40dfcf3',
               'size': 73819}],
         'e96a073b3cd049a6b14a1fb04c221a9c':
             [{'crc32': 134052443,
               'maxsize_code': 1,
               'hash': 'YWJjZGVmZ2hhYmNkZWZnaG...mdoYWJjZGVmZ2hhYmNkZWZnaA==',
               'uuid': '5b237ceb300d4c88b4c06331cb14b5b4',
               'size': 2097152},
              {'crc32': 2120017837,
               'maxsize_code': 0,
               'hash': 'MDEyMzQ1NjcwMTIzNDU2Nz...TY3MDEyMzQ1NjcwMTIzNDU2Nw==',
               'uuid': '940f071152d742fbbf4c818580f432dc',
               'size': 143941},
              {'crc32': 3704113112,
               'maxsize_code': 1,
               'hash': 'NzY1NDMyMTA3NjU0MzIxMD...jEwNzY1NDMyMTA3NjU0MzIxMA==',
               'uuid': 'a5b605f26ea549f38658d217b7e8e784',
               'size': 2097151}]}

        @type exp_dict: dict
        @precondition: consists_of(exp_dict.iterkeys(), PeerUUID)
        @precondition: consists_of(exp_dict.itervalues(), list)
        @precondition: all(consists_of(chunk_list, Chunk)
                               for chunk_list in exp_dict.itervalues())
        """
        return {inh_uuid.hex: [c.to_json() for c in chunk_list]
                    for inh_uuid, chunk_list in exp_dict.iteritems()}


    @staticmethod
    def _expect_map_decode(tr_dict):
        r"""
        Encode the dictionary mapping the Host UUID to the chunk list
        from the transportation form to the original one.

        >>> ReceiveChunksMessage ._expect_map_decode({
        ...     '233ad9c2268f4506ab0f4c71461c5d88':
        ...         [{'crc32': 710928501,
        ...           'maxsize_code': 1,
        ...           'hash': 'YWJjZGFiY2RhYmNkYWJjZGFiY2RhYmNk'
        ...                   'YWJjZGFiY2RhYmNkYWJjZGFiY2RhYmNk'
        ...                   'YWJjZGFiY2RhYmNkYWJjZA==',
        ...           'uuid': '0a7064b3bef645c09e82e9f9a40dfcf3',
        ...           'size': 73819}],
        ...     'e96a073b3cd049a6b14a1fb04c221a9c':
        ...         [{'crc32': 134052443,
        ...           'maxsize_code': 1,
        ...           'hash': 'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo'
        ...                   'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo'
        ...                   'YWJjZGVmZ2hhYmNkZWZnaA==',
        ...           'uuid': '5b237ceb300d4c88b4c06331cb14b5b4',
        ...           'size': 2097152},
        ...          {'crc32': 2120017837,
        ...           'maxsize_code': 0,
        ...           'hash': 'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3'
        ...                   'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3'
        ...                   'MDEyMzQ1NjcwMTIzNDU2Nw==',
        ...           'uuid': '940f071152d742fbbf4c818580f432dc',
        ...           'size': 143941},
        ...          {'crc32': 3704113112,
        ...           'maxsize_code': 1,
        ...           'hash': 'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw'
        ...                   'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw'
        ...                   'NzY1NDMyMTA3NjU0MzIxMA==',
        ...           'uuid': 'a5b605f26ea549f38658d217b7e8e784',
        ...           'size': 2097151}]
        ... })  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {HostUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'):
             [ChunkInfo(uuid=ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
                        maxsize_code=1,
                        hash=unhexlify('6162636465666...67686162636465666768'),
                        size=2097152, crc32=0x07FD7A5B),
              ChunkInfo(uuid=ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
                        maxsize_code=0,
                        hash=unhexlify('3031323334353...36373031323334353637'),
                        size=143941, crc32=0x7E5CE7AD),
              ChunkInfo(uuid=ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
                        maxsize_code=1,
                        hash=unhexlify('3736353433323...31303736353433323130'),
                        size=2097151, crc32=0xDCC847D8)],
         HostUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'):
             [ChunkInfo(uuid=ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'),
                        maxsize_code=1,
                        hash=unhexlify('6162636461626...63646162636461626364'),
                        size=73819, crc32=0x2A5FE875)]}
        """
        return {HostUUID(inh_uuid_str):
                        [ChunkInfo.from_json(c)() for c in per_inh]
                    for inh_uuid_str, per_inh in tr_dict.iteritems()}


    @bzip2
    @json_dumps
    def _get_body(self):
        # N2H
        cls = self.__class__
        assert self.__invariant()
        return {'replication': cls._expect_map_encode(self.expect_replication),
                'restore': cls._expect_map_encode(self.expect_restore)}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # N2H
        cls = self.__class__
        self.expect_replication = cls._expect_map_decode(body['replication'])
        self.expect_restore = cls._expect_map_decode(body['restore'])


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # H2N
        return {'ready': int(self.ack_ready)}


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # H2N
        self.ack_ready = bool(body.get('ready', 0))
