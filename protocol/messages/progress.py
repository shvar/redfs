#!/usr/bin/python
"""PROGRESS message implementation.

PROGRESS message sends the Node information about the progress of backup
procedure running on this Host.

Three combinations of progress data are currently allowed:

  1. dataset - on backup begin.
  2. host_chunks_map - on any successful chunk upload (actually, not necessary
     to be inside/related to e Backup procedure).
  3. dataset + blocks_map - on backup completion.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from common.abstract_dataset import AbstractDataset
from common.abstractions import AbstractMessage
from common.chunks import Chunk, ChunkInfo
from common.datasets import DatasetOnChunks
from common.datatypes import ProgressNotificationPerHost
from common.inhabitants import Host, Node
from common.itertools_ex import repr_long_sized_iterable
from common.typed_uuids import ChunkUUID

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ProgressMessage(AbstractMessage):
    """PROGRESS message.

    @ivar chunks_by_uuid: the mapping from the chunk UUID to the Chunk
      structure itself.
    @type chunks_by_uuid: col.Mapping

    @ivar chunks_map_getter: the callable which can get the actual mapping
                             from the host to the progress notifications.
    """

    name = 'PROGRESS'
    version = 0

    __slots__ = ('dataset', 'completion',
                 'chunks_by_uuid', 'chunks_map_getter', 'blocks_map')


    def __init__(self, *args, **kwargs):
        r"""Constructor.

        >>> from common.typed_uuids import MessageUUID, PeerUUID

        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> # Smoke test
        >>> ProgressMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ProgressMessage(src=Node(uuid=PeerUUID('11111111-...-8c4ce6a2aed9')),
            dst=Host(uuid=PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d')),
            uuid=MessageUUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15'),
            status_code=0)
        """
        super(ProgressMessage, self).__init__(*args, **kwargs)
        self.completion = False
        self.dataset = None
        self.chunks_by_uuid = None
        self.chunks_map_getter = None
        self.blocks_map = None


    def __str__(self):

        opt_completion = u'completion={!r}'.format(self.completion) \
                             if self.completion != False \
                             else ''
        opt_dataset = u'dataset={!r}'.format(self.dataset) \
                          if self.dataset is not None \
                          else ''
        opt_chunks_by_uuid = u'chunks_by_uuid={!r}'.format(
                                     self.chunks_by_uuid) \
                                 if self.chunks_by_uuid is not None \
                                 else ''
        opt_chunks_map_getter = u'chunks_map_getter={!r}'.format(
                                        self.chunks_map_getter) \
                                    if self.chunks_map_getter is not None \
                                    else ''
        opt_blocks_map = \
            u'blocks_map={!s}'.format(repr_long_sized_iterable(
                                          self.blocks_map, 15)) \
            if self.blocks_map is not None \
            else ''
        opt_strings = [opt_completion,
                       opt_dataset,
                       opt_chunks_by_uuid,
                       opt_chunks_map_getter,
                       opt_blocks_map]
        opts = ', '.join(filter(None, opt_strings))

        return u'{super}{opts}'.format(
                   super=super(ProgressMessage, self).__str__(),
                   opts=' [ {} ]'.format(opts) if opts else '')


    @bzip2
    @json_dumps
    @contract_epydoc
    def _get_body(self):
        r"""
        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> u1, u2, u3 = \
        ...     (ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...      ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
        ...      ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'))

        >>> msg0 = ProgressMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )
        >>> ProgressMessage._get_body._without_bzip2._without_json_dumps(msg0)
        {}

        >>> msg1 = ProgressMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f')
        ... )
        >>> msg1.completion = True
        >>> # msg1.dataset = ??? # TODO: DatasetOnChunks() ?
        >>> msg1.chunks_by_uuid = {
        ...     u1: ChunkInfo(crc32=0x07FD7A5B,
        ...                   uuid=u1,
        ...                   maxsize_code=1,
        ...                   hash='abcdefgh' * 8,
        ...                   size=2097152),
        ...     u2: ChunkInfo(crc32=0x7E5CE7AD,
        ...                   uuid=u2,
        ...                   maxsize_code=0,
        ...                   hash='01234567' * 8,
        ...                   size=143941),
        ...     u3: ChunkInfo(crc32=0xDCC847D8,
        ...                   uuid=u3,
        ...                   maxsize_code=1,
        ...                   hash='76543210' * 8,
        ...                   size=2097152)
        ... }
        >>> # msg1.chunks_map_getter = ??? # TODO
        >>> # msg1.blocks_map = ??? # TODO

        >>> ProgressMessage._get_body._without_bzip2._without_json_dumps(
        ...     msg1)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'chunks':
             [{'crc32': 2120017837, 'maxsize_code': 0,
               'hash': 'MDEyMzQ1NjcwMTIzNDU2NzAxM...EyMzQ1NjcwMTIzNDU2Nw==',
               'uuid': '940f071152d742fbbf4c818580f432dc', 'size': 143941},
              {'crc32': 3704113112, 'maxsize_code': 1,
               'hash': 'NzY1NDMyMTA3NjU0MzIxMDc2N...Y1NDMyMTA3NjU0MzIxMA==',
               'uuid': 'a5b605f26ea549f38658d217b7e8e784', 'size': 2097152},
              {'crc32': 134052443, 'maxsize_code': 1,
               'hash': 'YWJjZGVmZ2hhYmNkZWZnaGFiY...JjZGVmZ2hhYmNkZWZnaA==',
               'uuid': '5b237ceb300d4c88b4c06331cb14b5b4', 'size': 2097152}]}

        @precondition: isinstance(self.dataset, (NoneType, AbstractDataset))
        @precondition: isinstance(self.chunks_map_getter,
                                  (NoneType, col.Callable))
        @precondition: isinstance(self.chunks_by_uuid,
                                  (NoneType, col.Mapping))
        """
        if not any((self.dataset is not None,
                    self.chunks_map_getter is not None)):
            logger.warning('For %r, dataset and chunks_map are both empty',
                           self)
        # H2N
        data = {}

        # Will be added at the last moment
        _chunks_by_uuid = {}

        # Encode dataset
        if self.dataset is not None:
            data['dataset_completed' if self.completion
                                     else 'dataset'] = self.dataset.to_json()

        # Encode mapping from chunk UUID to the Chunk Info
        if self.chunks_by_uuid is not None:
            assert consists_of(self.chunks_by_uuid.iterkeys(), UUID), \
                   repr(self.chunks_by_uuid)
            assert consists_of(self.chunks_by_uuid.itervalues(), Chunk), \
                   repr(self.chunks_by_uuid)
            _chunks_by_uuid = dict(self.chunks_by_uuid)

        # Encode host_chunks_map (a mapping defining which host
        # received which chunks).
        if self.chunks_map_getter is not None:
            host_chunks_map = self.chunks_map_getter()

            assert consists_of(host_chunks_map.iterkeys(), Host), \
                   repr(host_chunks_map)

            assert consists_of(host_chunks_map.iterkeys(), Host), \
                   repr(host_chunks_map)
            assert all(consists_of(v, ProgressNotificationPerHost)
                           for v in host_chunks_map.itervalues()), \
                   repr(host_chunks_map)

            data['host_chunks_map'] = \
                {host.uuid.hex: [notif.to_json() for notif in per_host_notifs]
                     for host, per_host_notifs in host_chunks_map.iteritems()}

        # Encode blocks_map (a mapping defining which chunks
        # contain which blocks).
        if self.blocks_map is not None:
            data['blocks_map'] = [block.to_json(chunk)
                                      for (block, chunk) in self.blocks_map]
            # Update the 'chunks' field as well
            _chunks_by_uuid.update({chunk.uuid: chunk
                                        for (block, chunk) in self.blocks_map})

        # Finally, let's add the info about the chunks.
        # Currently it is stored as a dict, but it will be kept in the JSON
        # as the list.
        if _chunks_by_uuid:
            data['chunks'] = [v.to_json()
                                  for v in _chunks_by_uuid.itervalues()]

        return data


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        r"""
        >>> from common.typed_uuids import ChunkUUID, MessageUUID, PeerUUID

        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> msg0 = ProgressMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )
        >>> ProgressMessage._init_from_body \
        ...                ._without_bunzip2._without_json_loads(msg0, {})

        >>> msg1 = ProgressMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f'),
        ...     status_code=1
        ... )
        >>> # TODO: add other fields, see _get_body() doctest for that.
        >>> data1 = {
        ...     'chunks': [
        ...         {'crc32': 2120017837,
        ...          'maxsize_code': 0,
        ...          'hash': 'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3'
        ...                  'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3'
        ...                  'MDEyMzQ1NjcwMTIzNDU2Nw==',
        ...          'uuid': '940f071152d742fbbf4c818580f432dc',
        ...          'size': 143941},
        ...         {'crc32': 3704113112,
        ...          'maxsize_code': 1,
        ...          'hash': 'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw'
        ...                  'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw'
        ...                  'NzY1NDMyMTA3NjU0MzIxMA==',
        ...          'uuid': 'a5b605f26ea549f38658d217b7e8e784',
        ...          'size': 2097152},
        ...         {'crc32': 134052443,
        ...          'maxsize_code': 1,
        ...          'hash': 'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo'
        ...                  'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo'
        ...                  'YWJjZGVmZ2hhYmNkZWZnaA==',
        ...          'uuid': '5b237ceb300d4c88b4c06331cb14b5b4',
        ...          'size': 2097152}]
        ... }

        >>> ProgressMessage._init_from_body \
        ...                ._without_bunzip2._without_json_loads(msg1, data1)
        >>> msg1  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ProgressMessage(src=Node(uuid=PeerUUID('11111111-...-8c4ce6a2aed9')),
            dst=Host(uuid=PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d')),
            uuid=MessageUUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f'),
            status_code=1
            [
                chunks_by_uuid={ChunkUUID('940f0711-52d7-...-818580f432dc'):
                        ChunkInfo(uuid=ChunkUUID('940f0711-...-818580f432dc'),
                            maxsize_code=0,
                            hash=unhexlify('30313233343...373031323334353637'),
                            size=143941, crc32=0x7E5CE7AD),
                    ChunkUUID('a5b605f2-6ea5-...-d217b7e8e784'):
                        ChunkInfo(uuid=ChunkUUID('a5b605f2-...-d217b7e8e784'),
                            maxsize_code=1,
                            hash=unhexlify('37363534333...303736353433323130'),
                            size=2097152, crc32=0xDCC847D8),
                    ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'):
                        ChunkInfo(uuid=ChunkUUID('5b237ceb-...-6331cb14b5b4'),
                            maxsize_code=1,
                            hash=unhexlify('61626364656...686162636465666768'),
                            size=2097152, crc32=0x07FD7A5B)}
            ])
        """
        # H2N

        # Parse dataset
        if 'dataset' in body:
            self.completion = False
            self.dataset = DatasetOnChunks.from_json(body['dataset'])()
        elif 'dataset_completed' in body:
            self.completion = True
            self.dataset = \
                DatasetOnChunks.from_json(body['dataset_completed'])()

        # Parse chunks_by_uuid
        chunks_unp = body.get('chunks', None)
        if chunks_unp is not None:
            _chunks = (ChunkInfo.from_json(ch)() for ch in chunks_unp)
            self.chunks_by_uuid = {ch.uuid: ch
                                       for ch in _chunks}

        # Parse host_chunks_map
        host_chunks_map_unp = body.get('host_chunks_map', None)
        if host_chunks_map_unp is not None:
            host_chunks_map = \
                {Host(uuid=UUID(uuid)):
                         [ProgressNotificationPerHost.from_json(pr)()
                              for pr in per_host_notifs]
                     for uuid, per_host_notifs
                         in host_chunks_map_unp.iteritems()}
            self.chunks_map_getter = lambda: host_chunks_map

        # Parse blocks_map
        blocks_map_unp = body.get('blocks_map', None)
        if blocks_map_unp is not None:
            self.blocks_map = [Chunk.Block.from_json(bl)()
                                   for bl in blocks_map_unp]
