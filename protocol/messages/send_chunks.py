#!/usr/bin/python
"""SEND_CHUNKS message implementation.

SEND_CHUNKS message contains a command to host to send some chunks
to some hosts.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from contrib.dbc import consists_of

from common.abstractions import AbstractMessage
from common.chunks import Chunk, ChunkInfo
from common.inhabitants import Host
from common.typed_uuids import HostUUID

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class SendChunksMessage(AbstractMessage):
    """SEND_CHUNKS message.

    @note: it loses the original name of the host, and leaves just the UUID
        and the host URLs.
    """

    name = 'SEND_CHUNKS'
    version = 0

    __slots__ = ('chunks_map',)


    def __init__(self, *args, **kwargs):
        super(SendChunksMessage, self).__init__(*args, **kwargs)
        self.chunks_map = {}


    @bzip2
    @json_dumps
    def _get_body(self):
        assert consists_of(self.chunks_map.iterkeys(), Host), \
               repr(self.chunks_map)
        assert consists_of((ch
                                for ch_list in self.chunks_map.itervalues()
                                for ch in ch_list),
                           Chunk), \
               repr(self.chunks_map)

        chunks_data = {host.uuid.hex: {'urls': host.urls,
                                       'chunks': [c.to_json() for c in chunks]}
                           for host, chunks in self.chunks_map.iteritems()}
        return {'chunks': chunks_data}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # N2H
        chunks_data = body['chunks']

        self.chunks_map = \
            {Host(uuid=HostUUID(host_uuid), urls=per_host_data['urls']):
                     [ChunkInfo.from_json(ch)()
                          for ch in per_host_data['chunks']]
                 for host_uuid, per_host_data
                     in chunks_data.iteritems()}
