#!/usr/bin/python
"""
RECEIVE_CHUNKS transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from itertools import chain

from contrib.dbc import contract_epydoc

from common.abstractions import AbstractInhabitant, unpauses_incoming
from common.db import Queries
from common.inhabitants import Host
from common.typed_uuids import PeerUUID

from protocol import transactions

from ._host import AbstractHostTransaction
from host import db
from host.db import HostQueries



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ReceiveChunksTransaction_Host(transactions.ReceiveChunksTransaction,
                                    AbstractHostTransaction):
    """RECEIVE_CHUNKS transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the RECEIVE_CHUNKS transaction on the Host."""

        __slots__ = ()

        name = 'RECEIVE_CHUNKS'



    @unpauses_incoming
    def on_begin(self):
        """
        @todo: If we ever change the app.known_peers to use C{PeerBook} class,
        then it is the class who must control the DB storage,
        rather than the caller.
        """
        cls = self.__class__

        assert self.is_incoming(), repr(self)

        _known_peers = self.manager.app.known_peers
        _message = self.message

        # Add the expected hosts to the list of known peers.
        add_peers_to_db = []
        for exp_peer_uuid in chain(_message.expect_replication.iterkeys(),
                                   _message.expect_restore.iterkeys()):
            assert isinstance(exp_peer_uuid, PeerUUID), \
                   exp_peer_uuid

            if exp_peer_uuid not in _known_peers:
                # Need to add some peer, unknown before.
                peer_to_add = Host(uuid=exp_peer_uuid)
                _known_peers[exp_peer_uuid] = peer_to_add
                # Add to the DB as well, but later
                add_peers_to_db.append(peer_to_add)

        expect_host_chunk_pairs = \
            chain(cls._expect_mapping_as_list(_message.expect_replication,
                                              is_restore=False),
                  cls._expect_mapping_as_list(_message.expect_restore,
                                              is_restore=True))

        # Do we need to actually update the database? Do that, if yes.
        with db.RDB() as rdbw:
            if add_peers_to_db:
                logger.debug('Adding peers %r', add_peers_to_db)
                Queries.Inhabitants.set_peers(add_peers_to_db, rdbw)

            HostQueries.HostChunks.expect_chunks(expect_host_chunk_pairs, rdbw)


    @staticmethod
    @contract_epydoc
    def _expect_mapping_as_list(mapping, is_restore):
        r"""Flatten some expectation data to the plain list.

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

        >>> list(ReceiveChunksTransaction_Host
        ...          ._expect_mapping_as_list(mapping, False)
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [(PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
          ChunkInfo(uuid=ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
                    maxsize_code=1,
                    hash=unhexlify('616263646566676861...7686162636465666768'),
                    size=2097152, crc32=0x07FD7A5B),
          False),
         (PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
          ChunkInfo(uuid=ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
                    maxsize_code=0,
                    hash=unhexlify('303132333435363730...6373031323334353637'),
                    size=143941, crc32=0x7E5CE7AD),
          False),
         (PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
          ChunkInfo(uuid=ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
                    maxsize_code=1,
                    hash=unhexlify('373635343332313037...1303736353433323130'),
                    size=2097151, crc32=0xDCC847D8),
          False),
         (PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
          ChunkInfo(uuid=ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'),
                    maxsize_code=1,
                    hash=unhexlify('616263646162636461...3646162636461626364'),
                    size=73819, crc32=0x2A5FE875),
          False)]

        >>> list(ReceiveChunksTransaction_Host
        ...          ._expect_mapping_as_list(mapping, True)
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        [(PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
          ChunkInfo(uuid=ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
                    maxsize_code=1,
                    hash=unhexlify('616263646566676861...7686162636465666768'),
                    size=2097152, crc32=0x07FD7A5B),
          True),
         (PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
          ChunkInfo(uuid=ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
                    maxsize_code=0,
                    hash=unhexlify('303132333435363730...6373031323334353637'),
                    size=143941, crc32=0x7E5CE7AD),
          True),
         (PeerUUID('e96a073b-3cd0-49a6-b14a-1fb04c221a9c'),
          ChunkInfo(uuid=ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
                    maxsize_code=1,
                    hash=unhexlify('373635343332313037...1303736353433323130'),
                    size=2097151, crc32=0xDCC847D8),
          True),
         (PeerUUID('233ad9c2-268f-4506-ab0f-4c71461c5d88'),
          ChunkInfo(uuid=ChunkUUID('0a7064b3-bef6-45c0-9e82-e9f9a40dfcf3'),
                    maxsize_code=1,
                    hash=unhexlify('616263646162636461...3646162636461626364'),
                    size=73819, crc32=0x2A5FE875),
          True)]

        @type mapping: col.Mapping
        @param is_restore: whether a mapping is a restore-specific one.
        @type is_restore: bool

        @returns: an iterable over tuples of
            (sender host UUID,
             chunk information,
             flag whether the chunk is dedicated for restore).

        @rtype: col.Iterable
        """
        return ((h, c, is_restore)
                    for h, chunks in mapping.iteritems()
                    for c in chunks)


    @contract_epydoc
    def on_end(self):
        assert self.is_incoming(), repr(self)
        self.message_ack = self.message.reply()

        self.manager.post_message(self.message_ack)
