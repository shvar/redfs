#!/usr/bin/python
"""
SEND_CHUNKS transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from functools import partial

from contrib.dbc import consists_of, contract_epydoc

from common.abstractions import unpauses_incoming
from common.utils import exceptions_logged

from protocol import transactions

from host import settings

from ._host import AbstractHostTransaction



#
# Constants
#

logger = logging.getLogger(__name__)

# Whether we are allowed to send multiple chunks
# in a single CHUNKS transaction...
MERGE_CHUNKS_ON_SENDCHUNKS = True
# .. and how large (approximately) should be the CHUNKS message.
try:
    from host_magic import MAX_CHUNK_MESSAGE_SIZE  # pylint:disable=F0401
except ImportError:
    MAX_CHUNK_MESSAGE_SIZE = 16



#
# Classes
#

class SendChunksTransaction_Host(transactions.SendChunksTransaction,
                                 AbstractHostTransaction):
    """SEND_CHUNKS transaction on the Host.

    @note: self.chunks_map contains the working copy of remaining chunks
           (which may and will alter during the transaction),
           while self.message.chunks_map always contains the original data.
    """

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the SEND_CHUNKS transaction on the Host."""

        __slots__ = ('chunks_map',)

        name = 'SEND_CHUNKS'


        def __init__(self, *args, **kwargs):
            super(SendChunksTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.chunks_map = None


        def __str__(self):
            return u'chunks_map={self.chunks_map!r}'.format(self=self)



    @unpauses_incoming
    def on_begin(self):
        assert self.is_incoming(), repr(self)

        with self.open_state(for_update=True) as state:
            state.chunks_map = \
                {k: list(v)
                     for k, v in self.message.chunks_map.iteritems()}

        self.__send_more_chunks()


    def __send_more_chunks(self):
        """
        Try sending some more chunks. If no more chunks can be sent out,
        unpause the transaction.
        """
        if not self.__try_send_more_chunks():
            self.complete_transaction()


    def __try_send_more_chunks(self):
        """
        Check whether we have pending chunks to upload,
        and upload them if needed.

        @returns: Whether any more chunks were uploaded;
                  i.e. returns False, if no more chunks.
        @rtype: bool
        """
        with self.open_state(for_update=True) as state:
            _chunks_map = state.chunks_map

            if not _chunks_map:
                return False
            else:
                _message = self.message
                _known_peers = self.manager.app.known_peers

                logger.verbose('Trying to send %i chunks to %i hosts: %r',
                               sum(len(ch) for ch in _chunks_map.itervalues()),
                               len(_chunks_map),
                               _chunks_map)
                # Find all chunks for the target host.
                target_host = _chunks_map.keys()[0]
                if target_host.uuid not in _known_peers:
                    _known_peers[target_host.uuid] = target_host

                chunks_for_host = _chunks_map[target_host]

                chunks_to_send = []

                add_more = True
                while add_more:
                    # Find the next chunk for the host.
                    assert (chunks_for_host and
                            isinstance(chunks_for_host, list)), \
                           repr(chunks_for_host)
                    next_chunk = chunks_for_host.pop()

                    # Cleanup the _chunks_map if needed.
                    if not chunks_for_host:
                        del _chunks_map[target_host]

                    _chunk = self.manager.app.chunk_storage.read_chunk(
                                 next_chunk.uuid,
                                 next_chunk.crc32,
                                 next_chunk.size())
                    chunks_to_send.append(_chunk)
                    logger.debug('Sending chunk %s/%r from %r to %r',
                                 next_chunk.uuid, _chunk,
                                 _message.dst, target_host)

                    # In what cases do we need to stop forming
                    # the CHUNKS message?
                    _current_total_size = sum(c.size() for c in chunks_to_send)
                    add_more = \
                        chunks_for_host and \
                        MERGE_CHUNKS_ON_SENDCHUNKS and \
                        _current_total_size < MAX_CHUNK_MESSAGE_SIZE * 0x100000

        # We've chosen what chunks to send; leaving the state context.

        ch_tr = self.manager.create_new_transaction(name='CHUNKS',
                                                    src=_message.dst,
                                                    dst=target_host,
                                                    parent=self,
                                                    # CHUNKS-specific
                                                    chunks=chunks_to_send)

        # Wait until the body is received, and send BACKUP_ACK manually
        ch_tr.completed.addCallbacks(
            partial(logger.debug, 'Child CHUNKS completed! %r'),
            partial(logger.error, 'Child CHUNKS issue: %r'))


        @exceptions_logged(logger)
        def _send_more_chunks_on_completion(ignore):
            self.__send_more_chunks()


        ch_tr.completed.addBoth(_send_more_chunks_on_completion)

        return True


    @unpauses_incoming
    def complete_transaction(self):
        """
        Just complete the transaction, no matter of success or error.
        """
        pass


    def on_end(self):
        self.message_ack = self.message.reply()
        self.manager.post_message(self.message_ack)
