#!/usr/bin/python
"""
CHUNKS transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import binascii
import logging
from datetime import datetime
from itertools import izip

from twisted.internet import defer

from contrib.dbc import contract_epydoc, consists_of

from common.abstractions import pauses, unpauses_incoming
from common.chunks import AbstractChunkWithContents
from common.utils import coalesce, exceptions_logged, round_up_to_multiply

from protocol import transactions, messages

from host.db import HostQueries

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ChunksTransaction_Host(transactions.ChunksTransaction,
                             AbstractHostTransaction):
    """CHUNKS transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the CHUNKS transaction on the Host."""

        __slots__ = ('chunks', 'out_progress_deferred',
                     'incoming_success', 'outgoing_success')

        name = 'CHUNKS'


        def __init__(self, chunks=None,
                     incoming_success=False, outgoing_success=False,
                     *args, **kwargs):
            """Constructor.

            @type incoming_success: bool
            @type outgoing_success: bool
            """
            super(ChunksTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.chunks = chunks
            self.out_progress_deferred = None
            # Incoming-specific
            self.incoming_success = incoming_success
            # Outgoing-specific
            self.outgoing_success = outgoing_success


        def __str__(self):
            return u'chunks={self.chunks!r}, '\
                   u'incoming_success={self.incoming_success!r}, ' \
                   u'outgoing_success={self.outgoing_success!r}' \
                       .format(self=self)



    @pauses
    def on_begin(self):
        """
        Either send CHUNKS request to another Host (if C{self.is_outgoing()}),
        or receive CHUNKS request from another Host.
        """
        with self.open_state() as state:
            chunks = state.chunks

        logger.debug('Initializing CHUNKS transaction with %i chunks',
                     len(coalesce(chunks, [])))

        my_host = self.manager.app.host

        assert ((self.message.src == my_host and
                 consists_of(chunks, AbstractChunkWithContents)) or
                (self.message.dst == my_host and
                 chunks is None)), repr(self)

        if self.is_outgoing():
            logger.verbose('Going to send %i chunk(s): %r',
                           len(coalesce(chunks, [])),
                           [ch.uuid for ch in chunks])

            self.message.chunks = chunks
            self.manager.post_message(self.message)

        elif self.is_incoming():
            logger.debug('Going to receive the chunks')

            self.message.body_receiver.on_finish.addCallback(
                self._incoming_chunks_body_received)

            self.message.body_receiver.on_finish.addBoth(
                self._incoming_chunks_body_finish)

        else:
            self.neither_incoming_nor_outgoing()


    @exceptions_logged(logger)
    @contract_epydoc
    def _incoming_chunks_body_received(self, data):
        """
        @todo: What if the "since" field is too old?
        """
        assert self.is_incoming(), repr(self)
        sender = self.message.src

        chunks = self.message.chunks
        _chunk_lengths_iter = (ch.phys_size() for ch in chunks)

        logger.verbose('Received %i chunk(s) from %r: %r',
                       len(chunks) if chunks else 0,
                       sender,
                       chunks)

        # Check if the chunks were expected...
        exp_chunk_tuples = [HostQueries.HostChunks
                                       .get_expected_chunk(_chunk.uuid,
                                                           sender.uuid)
                                for _chunk in chunks]

        byte_size_to_wipe = 0
        for _chunk_len, exp_chunk_tuple in izip(_chunk_lengths_iter,
                                                exp_chunk_tuples):
            doing_replication_not_restore = exp_chunk_tuple is None or \
                                            not exp_chunk_tuple.restore_flag

            # But if it was the dummy replication chunk,
            # we need to free some space by killing some dummy chunks.
            if doing_replication_not_restore:
                byte_size_to_wipe += _chunk_len

        # Now really cleanup some dummy chunks
        if byte_size_to_wipe != 0:
            how_many_chunks_to_wipe = \
                round_up_to_multiply(byte_size_to_wipe, 0x100000) // 0x100000
            logger.debug('Before writing chunks body: wiping %i chunks',
                         how_many_chunks_to_wipe)
            self.manager.app.chunk_storage.delete_some_dummy_chunks(
                how_many_chunks_to_wipe)

        # Let's consider it successful for the phase of chunks writing,
        # unless even a single chunk fails to write.
        overall_success = True

        # And finally, loop over the chunks and write their bodies.
        for _chunk, exp_chunk_tuple in izip(chunks, exp_chunk_tuples):
            _chunk_uuid = _chunk.uuid

            _expected_hash = _chunk.hash
            _actual_hash = _chunk.hash_from_body
            if _expected_hash != _actual_hash:
                logger.error('Chunk %s has hash %s rather than %s; '
                                 'writing this for now, but someday '
                                 'might need to fix it to prevent '
                                 'the cloud/deduplication clogging '
                                 'with the unreadable files.',
                             _chunk_uuid,
                             binascii.hexlify(_actual_hash),
                             binascii.hexlify(_expected_hash))

            logger.debug('Writing chunk %r', _chunk)
            try:
                self.manager.app.chunk_storage.write_chunk(_chunk,
                                                           is_dummy=False)
            except Exception:
                logger.exception('Could not store the incoming chunk %r; '
                                     'will be considered as failed',
                                 _chunk)
                overall_success = False
            else:  # written successfully
                if exp_chunk_tuple is not None:
                    # This is an expected chunk, either replication
                    # (restore_flag = False)
                    # or restore-related (restore_flag = True) one.
                    logger.debug('Received unexpected chunk %r from %r, '
                                     'meeting expectation (%s)',
                                 _chunk_uuid,
                                 sender.uuid,
                                 exp_chunk_tuple.restore_flag)
                    HostQueries.HostChunks.meet_expectations(
                        _chunk_uuid, sender.uuid, exp_chunk_tuple.restore_flag)
                else:
                    # This is an unexpected chunk,
                    # assume it is a replication
                    logger.debug('Received unexpected chunk %r from %r, '
                                     'assume replication',
                                 _chunk_uuid, sender)

        # If something failed before this moment, the default value of
        # C{incoming_success} of False will consider the transaction failed.
        with self.open_state(for_update=True) as state:
            state.incoming_success = overall_success
            logger.debug('Overall success of %r is %r', self, overall_success)

        # At the very end, regenerate back the dummy chunks, if needed.
        logger.debug('After writing chunk body: recalculating dummy chunks')
        self.manager.app.chunk_storage.update_dummy_chunks_size()


    @unpauses_incoming
    @exceptions_logged(logger)
    def _incoming_chunks_body_finish(self, data):
        """
        Handle finish with incoming chunks, no matter if successful or failed.
        """
        pass


    @defer.inlineCallbacks
    @exceptions_logged(logger)
    def on_end(self):
        """
        Either send CHUNKS response to another Host (if C{self.is_outgoing()}),
        or receive CHUNKS response from another Host.
        """
        yield defer.succeed(None)  # to satisfy @inlineCallbacks

        _app = self.manager.app
        _now = datetime.utcnow()
        _duration = _now - self.start_time

        if self.is_outgoing():

            outgoing_success = \
                (self.message_ack is not None and
                 messages.ChunksMessage.StatusCodes.is_good(
                     self.message_ack.status_code))
            logger.debug('%r completed; success? %r', self, outgoing_success)

            with self.open_state(for_update=True) as state:
                state.outgoing_success = outgoing_success

                if outgoing_success:
                    uploaded_chunks = state.chunks

                    logger.debug('...was outgoing')
                    logger.verbose('Just sent out the chunks to %r: %r',
                                   self.message.dst, uploaded_chunks)

                    # Notify the node about the new chunk uploaded
                    d = state.out_progress_deferred \
                      = _app.progress_notificator.add(
                            chunks=uploaded_chunks, dst_peer=self.message.dst,
                            end_ts=_now, duration=_duration)
                    assert isinstance(d, defer.Deferred), repr(d)
                    # This Deferred object fires when the data above is
                    # sent to the host; .callback() is fired if it
                    # was sent successfully, .errback() otherwise.

            # Getting out of the state context, saving state.
            if outgoing_success:
                logger.debug('Sending progress from %r: %r', self, d)
                try:
                    # yield result is a kindof progress_transaction
                    # and is unused
                    yield d  # inlineCallbacks
                    logger.debug('Reported to the Node about %i '
                                     'uploaded chunks successfully in %r',
                                 len(uploaded_chunks), d)
                except Exception as e:
                    logger.exception('Chunks reporting issue with %r',
                                     uploaded_chunks)
            else:
                logger.error('%r (outgoing) has been failed', self)

        elif self.is_incoming():
            logger.debug('...was incoming')

            with self.open_state() as state:
                logger.verbose('Just received the chunks from %r: %r',
                               self.message.src, state.chunks)
                _incoming_success = state.incoming_success

            statuscodes_cls = messages.ChunksMessage.StatusCodes

            self.message_ack = self.message.reply()
            self.message_ack.status_code = \
                statuscodes_cls.OK if _incoming_success \
                                   else statuscodes_cls.GENERAL_FAILURE
            self.manager.post_message(self.message_ack)

        else:
            self.neither_incoming_nor_outgoing()
