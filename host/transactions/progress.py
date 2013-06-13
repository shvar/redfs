#!/usr/bin/python
"""
PROGRESS transaction implementation on the Host.

@todo: C{host_chunks_map_getter} MUST BE SERIALIZED!
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from types import NoneType

from contrib.dbc import consists_of, contract_epydoc

from common.abstract_dataset import AbstractDataset
from common.abstractions import pauses
from common.chunks import Chunk
from common.inhabitants import Host

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ProgressTransaction_Host(transactions.ProgressTransaction,
                               AbstractHostTransaction):
    """PROGRESS transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the PROGRESS transaction on the Host.

        @ivar chunks: the container of the chunks in the dataset
                      which was just uploaded.
        @type chunks: (set, frozenset)
        @invariant: consists_of(chunks, Chunk)
        """

        __slots__ = ('dataset', 'chunks', 'host_chunks_map_getter')

        name = 'PROGRESS'


        @contract_epydoc
        def __init__(self,
                     dataset=None, host_chunks_map_getter=None, chunks=None,
                     *args, **kwargs):
            """Constructor.

            @note: all arguments are optional and may exist or non-exist
            independently

            @todo: SERIALIZE OR AVOID C{host_chunks_map_getter}!

            @type dataset: NoneType, AbstractDataset
            @type host_chunks_map_getter: NoneType, col.Callable
            @type chunks: NoneType, col.Iterable

            @precondition: (dataset is not None or
                            host_chunks_map_getter is not None or
                            chunks is not None)
            """
            assert dataset is not None or \
                   host_chunks_map_getter is not None or \
                   chunks is not None, \
                   (dataset, host_chunks_map_getter, chunks)

            super(ProgressTransaction_Host.State, self) \
                .__init__(*args, **kwargs)

            self.dataset = dataset
            self.host_chunks_map_getter = host_chunks_map_getter
            self.chunks = list(chunks) if chunks is not None else None

            assert self.chunks is None or consists_of(self.chunks, Chunk), \
                   repr(self.chunks)


        def __str__(self):
            return (u'dataset={self.dataset!r}, '
                     'host_chunks_map_getter={self.host_chunks_map_getter!r}, '
                     'chunks={self.chunks!r}'
                         .format(self=self))



    @pauses
    @contract_epydoc
    def on_begin(self):
        """
        @precondition: self.is_outgoing()
        """
        _message = self.message

        with self.open_state() as state:
            _message.dataset = _dataset = state.dataset
            if state.host_chunks_map_getter is not None:
                _message.chunks_map_getter = state.host_chunks_map_getter
            if state.chunks is not None:
                _message.blocks_map = [(block, chunk)
                                           for chunk in state.chunks
                                           for block in chunk.blocks]

        if _dataset is not None:
            _message.completion = (_dataset.time_completed is not None)

        self.manager.post_message(self.message)


    @contract_epydoc
    def on_end(self):
        """
        @precondition: self.is_outgoing()
        """
        with self.open_state() as state:
            if state.host_chunks_map_getter is not None:
                if not self.failure:
                    state.host_chunks_map_getter.run_callbacks(True)
                else:
                    logger.debug('There was a problem with compound PROGRESS '
                                     '(running errbacks now): %r',
                                 self.failure)
                    state.host_chunks_map_getter.run_errbacks(self.failure)
