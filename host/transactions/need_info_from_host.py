#!/usr/bin/python
"""
NEED_INFO_FROM_HOST transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging

from contrib.dbc import contract_epydoc

from common.abstractions import unpauses_incoming

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class NeedInfoFromHostTransaction_Host(
          transactions.NeedInfoFromHostTransaction, AbstractHostTransaction):
    """NEED_INFO_FROM_HOST transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the NEED_INFO_FROM_HOST transaction on the Host."""

        __slots__ = ()

        name = 'NEED_INFO_FROM_HOST'



    @unpauses_incoming
    def on_begin(self):
        """Receive NEED_INFO_FROM_HOST request from the Node."""
        assert self.is_incoming(), repr(self)


    @contract_epydoc
    def __execute_single_query(self, query):
        """Execute a query for a single result type.

        @type query: col.Mapping

        @returns: a tuple of ("type", "value") of the result
            of a single query execution.
        @rtype: tuple
        """
        _from_type = query['from']

        if _from_type == 'chunks':
            assert query['select'] == 'uuid', repr(query)

            # TODO: implement calculation of "fs:own", "db" and "db:own"
            # chunks.
            # "fs:own" is a future-proof extension; it may be used
            # to find out the restore chunks,
            # but should not be, cause restore dataset may not yet be written
            # into the DB.
            return ('chunks::uuid',
                    {'fs': list(self.manager
                                    .app.chunk_storage
                                    .get_real_stored_chunk_uuids()),
                     'fs:own': [],
                     'db': [],
                     'db:own': []})

        elif _from_type == 'datasets':
            assert query['select'] == 'uuid', repr(query)

            # TODO: implement!
            return ('datasets::uuid', [])

        else:
            raise NotImplementedError('Unsupported from_type {}'
                                          .format(_from_type))


    @contract_epydoc
    def execute_incoming_query(self, query):
        """
        Execute an query from the incoming message (possibly with multiple
        results requested).

        Given a somehow-resembling-SQL query in the mapping form,
        return the result types and the result (as a mapping from the result
        type to the result).

        @type query: col.Iterable
        """
        logger.debug('%r querying host: %r', self, query)

        result_tuples = (self.__execute_single_query(q)
                             for q in query)
        return {_type: _value
                    for _type, _value in result_tuples}


    @contract_epydoc
    def on_end(self):
        """Send NEED_INFO_FROM_HOST response to the Node."""
        assert self.is_incoming(), repr(self)

        self.message_ack = self.message.reply()
        self.message_ack.ack_result = \
            self.execute_incoming_query(self.message.query)

        self.manager.post_message(self.message_ack)
