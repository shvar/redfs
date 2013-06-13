#!/usr/bin/python
"""
NEED_INFO_FROM_NODE transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.abstractions import unpauses_incoming
from common.db import Queries
from common.path_ex import decode_posix_path
from common.typed_uuids import PeerUUID

from protocol import transactions

from trusted.docstore.models.transaction_states.need_info_from_node \
    import NeedInfoFromNodeTransactionState_Node

from trusted import db
from trusted.db import TrustedQueries

from ._node import AbstractNodeTransaction



#
# Constants
#

logger = logging.getLogger(__name__)

MAX_ATTEMPTS_TO_SAVE_CHUNKS = 5



#
# Classes
#

QueryResultData = col.namedtuple('QueryResultData', ('result_type', 'result'))


class NeedInfoFromNodeTransaction_Node(
          transactions.NeedInfoFromNodeTransaction,
          AbstractNodeTransaction):
    """NEED_INFO_FROM_NODE transaction on the Node."""

    __slots__ = ()

    State = NeedInfoFromNodeTransactionState_Node


    @unpauses_incoming
    def on_begin(self):
        assert self.is_incoming()
        with self.open_state(for_update=True) as state:
            state.query = self.message.query


    @contract_epydoc
    def __execute_incoming_query(self, host_uuid, query):
        """
        Given a somehow-resembling-SQL query in the dict form,
        return the result.

        @param host_uuid: the UUID of the host which requests the query
            to be executed.
        @type host_uuid: PeerUUID

        @type query: dict
        @precondition: 'select' in query and 'from' in query # query

        @returns: the result of the query, in form of final C{State} object.
        @rtype: QueryResultData

        @todo: For cloud_stats, cache the values.
        """
        logger.verbose('%r querying node: %r', self, query)

        res_type = query['from']

        # Execute the query, and put the result into the "result" variable.

        if res_type == 'datasets':
            assert 'where' not in query

            with db.RDB() as rdbw:
                result = list(Queries.Datasets.get_just_datasets(host_uuid,
                                                                 rdbw))

        elif res_type == 'files':
            assert 'where' in query, repr(query)
            where_cond = query['where']

            assert len(where_cond.keys()) == 1, repr(where_cond)
            assert 'dataset' in where_cond, repr(where_cond)
            where_dataset_uuid = UUID(where_cond['dataset'])

            with db.RDB() as rdbw:
                files_iter = \
                    Queries.Files.get_files_for_dataset(host_uuid,
                                                        where_dataset_uuid,
                                                        rdbw)

                result = {}
                for f in files_iter:
                    result.setdefault(f.base_dir, []).append(f)

        elif res_type == 'chunks':

            select_what = query['select']
            where_cond = query['where']

            expected_key = '["hash", "size", "uuid"]'

            assert set(select_what) == {'chunks.uuid', 'uuid'}, \
                   repr(select_what)
            assert where_cond.keys() == [expected_key], \
                   (where_cond, expected_key)

            attempt_count = 0
            successfully_saved = False
            while attempt_count < MAX_ATTEMPTS_TO_SAVE_CHUNKS and \
                  not successfully_saved:
                # Try to save chunks, but if saving fails
                # (due to parallel request completed before),
                # retry several times.
                attempt_count += 1
                try:
                    with db.RDB() as rdbw:
                        result = TrustedQueries.TrustedChunks \
                                               .save_chunks_and_get_duplicates(
                                                    where_cond[expected_key],
                                                    rdbw)
                except Exception as e:
                    if attempt_count < MAX_ATTEMPTS_TO_SAVE_CHUNKS:
                        logger.debug('Saving chunks failed on attempt %d, '
                                         'retrying',
                                     attempt_count)
                    else:
                        logger.exception('Could not save chunks during '
                                             '%d attempts',
                                         MAX_ATTEMPTS_TO_SAVE_CHUNKS)
                        raise
                else:
                    successfully_saved = True
                    logger.debug('Saved chunks in %d attempt(s)',
                                 attempt_count)

        elif res_type == 'cloud_stats':
            assert 'where' not in query, repr(query)

            select_what = query['select']

            assert set(select_what) == {'total_hosts_count',
                                        'alive_hosts_count',
                                        'total_mb',
                                        'used_mb'}, repr(select_what)

            _known_hosts = self.manager.app.known_hosts
            total_hosts_count = _known_hosts.peers_count()
            alive_hosts_count = _known_hosts.alive_peers_count()

            total_mb, used_mb = \
                TrustedQueries.TrustedChunks.get_cloud_sizes(
                    ignore_uuid=host_uuid)

            result = {'total_hosts_count': total_hosts_count,
                      'alive_hosts_count': alive_hosts_count,
                      'total_mb': total_mb,
                      'used_mb': used_mb}

        elif res_type == 'data_stats':
            assert 'where' in query, repr(query)

            select_what = query['select']
            where_cond = query['where']

            assert set(select_what) == {'file_count',
                                        'file_size',
                                        'uniq_file_count',
                                        'uniq_file_size',
                                        'full_replicas_count',
                                        'chunk_count',
                                        'chunk_replicas_count',
                                        'hosts_count'}, \
                   repr(select_what)

            ds_uuid, path = (UUID(where_cond['dataset']),
                             decode_posix_path(where_cond['path']))

            result = TrustedQueries.TrustedChunks.get_data_stats(
                         ds_uuid=ds_uuid if ds_uuid != '*'
                                         else None,
                         path=path if path != '*'
                                   else None,
                         path_rec=where_cond['rec'] if path != '*'
                                                    else None)
        else:
            raise NotImplementedError('Unsupported result_type {}'
                                          .format(res_type))

        logger.verbose('For %r, result is %r (%r)',
                       query, result, res_type)

        return QueryResultData(result_type=res_type,
                               result=result)


    def on_end(self):
        assert self.is_incoming()

        _message = self.message_ack = self.message.reply()

        with self.open_state() as state:
            _query = state.query

        # We execute the actual query at the last possible moment (to get
        # the latest data), thus in on_end() rather than in on_begin().
        result_data = self.__execute_incoming_query(_message.dst.uuid, _query)

        _message.ack_result_type = result_data.result_type
        _message.ack_result = result_data.result

        self.manager.post_message(_message)
