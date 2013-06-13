#!/usr/bin/python
"""NEED_INFO_FROM_NODE message implementation.

NEED_INFO_FROM_NODE message contains a request to the Node for various
information.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from contrib.dbc import consists_of

from common.abstractions import AbstractMessage
from common.chunks import Chunk, ChunkInfo, LocalFile
from common.datasets import DatasetWithAggrInfo
from common.path_ex import decode_posix_path, encode_posix_path
from common.typed_uuids import ChunkUUID

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class NeedInfoFromNodeMessage(AbstractMessage):
    """NEED_INFO_FROM_NODE message."""

    name = 'NEED_INFO_FROM_NODE'
    version = 0

    __slots__ = ('query', 'ack_result_type', 'ack_result')

    CHUNK_FIELDS = '["hash", "size", "uuid"]'


    def __init__(self, *args, **kwargs):
        super(NeedInfoFromNodeMessage, self).__init__(*args, **kwargs)
        self.query = None
        self.ack_result_type = None
        self.ack_result = None


    @bzip2
    @json_dumps
    def _get_body(self):
        cls = self.__class__

        # H2N
        assert isinstance(self.query, dict)
        # Maybe, substitute chunks to their serialized representation
        if 'where' in self.query:
            # Copy the dictionary, for not to modify the original
            query = dict(self.query)
            _old_where = query['where']

            _CHUNK_FIELDS = cls.CHUNK_FIELDS

            if _CHUNK_FIELDS in _old_where:
                # Some chunks-specific infos
                assert _old_where.keys() == [_CHUNK_FIELDS]

                # Fix the condition inline, with serialized chunks
                query['where'] = \
                    {_CHUNK_FIELDS: [c.to_json()
                                         for c in _old_where[_CHUNK_FIELDS]]}
            elif 'path' in _old_where:
                query['where'].update({
                    'path': encode_posix_path(_old_where['path']),
                    'rec': 1 if _old_where['rec'] else 0
                })

        else:
            query = self.query

        return {'query': query}


    @bunzip2
    @json_loads
    def _init_from_body(self, _body):
        cls = self.__class__

        # H2N
        assert isinstance(_body, dict), _body
        _query = _body['query']

        # Maybe, deserialize chunks
        if 'where' in _query:
            # Copy the dictionary, for not to modify the original
            query = dict(_query)
            _old_where = query['where']

            _CHUNK_FIELDS = cls.CHUNK_FIELDS

            if _CHUNK_FIELDS in _old_where:
                assert _old_where.keys() == [_CHUNK_FIELDS]
                # New condition, with deserialized chunks
                query['where'] = \
                    {_CHUNK_FIELDS: [ChunkInfo.from_json(c)()
                                         for c in _old_where[_CHUNK_FIELDS]]}
            elif 'path' in _old_where:
                query['where'].update({
                    'path': decode_posix_path(_old_where['path']),
                    'rec': bool(_old_where.get('rec', 0))
                })

        else:
            query = _query

        self.query = query


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # N2H
        _res_type = self.ack_result_type
        _result = self.ack_result

        if _res_type == 'datasets':
            result = {_res_type: [i.to_json() for i in _result]}
        elif _res_type == 'files':
            result = \
                {_res_type: {dir_name: [f.to_json() for f in dir_files]
                                 for dir_name, dir_files
                                     in _result.iteritems()}}
        elif _res_type == 'chunks':
            assert consists_of(_result.iterkeys(), UUID), repr(_result)
            assert consists_of(_result.itervalues(), UUID), repr(_result)
            result = {_res_type: {k.hex: v.hex
                                      for k, v in _result.iteritems()}}
        elif _res_type in ('cloud_stats', 'data_stats'):
            result = {_res_type: _result}
        else:
            raise NotImplementedError('Result type {!r} unsupported; '
                                          'value {!r}'
                                          .format(_res_type, _result))

        return result


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # N2H
        assert isinstance(body, dict), repr(body)
        assert len(body) == 1, repr(body)

        self.ack_result_type = _result_type = body.keys()[0]
        result_unp = body[_result_type]

        if _result_type == 'datasets':
            self.ack_result = [DatasetWithAggrInfo.from_json(r_unp)()
                                   for r_unp in result_unp]

        elif _result_type == 'files':
            self.ack_result = {dir_name: [LocalFile.from_json(f_unp)()
                                              for f_unp in files_unp]
                                   for dir_name, files_unp
                                       in result_unp.iteritems()}

        elif _result_type == 'chunks':
            self.ack_result = {UUID(k): UUID(v)
                                   for k, v in result_unp.iteritems()}

        elif _result_type in ('cloud_stats', 'data_stats'):
            self.ack_result = result_unp

        else:
            raise NotImplementedError('Result type {!r} not supported'
                                          .format(_result_type))
