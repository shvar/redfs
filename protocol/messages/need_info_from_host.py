#!/usr/bin/python
"""NEED_INFO_FROM_HOST message implementation.

NEED_INFO_FROM_HOST message contains a request to the Host for various
information.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from contrib.dbc import consists_of

from common.abstractions import AbstractMessage

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class NeedInfoFromHostMessage(AbstractMessage):
    """NEED_INFO_FROM_HOST message."""

    name = 'NEED_INFO_FROM_HOST'
    version = 0

    __slots__ = ('query', 'ack_result')


    def __init__(self, *args, **kwargs):
        super(NeedInfoFromHostMessage, self).__init__(*args, **kwargs)
        self.query = None
        self.ack_result = None


    @bzip2
    @json_dumps
    def _get_body(self):
        # N2H
        assert isinstance(self.query, list), repr(self.query)

        assert all(q['select'] == 'uuid' and
                   q['from'] in ('chunks', 'datasets')
                       for q in self.query), \
               repr(self.query)

        return {'query': self.query}


    @bunzip2
    @json_loads
    def _init_from_body(self, _body):
        # N2H
        assert isinstance(_body, dict), _body
        _query = _body['query']

        assert isinstance(_query, list), repr(_query)
        assert all(q['select'] == 'uuid' and
                   q['from'] in ('chunks', 'datasets')
                       for q in _query), \
               repr(_query)

        self.query = _query


    def __assert_invariants(self):
        for _res_type, _res in self.ack_result.iteritems():
            if _res_type == 'chunks::uuid':
                assert all(k in ('fs', 'fs:own', 'db', 'db:own')
                               for k in _res.iterkeys()), \
                       repr(_res)
                assert (consists_of(_res['fs'], UUID) and
                        consists_of(_res['fs:own'], UUID) and
                        consists_of(_res['db'], UUID) and
                        consists_of(_res['db:own'], UUID)), \
                       repr(_res)
                assert all(consists_of(v, UUID)
                               for v in _res.itervalues()), \
                       repr(_res)

            elif _res_type == 'datasets::uuid':
                assert consists_of(_res, UUID), repr(_res)

            else:
                assert False, repr(_res_type)


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # H2N
        _total_result = {}

        if __debug__:
            self.__assert_invariants()

        for _res_type, _res in self.ack_result.iteritems():
            if _res_type == 'chunks::uuid':
                # TODO: what about restore chunks?
                _total_result[_res_type] = {k: [u.hex for u in v]
                                                for k, v in _res.iteritems()}


            elif _res_type == 'datasets::uuid':
                _total_result[_res_type] = [u.hex for u in _res]

            else:
                raise NotImplementedError('Result type {!r} unsupported; '
                                              'value {!r}'
                                              .format(_res_type, _res))

        return _total_result


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # H2N
        _result = {}

        for _res_type, _raw_res in body.iteritems():
            if _res_type == 'chunks::uuid':
                _result[_res_type] = {k: map(UUID, v)
                                          for k, v in _raw_res.iteritems()}

            elif _res_type == 'datasets::uuid':
                _result[_res_type] = map(UUID, _raw_res)

            else:
                raise NotImplementedError('Result type {!r} not supported'
                                              .format(_res_type))
        self.ack_result = _result

        if __debug__:
            self.__assert_invariants()
