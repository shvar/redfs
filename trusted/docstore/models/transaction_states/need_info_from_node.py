#!/usr/bin/python
"""
NEED_INFO_FROM_NODE transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
from functools import partial
from types import NoneType

from ..types import ChunkInfo as model_ChunkInfo

from ._common import NodeTransactionState



#
# Classes
#

class NeedInfoFromNodeTransactionState_Node(NodeTransactionState):
    """The state for the NEED_INFO_FROM_NODE transaction on the Node."""

    __slots__ = ('query',)

    name = 'NEED_INFO_FROM_NODE'

    bson_schema = {
        'query': (NoneType, dict),
    }


    def __init__(self, query=None, *args, **kwargs):
        r"""Constructor.

        >>> from datetime import datetime
        >>> from uuid import UUID
        >>> # No optional arguments
        >>> NeedInfoFromNodeTransactionState_Node(
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))
        >>> # With a query
        >>> NeedInfoFromNodeTransactionState_Node(
        ...     query={u'from': u'cloud_stats',
        ...            u'select': [u'total_hosts_count', u'alive_hosts_count',
        ...                        u'total_mb', u'used_mb']},
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            query={u'from': u'cloud_stats',
                   u'select': [u'total_hosts_count', u'alive_hosts_count',
                               u'total_mb', u'used_mb']})
        """
        super(NeedInfoFromNodeTransactionState_Node, self).__init__(*args,
                                                                    **kwargs)
        self.query = query

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        # self.query can be rendered in a too huge string, unfortunately :(
        return u'{super}{opt_query}'.format(
                   super=super(NeedInfoFromNodeTransactionState_Node, self)
                             .__str__(),
                   opt_query=u', query={!r}'.format(self.query)
                                 if self.query is not None
                                 else '')


    def to_bson(self):
        r"""
        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> # Test with no arguments
        >>> NeedInfoFromNodeTransactionState_Node(
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'query': None}

        >>> # Test only querying host counts here.
        >>> NeedInfoFromNodeTransactionState_Node(
        ...     query={u'select': ['total_hosts_count',
        ...                        'alive_hosts_count',
        ...                        'total_mb', 'used_mb'],
        ...            u'from': u'cloud_stats'},
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'query': {u'from': u'cloud_stats',
                   u'select': ['total_hosts_count', 'alive_hosts_count',
                               'total_mb', 'used_mb']}}
        """
        cls = self.__class__

        doc = super(NeedInfoFromNodeTransactionState_Node, self).to_bson()
        # Mandatory

        if self.query is None:
            query_bson = None

        else:
            _from = self.query['from']
            if _from == 'chunks':
                # Copy and modify
                query_bson = dict(self.query)
                new_where = dict(self.query['where'])

                chunks_orig = self.query['where']['["hash", "size", "uuid"]']

                new_where['["hash", "size", "uuid"]'] = \
                    [model_ChunkInfo.from_common(c).to_bson()
                         for c in chunks_orig]

                query_bson['where'] = new_where

            elif _from == 'cloud_stats':
                query_bson = self.query

            else:
                raise NotImplementedError(self.query['from'])

        doc.update({'query': query_bson})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> from datetime import datetime
        >>> from uuid import UUID

        >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
        >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
        >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
        >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')

        >>> # Deserialize with no aruments
        >>> NeedInfoFromNodeTransactionState_Node.from_bson(
        ...     {'query': None}
        ... )(tr_start_time=tr_start_time,
        ...   tr_uuid=tr_uuid,
        ...   tr_src_uuid=tr_src_uuid,
        ...   tr_dst_uuid=tr_dst_uuid
        ... )# doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))

        >>> # Deserialize with host counts here.
        >>> NeedInfoFromNodeTransactionState_Node.from_bson({
        ...     'query': {'select': ['total_hosts_count', 'alive_hosts_count',
        ...                          'total_mb', 'used_mb'],
        ...               'from': 'cloud_stats'}
        ... })(tr_start_time=tr_start_time,
        ...    tr_uuid=tr_uuid,
        ...    tr_src_uuid=tr_src_uuid,
        ...    tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            query={'from': 'cloud_stats',
                   'select': ['total_hosts_count', 'alive_hosts_count',
                              'total_mb', 'used_mb']})
        """
        assert cls.validate_schema(doc), repr(doc)

        query_raw = doc['query']

        if query_raw is None:
            new_query = None
        else:
            _from = query_raw['from']
            if _from == 'chunks':
                # Copy and modify
                new_query = dict(query_raw)
                new_where = dict(query_raw['where'])

                chunks_orig = query_raw['where']['["hash", "size", "uuid"]']

                new_where['["hash", "size", "uuid"]'] = \
                    [model_ChunkInfo.from_bson(c)()
                         for c in chunks_orig]

                new_query['where'] = new_where

            elif _from == 'cloud_stats':
                new_query = query_raw

            else:
                raise NotImplementedError(query_raw['from'])

        return partial(super(NeedInfoFromNodeTransactionState_Node, cls)
                           .from_bson(doc),
                         # Mandatory
                       query=new_query)



if __debug__:
    __test__ = {
        'serialize-deserialize-selecting-chunks':
            r"""
            >>> from datetime import datetime
            >>> from uuid import UUID
            >>> from common.chunks import ChunkInfo
            >>> from common.typed_uuids import ChunkUUID

            >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
            >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
            >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
            >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
            >>> u1, u2, u3 = \
            ...     (ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
            ...      ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
            ...      ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'))

            >>> # Create
            >>> st = NeedInfoFromNodeTransactionState_Node(
            ...          query={'select': ['chunks.uuid', 'uuid'],
            ...                 'from': 'chunks',
            ...                 'where': {'["hash", "size", "uuid"]': [
            ...                               ChunkInfo(crc32=0x07FD7A5B,
            ...                                         uuid=u1,
            ...                                         maxsize_code=1,
            ...                                         hash='abcdefgh' * 8,
            ...                                         size=2097152),
            ...                               ChunkInfo(crc32=0x7E5CE7AD,
            ...                                         uuid=u2,
            ...                                         maxsize_code=0,
            ...                                         hash='01234567' * 8,
            ...                                         size=143941),
            ...                               ChunkInfo(crc32=0xDCC847D8,
            ...                                         uuid=u3,
            ...                                         maxsize_code=1,
            ...                                         hash='76543210' * 8,
            ...                                         size=2097152)
            ...                           ]}},
            ...          tr_start_time=tr_start_time,
            ...          tr_uuid=tr_uuid,
            ...          tr_src_uuid=tr_src_uuid,
            ...          tr_dst_uuid=tr_dst_uuid
            ...      )
            >>> st  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                query={'where': {'["hash", "size", "uuid"]':
                                     [ChunkInfo(uuid=ChunkUUID('5b237ceb-...'),
                                                maxsize_code=1,
                                                hash=unhexlify('61626...6768'),
                                                size=2097152,
                                                crc32=0x07FD7A5B),
                                      ChunkInfo(uuid=ChunkUUID('940f0711-...'),
                                                maxsize_code=0,
                                                hash=unhexlify('30313...3637'),
                                                size=143941,
                                                crc32=0x7E5CE7AD),
                                      ChunkInfo(uuid=ChunkUUID('a5b605f2-...'),
                                                maxsize_code=1,
                                                hash=unhexlify('37363...3130'),
                                                size=2097152,
                                                crc32=0xDCC847D8)]},
                       'from': 'chunks',
                       'select': ['chunks.uuid', 'uuid']})

            >>> # Serialize to BSON
            >>> st.to_bson()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            {'query':
                {'from': 'chunks',
                 'where':
                     {'["hash", "size", "uuid"]':
                          [{'crc32': 134052443,
                            'maxsize_code': 1,
                            'hash': Binary('abcdefghabcdefgh...abcdefgh', 0),
                            'uuid': ChunkUUID('5b237ceb-...-6331cb14b5b4'),
                            'size': 2097152},
                           {'crc32': 2120017837,
                            'maxsize_code': 0,
                            'hash': Binary('0123456701234567...01234567', 0),
                            'uuid': ChunkUUID('940f0711-...-818580f432dc'),
                            'size': 143941},
                           {'crc32': 3704113112,
                            'maxsize_code': 1,
                            'hash': Binary('7654321076543210...76543210', 0),
                            'uuid': ChunkUUID('a5b605f2-...-d217b7e8e784'),
                            'size': 2097152}]},
                 'select': ['chunks.uuid', 'uuid']}}

            >>> # Deserialize from BSON
            >>> NeedInfoFromNodeTransactionState_Node.from_bson(
            ...     {'query': {'from': 'chunks',
            ...                'where': {'["hash", "size", "uuid"]':
            ...                              [{'crc32': 134052443,
            ...                                'maxsize_code': 1,
            ...                                'hash': 'abcdefgh' * 8,
            ...                                'uuid': u1,
            ...                                'size': 2097152},
            ...                               {'crc32': 2120017837,
            ...                                'maxsize_code': 0,
            ...                                'hash': '01234567' * 8,
            ...                                'uuid': u2,
            ...                                'size': 143941},
            ...                               {'crc32': 3704113112,
            ...                                'maxsize_code': 1,
            ...                                'hash': '76543210' * 8,
            ...                                'uuid': u3,
            ...                                'size': 2097152}]},
            ...                'select': ['chunks.uuid', 'uuid']}}
            ... )(tr_start_time=tr_start_time,
            ...   tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...   tr_src_uuid=tr_src_uuid,
            ...   tr_dst_uuid=tr_dst_uuid
            ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                query={'from': 'chunks',
                       'where':
                           {'["hash", "size", "uuid"]':
                                [models.types.ChunkInfo(uuid=ChunkUUID('5...'),
                                     maxsize_code=1,
                                     hash=unhexlify('6162636465...6465666768'),
                                     size=2097152,
                                     crc32=0x07FD7A5B),
                                 models.types.ChunkInfo(uuid=ChunkUUID('9...'),
                                     maxsize_code=0,
                                     hash=unhexlify('3031323334...3334353637'),
                                     size=143941,
                                     crc32=0x7E5CE7AD),
                                 models.types.ChunkInfo(uuid=ChunkUUID('a...'),
                                     maxsize_code=1,
                                     hash=unhexlify('3736353433...3433323130'),
                                     size=2097152,
                                     crc32=0xDCC847D8)]},
                       'select': ['chunks.uuid', 'uuid']})
            """,

        'serialize-deserialize-selecting-hosts':
            r"""
            >>> from datetime import datetime
            >>> from uuid import UUID

            >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
            >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
            >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
            >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')

            >>> # Create
            >>> st = NeedInfoFromNodeTransactionState_Node(
            ...          query={'select': ['total_hosts_count',
            ...                            'alive_hosts_count',
            ...                            'total_mb', 'used_mb'],
            ...                 'from': 'cloud_stats'},
            ...          tr_start_time=tr_start_time,
            ...          tr_uuid=tr_uuid,
            ...          tr_src_uuid=tr_src_uuid,
            ...          tr_dst_uuid=tr_dst_uuid)
            ...      )
            >>> st  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                query={'from': 'cloud_stats',
                       'select': ['total_hosts_count', 'alive_hosts_count',
                                  'total_mb', 'used_mb']})

            >>> # Serialize to BSON
            >>> st.to_bson()  # doctest:+NORMALIZE_WHITESPACE
            {'query': {'from': 'cloud_stats',
                       'select': ['total_hosts_count', 'alive_hosts_count',
                                  'total_mb', 'used_mb']}}

            >>> # Deserialize from BSON
            >>> NeedInfoFromNodeTransactionState_Node.from_bson(
            ...     {'query': {'select': ['total_hosts_count',
            ...                           'alive_hosts_count',
            ...                           'total_mb', 'used_mb'],
            ...                'from': 'cloud_stats'}}
            ... )(tr_start_time=tr_start_time,
            ...   tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...   tr_src_uuid=tr_src_uuid,
            ...   tr_dst_uuid=tr_dst_uuid
            ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            NeedInfoFromNode...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                query={'from': 'cloud_stats',
                       'select': ['total_hosts_count', 'alive_hosts_count',
                                  'total_mb', 'used_mb']})
            """
   }
