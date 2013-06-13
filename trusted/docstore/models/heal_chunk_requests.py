#!/usr/bin/python
"""The FastDB model for the request to heal some chunk."""

#
# Imports
#

from __future__ import absolute_import
from datetime import datetime
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.typed_uuids import ChunkUUID

from trusted.ibsonable import IBSONable

from .idocstoredocument import IDocStoreTopDocument



#
# Classes
#

class HealChunkRequest(IDocStoreTopDocument, IBSONable):
    """A request to heal some chunks."""

    __slots__ = (
        'chunk_uuid', 'state', 'last_update_ts'
    )

    bson_schema = {
        'chunk_uuid': UUID,
        'state': basestring,
        'last_update_ts': datetime,
    }


    def __init__(self, chunk_uuid, state, last_update_ts,
                 *args, **kwargs):
        """Constructor.

        @param chunk_uuid: the UUID of the transaction.
        @type chunk_uuid: ChunkUUID

        @param state: the state of the task. May be C{'rth'} or C{'hea'}.
        @type state: basestring
        @precondition: state in ('rth', 'hea')

        @param last_update_ts: the time of last state update.
        @type last_update_ts: datetime
        """
        super(HealChunkRequest, self).__init__(*args, **kwargs)
        self.chunk_uuid = chunk_uuid
        self.state = state
        self.last_update_ts = last_update_ts


    def __str__(self):
        return u'{super}, ' \
               u'chunk_uuid={self.chunk_uuid!r}, ' \
               u'state={self.state!r}, ' \
               u'last_update_ts={self.last_update_ts!r}'.format(
                    super=super(HealChunkRequest, self).__str__(),
                    self=self)


    def to_bson(self):
        r"""
        >>> HealChunkRequest(
        ...     chunk_uuid=ChunkUUID(int=42),
        ...     state='hea',
        ...     last_update_ts=datetime(2012, 1, 2, 3, 4, 5, 6)
        ... ).to_bson() # doctest:+NORMALIZE_WHITESPACE
        {'chunk_uuid': ChunkUUID('00000000-0000-0000-0000-00000000002a'),
         'state': 'hea',
         'last_update_ts': datetime.datetime(2012, 1, 2, 3, 4, 5, 6)}

        >>> import bson
        >>> HealChunkRequest(
        ...     chunk_uuid=ChunkUUID(int=42),
        ...     state='hea',
        ...     last_update_ts=datetime(2012, 1, 2, 3, 4, 5, 6),
        ...     _id=bson.objectid.ObjectId('400b1e67d18ab62e36000000')
        ... ).to_bson() # doctest:+NORMALIZE_WHITESPACE
        {'chunk_uuid': ChunkUUID('00000000-0000-0000-0000-00000000002a'),
         'state': 'hea', '_id': ObjectId('400b1e67d18ab62e36000000'),
         'last_update_ts': datetime.datetime(2012, 1, 2, 3, 4, 5, 6)}
        """
        cls = self.__class__

        doc = super(HealChunkRequest, self).to_bson()

        doc.update({
            'chunk_uuid': self.chunk_uuid,
            'state': self.state,
            'last_update_ts': self.last_update_ts,
        })

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> HealChunkRequest.from_bson({
        ...     'chunk_uuid': UUID('00000000-0000-0000-0000-00000000002a'),
        ...     'state': 'hea',
        ...     'last_update_ts': datetime(2012, 1, 2, 3, 4, 5, 6)
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        HealChunkRequest(_id=None,
            chunk_uuid=ChunkUUID('00000000-0000-0000-0000-00000000002a'),
            state='hea',
            last_update_ts=datetime.datetime(2012, 1, 2, 3, 4, 5, 6))

        >>> import bson
        >>> HealChunkRequest.from_bson({
        ...     'chunk_uuid': UUID('00000000-0000-0000-0000-00000000002a'),
        ...     'state': 'hea',
        ...     '_id': bson.objectid.ObjectId('400b1e67d18ab62e36000000'),
        ...     'last_update_ts': datetime(2012, 1, 2, 3, 4, 5, 6)
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        HealChunkRequest(_id=ObjectId('400b1e67d18ab62e36000000'),
            chunk_uuid=ChunkUUID('00000000-0000-0000-0000-00000000002a'),
            state='hea',
            last_update_ts=datetime.datetime(2012, 1, 2, 3, 4, 5, 6))
        """
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(HealChunkRequest, cls).from_bson(doc),
                       chunk_uuid=ChunkUUID.safe_cast_uuid(doc['chunk_uuid']),
                       state=doc['state'],
                       last_update_ts=doc['last_update_ts'])
