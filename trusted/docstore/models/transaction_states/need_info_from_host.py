#!/usr/bin/python
"""
NEED_INFO_FROM_HOST transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
from functools import partial

from ._common import NodeTransactionState



#
# Classes
#

class NeedInfoFromHostTransactionState_Node(NodeTransactionState):
    """The state for the NEED_INFO_FROM_HOST transaction on the Node."""

    __slots__ = ('query', 'ack_result')

    name = 'NEED_INFO_FROM_HOST'


    def __init__(self, query, ack_result=None, *args, **kwargs):
        super(NeedInfoFromHostTransactionState_Node, self).__init__(*args,
                                                                    **kwargs)
        self.query = query
        # Result
        self.ack_result = ack_result

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return u'query={self.query!r}'.format(self=self)


    def to_bson(self):
        cls = self.__class__

        doc = super(NeedInfoFromHostTransactionState_Node, self).to_bson()
        # Mandatory
        doc.update({'query': self.query})
        # Optional
        if self.ack_result is not None:
            doc.update({'ack_result': self.ack_result})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(NeedInfoFromHostTransactionState_Node, cls)
                           .from_bson(doc),
                       # Mandatory
                       query=doc['query'],
                       # Optional
                       ack_result=doc.get('ack_result'))
