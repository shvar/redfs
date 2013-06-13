#!/usr/bin/python
"""
HEARTBEAT transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
from datetime import datetime
from functools import partial
from types import NoneType

from ._common import NodeTransactionState



#
# Classes
#

class HeartbeatTransactionState_Node(NodeTransactionState):
    """The state for the HEARTBEAT transaction on the Node."""

    __slots__ = ('last_update_time',)

    name = 'HEARTBEAT'

    bson_schema = {
        'last_update_time': (datetime, NoneType)
    }


    def __init__(self, last_update_time=None, *args, **kwargs):
        super(HeartbeatTransactionState_Node, self) \
            .__init__(*args, **kwargs)
        self.last_update_time = last_update_time

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return u'last_update_time={self.last_update_time!r}' \
                   .format(self=self)


    def to_bson(self):
        cls = self.__class__

        doc = super(HeartbeatTransactionState_Node, self).to_bson()
        # Mandatory fields
        doc.update({'last_update_time': self.last_update_time})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(HeartbeatTransactionState_Node, cls)
                           .from_bson(doc),
                         # Mandatory
                       last_update_time=doc['last_update_time'])
