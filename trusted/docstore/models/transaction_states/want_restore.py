#!/usr/bin/python
"""
WANT_RESTORE transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
from functools import partial

from protocol.messages import WantRestoreMessage as WR_Msg

from ._common import NodeTransactionState



#
# Classes
#

class WantRestoreTransactionState_Node(NodeTransactionState):
    """The state for the WANT_RESTORE transaction on the Node."""

    __slots__ = ('ack_result_code',)

    name = 'WANT_RESTORE'


    def __init__(self,
                 ack_result_code=WR_Msg.ResultCodes.GENERAL_FAILURE,
                 *args, **kwargs):
        """Constructor."""
        super(WantRestoreTransactionState_Node, self).__init__(*args, **kwargs)
        self.ack_result_code = ack_result_code

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return u'ack_result_code={self.ack_result_code!r}' \
                   .format(self=self)


    def to_bson(self):
        cls = self.__class__

        doc = super(WantRestoreTransactionState_Node, self).to_bson()

        # Optional fields
        if self.ack_result_code is not None:
            doc.update({'ack_result_code': self.ack_result_code})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(WantRestoreTransactionState_Node, cls)
                           .from_bson(doc),
                         # Optional
                       ack_result_code=doc.get('ack_result_code'))
