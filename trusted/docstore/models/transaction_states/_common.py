#!/usr/bin/python
"""Common stuff for the models for all transaction states."""

#
# Imports
#

from __future__ import absolute_import
from functools import partial

from common.abstractions import AbstractTransaction

from trusted.ibsonable import IBSONable

from ..idocstoredocument import IDocStoreAnyDocument



#
# Classes
#

class NodeTransactionState(AbstractTransaction.State,
                           IDocStoreAnyDocument, IBSONable):
    """
    An abstract class for every node-specific
    transaction state payload implementation.

    @cvar name: the name of the transaction. Every subclass implementation
        must contain one.

    @note: every transaction state payload on the node must be BSONable.
    """

    __slots__ = ()


    def __init__(self, *args, **kwargs):
        cls = self.__class__
        super(NodeTransactionState, self).__init__(tr_type=cls.name,
                                                   *args, **kwargs)


    def __str__(self):
        """
        Hack the C{AbstractTransaction.State.__str__()} to ignore C{tr_type}
        constructor argument.
        """
        return (u'tr_start_time={self.tr_start_time!r}, '
                 'tr_uuid={self.tr_uuid!r}, '
                 'tr_src_uuid={self.tr_src_uuid!r}, '
                 'tr_dst_uuid={self.tr_dst_uuid!r}'
                    .format(super=super(AbstractTransaction.State, self)
                                      .__str__(),
                            self=self))


    def to_bson(self):
        doc = super(NodeTransactionState, self).to_bson()
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(NodeTransactionState, cls).from_bson(doc)
                         # Optional
                      )
