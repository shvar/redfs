#!/usr/bin/python
"""
NOTIFY_HOST transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from functools import partial
from types import NoneType

from contrib.dbc import contract_epydoc

from common.utils import coalesce

from ._common import NodeTransactionState



#
# Classes
#

class NotifyHostTransactionState_Node(NodeTransactionState):
    """The state for the NOTIFY_HOST transaction on the Node."""

    __slots__ = ('chunk_uuids_to_replicate', 'chunk_uuids_to_restore')

    name = 'NOTIFY_HOST'


    @contract_epydoc
    def __init__(self,
                 chunk_uuids_to_replicate=None, chunk_uuids_to_restore=None,
                *args, **kwargs):
        """Constructor.

        @type chunk_uuids_to_replicate: NoneType, col.Iterable
        @type chunk_uuids_to_restore: NoneType, col.Iterable
        """
        super(NotifyHostTransactionState_Node, self).__init__(*args, **kwargs)
        self.chunk_uuids_to_replicate = list(coalesce(chunk_uuids_to_replicate,
                                                      []))
        self.chunk_uuids_to_restore = list(coalesce(chunk_uuids_to_restore,
                                                    []))

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return (u'chunk_uuids_to_replicate='
                     '{self.chunk_uuids_to_replicate!r}, '
                 'chunk_uuids_to_restore={self.chunk_uuids_to_restore}'
                     .format(self=self))


    def to_bson(self):
        cls = self.__class__

        doc = super(NotifyHostTransactionState_Node, self).to_bson()

        # Optional fields
        if self.chunk_uuids_to_replicate is not None:
            doc.update({'chunk_uuids_to_replicate':
                            self.chunk_uuids_to_replicate})
        if self.chunk_uuids_to_restore is not None:
            doc.update({'chunk_uuids_to_restore':
                            self.chunk_uuids_to_restore})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(NotifyHostTransactionState_Node, cls)
                           .from_bson(doc),
                         # Optional
                       chunk_uuids_to_replicate=
                           doc.get('chunk_uuids_to_replicate'),
                       chunk_uuids_to_restore=
                           doc.get('chunk_uuids_to_restore'))
