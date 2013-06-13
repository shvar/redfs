#!/usr/bin/python
"""
EXECUTE_ON_HOST transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from functools import partial
from uuid import UUID

from contrib.dbc import consists_of, contract_epydoc

from ._common import NodeTransactionState



#
# Classes
#

class ExecuteOnHostTransactionState_Node(NodeTransactionState):
    """The state for the EXECUTE_ON_HOST transaction on the Node."""

    __slots__ = ('chunk_uuids_to_delete',)

    name = 'EXECUTE_ON_HOST'


    @contract_epydoc
    def __init__(self, chunk_uuids_to_delete, *args, **kwargs):
        """Constructor.

        @type chunk_uuids_to_delete: col.Iterable
        """
        super(ExecuteOnHostTransactionState_Node, self) \
            .__init__(*args, **kwargs)
        self.chunk_uuids_to_delete = list(chunk_uuids_to_delete)
        assert consists_of(self.chunk_uuids_to_delete, UUID), \
               repr(self.chunk_uuids_to_delete)

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return u'chunk_uuids_to_delete={self.chunk_uuids_to_delete!r}' \
                   .format(self=self)


    def to_bson(self):
        cls = self.__class__

        doc = super(ExecuteOnHostTransactionState_Node, self).to_bson()
        # Mandatory fields
        doc.update({'chunk_uuids_to_delete': self.chunk_uuids_to_delete})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(ExecuteOnHostTransactionState_Node, cls)
                           .from_bson(doc),
                         # Mandatory
                       chunk_uuids_to_delete=doc['chunk_uuids_to_delete'])
