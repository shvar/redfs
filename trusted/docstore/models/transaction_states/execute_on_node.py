#!/usr/bin/python
"""
EXECUTE_ON_NODE transaction state in the FastDB.
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

class ExecuteOnNodeTransactionState_Node(NodeTransactionState):
    """The state for the EXECUTE_ON_NODE transaction on the Node."""

    __slots__ = ('deleted_datasets',)

    name = 'EXECUTE_ON_NODE'


    @contract_epydoc
    def __init__(self, deleted_datasets=None, *args, **kwargs):
        """Constructor.

        @type deleted_datasets: NoneType, col.Iterable
        """
        super(ExecuteOnNodeTransactionState_Node, self) \
            .__init__(*args, **kwargs)
        self.deleted_datasets = list(coalesce(deleted_datasets, []))

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return u'deleted_datasets={self.deleted_datasets!r}' \
                   .format(self=self)


    def to_bson(self):
        cls = self.__class__

        doc = super(ExecuteOnNodeTransactionState_Node, self).to_bson()
        # Mandatory fields
        doc.update({'deleted_datasets': list(self.deleted_datasets)})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(ExecuteOnNodeTransactionState_Node, cls)
                           .from_bson(doc),
                         # Mandatory
                       deleted_datasets=doc['deleted_datasets'])
