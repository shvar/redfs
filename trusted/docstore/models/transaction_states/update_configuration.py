#!/usr/bin/python
"""
UPDATE_CONFIGURATION transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from functools import partial

from contrib.dbc import contract_epydoc

from ._common import NodeTransactionState



#
# Classes
#

class UpdateConfigurationTransactionState_Node(NodeTransactionState):
    """The state for the UPDATE_CONFIGURATION transaction on the Node."""

    __slots__ = ('settings',)

    name = 'UPDATE_CONFIGURATION'


    @contract_epydoc
    def __init__(self, settings, *args, **kwargs):
        """Constructor.

        @type settings: col.Mapping
        """
        super(UpdateConfigurationTransactionState_Node, self) \
            .__init__(*args, **kwargs)
        self.settings = settings

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return u'{super}, settings={self.settings!r}'.format(
                   super=super(UpdateConfigurationTransactionState_Node, self)
                             .__str__(),
                   self=self)


    def to_bson(self):
        cls = self.__class__

        doc = super(UpdateConfigurationTransactionState_Node, self).to_bson()
        # Mandatory
        doc.update({'settings': self.settings})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(UpdateConfigurationTransactionState_Node, cls)
                           .from_bson(doc),
                         # Mandatory
                       settings=doc['settings'])
