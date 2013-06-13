#!/usr/bin/python
"""
PROGRESS transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import

from ._common import NodeTransactionState



#
# Classes
#

class ProgressTransactionState_Node(NodeTransactionState):
    """The state for the PROGRESS transaction on the Node."""

    __slots__ = ()

    name = 'PROGRESS'
