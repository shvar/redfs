#!/usr/bin/python
"""
CHUNKS transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import

from ._common import NodeTransactionState



#
# Classes
#

class ChunksTransactionState_Node(NodeTransactionState):
    """The state for the CHUNKS transaction on the Node."""

    __slots__ = ()

    name = 'CHUNKS'
