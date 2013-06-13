#!/usr/bin/python
"""
READY_FOR_CHUNKS transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import

from ._common import NodeTransactionState



#
# Classes
#

class ReadyForChunksTransactionState_Node(NodeTransactionState):
    """The state for the READY_FOR_CHUNKS transaction on the Node."""

    __slots__ = ()

    name = 'READY_FOR_CHUNKS'
