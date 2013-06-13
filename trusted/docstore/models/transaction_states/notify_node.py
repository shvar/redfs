#!/usr/bin/python
"""
NOTIFY_NODE transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import

from ._common import NodeTransactionState



#
# Classes
#

class NotifyNodeTransactionState_Node(NodeTransactionState):
    """The state for the NOTIFY_NODE transaction on the Node."""

    __slots__ = ()

    name = 'NOTIFY_NODE'
