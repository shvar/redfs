#!/usr/bin/python
"""
LOGOUT transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import

from ._common import NodeTransactionState



#
# Classes
#

class LogoutTransactionState_Node(NodeTransactionState):
    """The state for the STATE transaction on the Node."""

    __slots__ = ()

    name = 'LOGOUT'
