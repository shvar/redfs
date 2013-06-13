#!/usr/bin/python
"""
PROVIDE_BACKUP_HOST transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import

from ._common import NodeTransactionState



#
# Classes
#

class ProvideBackupHostsTransactionState_Node(NodeTransactionState):
    """The state for the PROVIDE_BACKUP_HOSTS transaction on the Node."""

    __slots__ = ()

    name = 'PROVIDE_BACKUP_HOSTS'
