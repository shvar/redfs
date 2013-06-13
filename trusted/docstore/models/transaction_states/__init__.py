#!/usr/bin/python
"""The docstore models for all transaction states."""

from .backup import BackupTransactionState_Node
from .chunks import ChunksTransactionState_Node
from .execute_on_host import ExecuteOnHostTransactionState_Node
from .execute_on_node import ExecuteOnNodeTransactionState_Node
from .heartbeat import HeartbeatTransactionState_Node
from .login import LoginTransactionState_Node
from .logout import LogoutTransactionState_Node
from .need_info_from_host import NeedInfoFromHostTransactionState_Node
from .need_info_from_node import NeedInfoFromNodeTransactionState_Node
from .notify_host import NotifyHostTransactionState_Node
from .notify_node import NotifyNodeTransactionState_Node
from .progress import ProgressTransactionState_Node
from .provide_backup_hosts import ProvideBackupHostsTransactionState_Node
from .ready_for_chunks import ReadyForChunksTransactionState_Node
from .receive_chunks import ReceiveChunksTransactionState_Node
from .restore import RestoreTransactionState_Node
from .send_chunks import SendChunksTransactionState_Node
from .update_configuration import UpdateConfigurationTransactionState_Node
from .want_backup import WantBackupTransactionState_Node
from .want_restore import WantRestoreTransactionState_Node


TRANSACTION_STATE_CLASS_BY_NAME = {
    'BACKUP': BackupTransactionState_Node,
    'CHUNKS': ChunksTransactionState_Node,
    'EXECUTE_ON_HOST': ExecuteOnHostTransactionState_Node,
    'EXECUTE_ON_NODE': ExecuteOnNodeTransactionState_Node,
    'HEARTBEAT': HeartbeatTransactionState_Node,
    'LOGIN': LoginTransactionState_Node,
    'LOGOUT': LogoutTransactionState_Node,
    'NEED_INFO_FROM_HOST': NeedInfoFromHostTransactionState_Node,
    'NEED_INFO_FROM_NODE': NeedInfoFromNodeTransactionState_Node,
    'NOTIFY_HOST': NotifyHostTransactionState_Node,
    'NOTIFY_NODE': NotifyNodeTransactionState_Node,
    'PROGRESS': ProgressTransactionState_Node,
    'PROVIDE_BACKUP_HOSTS': ProvideBackupHostsTransactionState_Node,
    'READY_FOR_CHUNKS': ReadyForChunksTransactionState_Node,
    'RECEIVE_CHUNKS': ReceiveChunksTransactionState_Node,
    'RESTORE': RestoreTransactionState_Node,
    'SEND_CHUNKS': SendChunksTransactionState_Node,
    'UPDATE_CONFIGURATION': UpdateConfigurationTransactionState_Node,
    'WANT_BACKUP': WantBackupTransactionState_Node,
    'WANT_RESTORE': WantRestoreTransactionState_Node,
}

if __debug__:
    from protocol.messages import ALL_MSG_CLASSES; 'OPTIONAL'  # snakefood
    assert all(msg_class.name in TRANSACTION_STATE_CLASS_BY_NAME
                   for msg_class in ALL_MSG_CLASSES), \
           (ALL_MSG_CLASSES, TRANSACTION_STATE_CLASS_BY_NAME)
