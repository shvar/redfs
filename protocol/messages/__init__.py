#!/usr/bin/python
"""
The implementation and serializing/deserializing code
for all the protocol messages between Hosts and Nodes.
"""

from .backup import BackupMessage
from .chunks import ChunksMessage
from .execute_on_host import ExecuteOnHostMessage
from .execute_on_node import ExecuteOnNodeMessage
from .heartbeat import HeartbeatMessage
from .login import LoginMessage
from .logout import LogoutMessage
from .need_info_from_host import NeedInfoFromHostMessage
from .need_info_from_node import NeedInfoFromNodeMessage
from .notify_host import NotifyHostMessage
from .notify_node import NotifyNodeMessage
from .progress import ProgressMessage
from .provide_backup_hosts import ProvideBackupHostsMessage
from .ready_for_chunks import ReadyForChunksMessage
from .receive_chunks import ReceiveChunksMessage
from .restore import RestoreMessage
from .send_chunks import SendChunksMessage
from .update_configuration import UpdateConfigurationMessage
from .want_backup import WantBackupMessage
from .want_restore import WantRestoreMessage


assert (ProvideBackupHostsMessage.ResultCodes._all() <=
        BackupMessage.ResultCodes._all())
assert BackupMessage.ResultCodes._all() <= WantBackupMessage.ResultCodes._all()


ALL_MSG_CLASSES = (
    BackupMessage,
    ChunksMessage,
    ExecuteOnHostMessage,
    ExecuteOnNodeMessage,
    HeartbeatMessage,
    LoginMessage,
    LogoutMessage,
    NeedInfoFromHostMessage,
    NeedInfoFromNodeMessage,
    NotifyHostMessage,
    NotifyNodeMessage,
    ProgressMessage,
    ProvideBackupHostsMessage,
    ReadyForChunksMessage,
    ReceiveChunksMessage,
    RestoreMessage,
    SendChunksMessage,
    UpdateConfigurationMessage,
    WantBackupMessage,
    WantRestoreMessage,
)
