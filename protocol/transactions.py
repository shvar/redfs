#!/usr/bin/python
"""
The base abstract classes for implementations of all transactions.
"""

#
# Imports
#

from common.abstractions import AbstractTransaction

import messages



#
# Classes
#

class BackupTransaction(AbstractTransaction):
    """
    Base abstract class for BACKUP transaction.
    """
    msg_class = messages.BackupMessage


class ChunksTransaction(AbstractTransaction):
    """
    Base abstract class for CHUNKS transaction.
    """
    msg_class = messages.ChunksMessage


class ExecuteOnHostTransaction(AbstractTransaction):
    """
    Base abstract class for EXECUTE_ON_HOST transaction.
    """
    msg_class = messages.ExecuteOnHostMessage


class ExecuteOnNodeTransaction(AbstractTransaction):
    """
    Base abstract class for EXECUTE_ON_NODE transaction.
    """
    msg_class = messages.ExecuteOnNodeMessage


class HeartbeatTransaction(AbstractTransaction):
    """
    Base abstract class for HEARTBEAT transaction.
    """
    msg_class = messages.HeartbeatMessage


class LoginTransaction(AbstractTransaction):
    """
    Base abstract class for LOGIN transaction.
    """
    msg_class = messages.LoginMessage


class LogoutTransaction(AbstractTransaction):
    """
    Base abstract class for LOGOUT transaction.
    """
    msg_class = messages.LogoutMessage


class NeedInfoFromHostTransaction(AbstractTransaction):
    """
    Base abstract class for NEED_INFO_FROM_HOST transaction.
    """
    msg_class = messages.NeedInfoFromHostMessage


class NeedInfoFromNodeTransaction(AbstractTransaction):
    """
    Base abstract class for NEED_INFO_FROM_NODE transaction.
    """
    msg_class = messages.NeedInfoFromNodeMessage


class NotifyHostTransaction(AbstractTransaction):
    """
    Base abstract class for NOTIFY_HOST transaction.
    """
    msg_class = messages.NotifyHostMessage


class NotifyNodeTransaction(AbstractTransaction):
    """
    Base abstract class for NOTIFY_NODE transaction.
    """
    msg_class = messages.NotifyNodeMessage


class ProgressTransaction(AbstractTransaction):
    """
    Base abstract class for PROGRESS transaction.
    """
    msg_class = messages.ProgressMessage


class ProvideBackupHostsTransaction(AbstractTransaction):
    """
    Base abstract class for PROVIDE_BACKUP_HOSTS transaction.
    """
    msg_class = messages.ProvideBackupHostsMessage


class ReadyForChunksTransaction(AbstractTransaction):
    """
    Base abstract class for READY_FOR_CHUNKS transaction.
    """
    msg_class = messages.ReadyForChunksMessage


class ReceiveChunksTransaction(AbstractTransaction):
    """
    Base abstract class for RECEIVE_CHUNKS transaction.
    """
    msg_class = messages.ReceiveChunksMessage


class RestoreTransaction(AbstractTransaction):
    """
    Base abstract class for RESTORE transaction.
    """
    msg_class = messages.RestoreMessage


class SendChunksTransaction(AbstractTransaction):
    """
    Base abstract class for SEND_CHUNKS transaction.
    """
    msg_class = messages.SendChunksMessage


class UpdateConfigurationTransaction(AbstractTransaction):
    """
    Base abstract class for UPDATE_CONFIGURATION transaction.
    """
    msg_class = messages.UpdateConfigurationMessage


class WantBackupTransaction(AbstractTransaction):
    """
    Base abstract class for WANT_BACKUP transaction.
    """
    msg_class = messages.WantBackupMessage


class WantRestoreTransaction(AbstractTransaction):
    """
    Base abstract class for WANT_RESTORE transaction.
    """
    msg_class = messages.WantRestoreMessage
