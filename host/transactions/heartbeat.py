#!/usr/bin/python
"""
HEARTBEAT transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from itertools import imap

from contrib.dbc import contract_epydoc

from common.abstractions import pauses

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)
logger_status = logging.getLogger('status.heartbeat')
logger_status_restore = logging.getLogger('status.restore.progress')



#
# Classes
#

class HeartbeatTransaction_Host(transactions.HeartbeatTransaction,
                                AbstractHostTransaction):
    """HEARTBEAT transaction on the Host."""

    __slots__ = ('settings_getter', 'prev_settings_sync_time')

    __hb_count = 0



    class State(AbstractHostTransaction.State):
        """The state for the HEARTBEAT transaction on the Host."""

        __slots__ = ('settings_getter',)

        name = 'HEARTBEAT'


        @contract_epydoc
        def __init__(self, settings_getter, *args, **kwargs):
            """
            @type settings_getter: col.Callable

            @todo: settings_getter IS NOT SERIALIZABLE!
            """
            super(HeartbeatTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.settings_getter = settings_getter



    @pauses
    def on_begin(self):

        with self.open_state() as state:
            self.message.settings_getter = state.settings_getter

        self.message.revive_getter = \
            lambda app=self.manager.app: app.do_heartbeats_revive

        self.message.inbox_update = self.__get_inbox_update_for_node()
        self.message.last_auth_token_sync_ts = \
                self.manager.app.auth_token_store.last_auth_token_sync_ts
        logger.verbose('host last_auth_token_sync_ts: %s',
                       self.message.last_auth_token_sync_ts)

        # The message, when sending out the settings,
        # may update the last setting sync time.
        # Reserve the previous one, to revert in case of errors.
        self.prev_settings_sync_time = self.manager.app.last_settings_sync_time

        self.manager.post_message(self.message)


    def __get_inbox_update_for_node(self):
        inbox = self.manager.app.inbox
        return {'last_msg_read_ts': inbox.last_msg_read_ts,
                'last_msg_sync_ts': inbox.last_msg_sync_ts}


    def on_end(self):
        if self.failure is None:
            # Success!
            message_ack = self.message_ack

            if message_ack is None:
                logger.warning('%r failed', self)
            else:
                # We indeed received a reply, rather than failed a transaction
                HeartbeatTransaction_Host.__hb_count += 1
                logger_status.info('Heartbeat count: %i',
                                   HeartbeatTransaction_Host.__hb_count,
                                   extra={
                                       '_hb_count':
                                           HeartbeatTransaction_Host.__hb_count
                                   })

                _ack_lu_time = self.message_ack.ack_lu_time
                if _ack_lu_time is not None:
                    logger.debug('Settings synced to the node, '
                                     'last update time is %s',
                                 _ack_lu_time)

                # Now process the components of message_ack

                ack_restore_progress = message_ack.ack_restore_progress
                if ack_restore_progress:
                    progress_format = \
                        '\t{0[uuid]!s}: started at {0[started]}, ' \
                        '{0[num]}/{0[of]}, remaining {0[remaining]!s}'
                    logger_status_restore.info(
                        'Restore progress (%i):\n\t%s',
                        len(ack_restore_progress),
                        '\n'.join(imap(progress_format.format,
                                       ack_restore_progress)),
                        extra={'progresses': ack_restore_progress})

                inbox_update = self.message_ack.inbox_update
                if inbox_update:
                    self.manager.app.inbox.update(
                            inbox_update.get('last_msg_read_ts'),
                            inbox_update.get('messages'))

                auth_tokens = self.message_ack.auth_tokens
                for t in auth_tokens:
                    self.manager.app.auth_token_store.put(t)

        else:  # if self.failure is not None
            # Failure!
            # Revert the last_settings_sync_time
            logger.debug('Failed %r, reverting last sync time from %s to %s',
                         self,
                         self.manager.app.last_settings_sync_time,
                         self.prev_settings_sync_time)
            self.manager.app.last_settings_sync_time = \
                self.prev_settings_sync_time
