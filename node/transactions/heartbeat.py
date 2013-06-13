#!/usr/bin/python
"""
HEARTBEAT transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from datetime import datetime
from threading import Lock
from uuid import UUID

from common.abstractions import pauses, unpauses
from common.datatypes import LocalizedUserMessage, AuthToken
from common.db import Queries
from common.utils import (
    coalesce, exceptions_logged, get_wider_locales, gen_uuid, in_main_thread)
from common.twisted_utils import callLaterInThread

from protocol import transactions

from trusted import db, docstore as ds
from trusted.db import TrustedQueries
from trusted.docstore.fdbqueries import FDBQueries
from trusted.docstore.models.transaction_states.heartbeat import \
    HeartbeatTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Constants
#

logger = logging.getLogger(__name__)




#
# Classes
#

class HeartbeatTransaction_Node(transactions.HeartbeatTransaction,
                                AbstractNodeTransaction):
    """HEARTBEAT transaction on the Node."""

    __slots__ = ()

    State = HeartbeatTransactionState_Node


    @pauses
    def on_begin(self):
        """
        This method is called when the HEARTBEAT request together with its body
        is received completely.

        @todo: Check that the received settings are among the ones
               that are allowed to update.
        """
        cls = self.__class__

        _message = self.message
        me = _message.dst
        from_host = _message.src
        _host_uuid = from_host.uuid

        _app = self.manager.app  # NodeApp
        _known_hosts = _app.known_hosts

        # We've probably received some settings,
        # let's update them in the database if needed.
        incoming_settings = _message.settings_getter()

        if not _message.revive and incoming_settings:
            logger.warning('Non-revival heartbeat sent some settings: %r',
                           incoming_settings)

        if _message.revive and incoming_settings:
            # Filter out the special-case settings
            assert 'urls' in incoming_settings, repr(incoming_settings)
            del incoming_settings['urls']

            # Don't update some settings which only the node is allowed
            # to update, and which are not real settings.
            real_settings = \
                {k: v
                     for k, v in incoming_settings.iteritems()
                     if k != 'urls' and
                        k not in Queries.Settings.SETTINGS_NODE_UPDATE_ONLY}

            if real_settings:
                # Need to write some new settings into the DB
                _now = datetime.utcnow()

                _last_update_time = \
                    max(time for value, time in real_settings.itervalues())

                with self.open_state(for_update=True) as state:
                    state.last_update_time = _last_update_time

                logger.debug('Received settings %r', real_settings)
                TrustedQueries.TrustedSettings.update_host_settings_directly(
                    _host_uuid,
                    {k: v
                         for k, (v, _time_sink)
                             in real_settings.iteritems()},
                    _now)

                # As we've just received the settings, let's also send our ones
                # to the Host.
                self.__do_nested_update_configuration(real_settings)

        if _message.revive:
            self.__update_inbox_status_from_host()
            self.__init_or_update_last_auth_token_sync_ts()

        _app.maybe_refresh_chunks_info(node=me,
                                       host=from_host,
                                       parent_tr=self)

        # End transaction.
        # We do nothing at all and stay paused,
        # so that when the timer fires and the transaction is unpaused,
        # the on_end will be called.
        with db.RDB() as rdbw:
            hb_delay = \
                TrustedQueries.TrustedSettings.get_setting_or_default(
                    rdbw, Queries.Settings.HEARTBEAT_INTERVAL, _host_uuid)
        logger.debug('Inside %r, delaying for %0.02f seconds', self, hb_delay)

        # This is where Node throttles the Heartbeat processing.
        callLaterInThread(hb_delay, self.__try_to_unpause_myself)


    @exceptions_logged(logger)
    def __try_to_unpause_myself(self):
        assert not in_main_thread()
        logger.debug('After delay, trying %r.unpause(try_destroy=True)', self)
        self.unpause(try_destroy=True)


    def on_end(self):
        _src = self.message.src

        # We completed HEARTBEAT processing, so let's prepare
        # the reply message.

        self.message_ack = self.message.reply()

        with self.open_state() as state:
            self.message_ack.ack_lu_time = state.last_update_time

        self.message_ack.ack_restore_progress = \
            list(self.manager.app.get_restore_progress_info(_src.uuid))

        if self.message_ack.ack_restore_progress:
            logger.verbose('Restore progress for %r: %r',
                           self.message_ack.dst.uuid,
                           self.message_ack.ack_restore_progress)

        self.message_ack.inbox_update = self.__get_inbox_update_for_host()
        self.message_ack.auth_tokens = list(self.__get_auth_tokens())

        self.manager.post_message(self.message_ack)


    def __update_inbox_status_from_host(self):
        """
        When the Node receives from Host the information about its
        last-read-ts/last-sync-ts, here it updates it in the Docstore.
        """
        host = self.message.src

        tr_msg_read_ts = self.message.inbox_update.get('last_msg_read_ts')
        tr_msg_sync_ts = self.message.inbox_update.get('last_msg_sync_ts')

        with ds.FDB() as fdbw:
            if tr_msg_read_ts is not None:
                FDBQueries.Users.update_last_msg_read_ts(host.user.name,
                                                         tr_msg_read_ts,
                                                         fdbw)
            if tr_msg_sync_ts is not None:
                FDBQueries.Users.update_last_msg_sync_ts(host.user.name,
                                                         host.uuid,
                                                         tr_msg_sync_ts,
                                                         fdbw)


    @unpauses
    def __do_nested_update_configuration(self, incoming_settings):
        _message = self.message

        with db.RDB() as rdbw:
            settings = Queries.Settings.get_all_except(
                           host_uuid=_message.src.uuid,
                           exclude=incoming_settings.iterkeys(),
                           rdbw=rdbw)

        uc_tr = self.manager.create_new_transaction(
                    name='UPDATE_CONFIGURATION',
                    src=_message.dst,
                    dst=_message.src,
                    parent=self,
                    # UPDATE_CONFIGURATION-specific
                    settings=settings)


    def __get_inbox_update_for_host(self):
        """
        Calculate which messages are missing from the host
        thus should be sent out to it in HEARTBEAT_ACK.

        @returns: dict (WHAT?)
        """
        host = self.message.src
        tr_data = self.message.inbox_update

        with db.RDB() as rdbw:
            locale = TrustedQueries.TrustedSettings.get_setting_or_default(
                        rdbw, Queries.Settings.LANGUAGE, host.uuid).lower()
        locales = get_wider_locales(locale)

        assert all(l.lower() == l for l in locales), repr(locales)

        with ds.FDB() as fdbw:
            if not tr_data.get('last_msg_sync_ts'):
                target_msg_read_ts = FDBQueries.Users.get_last_msg_read_ts(
                                         host.user.name,
                                         fdbw) or datetime.min
                target_msg_sync_ts = datetime.min
            else:
                _ds_msg_read_ts, _ds_msg_sync_ts = \
                    FDBQueries.Users.get_last_msg_read_and_sync_ts(
                        host.user.name, host.uuid, fdbw)

                ds_msg_read_ts = coalesce(_ds_msg_read_ts, datetime.min)
                ds_msg_sync_ts = coalesce(_ds_msg_sync_ts, datetime.min)

                target_msg_read_ts = max(tr_data.get('last_msg_read_ts') \
                                             or datetime.min,
                                         ds_msg_read_ts)
                target_msg_sync_ts = max(tr_data.get('last_msg_sync_ts') \
                                             or datetime.min,
                                         ds_msg_sync_ts)

            target_ts = max(target_msg_read_ts, target_msg_sync_ts)

            messages = FDBQueries.UserMessages.get_new_messages(host.user.name,
                                                                target_ts,
                                                                locales=locales,
                                                                fdbw=fdbw)

        _translated_messages = (m.translate(locale=locale, strict=False)
                                    for m in messages)

        translated_messages = (msg for msg in _translated_messages if msg.body)

        if target_msg_read_ts == datetime.min:
            target_msg_read_ts = None

        # Eagerly evaluate translated_messages to store in the message.
        return {'last_msg_read_ts': target_msg_read_ts,
                'messages': list(translated_messages)}


    def __init_or_update_last_auth_token_sync_ts(self):
        host_uuid = self.message.src.uuid
        last_sync_ts = self.message.last_auth_token_sync_ts

        with ds.FDB() as fdbw:
            if last_sync_ts is None:
                FDBQueries.AuthTokens.init_host_tokens(host_uuid, fdbw)
            else:
                FDBQueries.AuthTokens.update_last_token_sync_ts(
                    host_uuid, last_sync_ts, fdbw)


    def __get_auth_tokens(self):
        """
        @rtype: col.Iterable
        """
        host_uuid = self.message.src.uuid

        with ds.FDB() as fdbw:
            result = FDBQueries.AuthTokens.get_info_and_tokens(host_uuid, fdbw)

            if result is None:
                tokens_to_send = []
            else:
                tokens, last_delete_ts, last_synced_token_ts = result
                assert isinstance(tokens, list), repr(tokens)
                assert isinstance(last_delete_ts, datetime), \
                       repr(last_delete_ts)
                assert isinstance(last_synced_token_ts, datetime), \
                       repr(last_synced_token_ts)
                tokens_to_send = [t for t in tokens
                                    if t.ts > last_synced_token_ts]
            return tokens_to_send
