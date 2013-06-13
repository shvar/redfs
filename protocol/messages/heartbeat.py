#!/usr/bin/python
"""HEARTBEAT message implementation.

Outgoing HEARTBEAT message from the Host to the Node notifies the Node
of the Host's presence; also, it may contain the Host settings, the messages
from the Host to the Node, and the time of last Auth token synchronization.

The HEARTBEAT response from the Node may contain the messages, Auth tokens,
the information about the currently running restore processes.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from datetime import datetime
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from common.abstractions import IJSONable, AbstractMessage, AbstractInhabitant
from common.datatypes import LocalizedUserMessage, AuthToken
from common.utils import (strftime, strptime,
                          dt_to_ts, ts_to_dt, td_to_sec, sec_to_td)

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#


class HeartbeatMessage(AbstractMessage):
    """HEARTBEAT message.

    @cvar priority: Overrides the C{priority} field from AbstractMessage,
                    meaning the the heartbeats are in fact less important
                    than the messages with actual payload.
    """

    name = 'HEARTBEAT'
    version = 0

    priority = 20

    __slots__ = ('settings_getter', 'revive_getter',
                 'inbox_update', 'auth_tokens', 'last_auth_token_sync_ts',
                 'ack_lu_time', 'ack_restore_progress', 'inbox_update')


    def __init__(self, *args, **kwargs):
        super(HeartbeatMessage, self).__init__(*args, **kwargs)

        self.settings_getter = dict
        self.revive_getter = lambda: None

        self.inbox_update = {}
        self.auth_tokens = []
        self.last_auth_token_sync_ts = None

        self.ack_lu_time = None  # not actually used for anything important
        self.ack_restore_progress = []


    @bzip2
    @json_dumps
    def _get_body(self):
        # H2N
        assert callable(self.settings_getter), repr(self.settings_getter)
        pre_settings = self.settings_getter()
        assert isinstance(pre_settings, dict), repr(pre_settings)

        settings = {name: (value, strftime(time))
                        for name, (value, time) in pre_settings.iteritems()}

        result = {}

        # For "revive", we always get the up-to-date information from the app.
        # Note that .revive field is in AbstractMessage, and we override
        # it here.
        self.revive = _revive = self.revive_getter()
        logger.debug('For heartbeat, revive=%r', _revive)
        if not _revive:
            result['revive'] = 0
        if settings:
            result['settings'] = settings

        if self.inbox_update:
            result['msgs'] = HeartbeatMessage.serialize_inbox_update(
                                 self.inbox_update)
            logger.verbose('Host sends inbox_update: %s', self.inbox_update)

        if self.last_auth_token_sync_ts is not None:
            assert isinstance(self.last_auth_token_sync_ts, datetime), \
                   repr(self.last_auth_token_sync_ts)
            result['last_auth_token_sync_ts'] = \
                    dt_to_ts(self.last_auth_token_sync_ts)
            logger.verbose('Host sends last_auth_token_sync_ts: %s',
                           self.last_auth_token_sync_ts)

        return result


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # H2N
        if 'settings' in body:
            self.settings_getter = \
                lambda: {name: (value, strptime(time_str))
                             for name, (value, time_str)
                                 in body['settings'].iteritems()}

        if 'revive' in body:
            assert not body['revive']
            self.revive = bool(body['revive'])  # AbstractMessage

        self.revive_getter = lambda: self.revive

        if 'msgs' in body:
            self.inbox_update = HeartbeatMessage.deserialize_inbox_update(
                                    body['msgs'])
            logger.verbose('Node receives inbox_update: %s',
                            self.inbox_update)

        if 'last_auth_token_sync_ts' in body:
            self.last_auth_token_sync_ts = \
                    ts_to_dt(body['last_auth_token_sync_ts'])
            logger.verbose('Node receives last_auth_token_sync_ts: %s',
                           self.last_auth_token_sync_ts)


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # N2H
        result = {}

        # Process the last update time
        if self.ack_lu_time is not None:
            # We have a "last update time" to report back to the host
            assert isinstance(self.ack_lu_time, datetime), \
                   repr(self.ack_lu_time)
            result['last update time'] = strftime(self.ack_lu_time)
        else:
            # The settings were not updated on the node
            pass

        # Process the restore progress information:
        if self.ack_restore_progress:
            result['restore'] = \
                map(HeartbeatMessage.serialize_restore_progress,
                    self.ack_restore_progress)

        if self.inbox_update:
            result['msgs'] = HeartbeatMessage.serialize_inbox_update(
                                 self.inbox_update)
            logger.verbose('Node sends inbox_update: %s', self.inbox_update)

        if self.auth_tokens:
            assert consists_of(self.auth_tokens, AuthToken), \
                   repr(self.auth_tokens)
            result['auth_tokens'] = [t.to_json() for t in self.auth_tokens]
            logger.verbose('Node sends auth_tokens: %s',
                           self.auth_tokens)

        return result


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # N2H

        # Process the last update time
        try:
            self.ack_lu_time = strptime(body['last update time'])
        except KeyError:
            # No such key, ignore
            pass

        # Process the progress
        if 'restore' in body:
            self.ack_restore_progress = \
                map(HeartbeatMessage.deserialize_restore_progress,
                    body['restore'])

        if 'msgs' in body:
            self.inbox_update = HeartbeatMessage.deserialize_inbox_update(
                                    body['msgs'])
            logger.verbose('Host received inbox_update: %s', self.inbox_update)

        if 'auth_tokens' in body:
            self.auth_tokens = [AuthToken.from_json(t)()
                                   for t in body['auth_tokens']]
            assert consists_of(self.auth_tokens, AuthToken), \
                   repr(self.auth_tokens)
            logger.verbose('Host receives auth_tokens: %s',
                           self.auth_tokens)


    @staticmethod
    @contract_epydoc
    def serialize_restore_progress(unser):
        """
        @type unser: dict
        """
        result = {
            'started': dt_to_ts(unser['started']),
            'uuid': unser['uuid'].hex,
            'num': unser['num'],
            'of': unser['of'],
            'num_bytes': unser['num_bytes'],
            'of_bytes': unser['of_bytes'],
            'elapsed': td_to_sec(unser['elapsed']),
            'remaining': '' if unser['remaining'] is None
                            else td_to_sec(unser['remaining'])
        }
        assert set(result.iterkeys()) == set(unser.iterkeys()), \
               (unser, result)
        return result


    @staticmethod
    @contract_epydoc
    def deserialize_restore_progress(ser):
        """
        @type ser: dict
        """
        result = {
            'started': ts_to_dt(ser['started']),
            'uuid': UUID(ser['uuid']),
            'num': ser['num'],
            'of': ser['of'],
            'num_bytes': ser['num_bytes'],
            'of_bytes': ser['of_bytes'],
            'elapsed': sec_to_td(ser['elapsed']),
            'remaining': sec_to_td(ser['remaining']) if ser['remaining']
                                                     else None
        }
        assert set(result.iterkeys()) == set(ser.iterkeys()), \
               (ser, result)
        return result


    @classmethod
    @contract_epydoc
    def serialize_inbox_update(cls, unser):
        """
        @type unser: dict
        """
        result = {
            'last_msg_read_ts': dt_to_ts(unser['last_msg_read_ts'])
                                    if unser.get('last_msg_read_ts')
                                    else None,
            'last_msg_sync_ts': dt_to_ts(unser['last_msg_sync_ts'])
                                    if unser.get('last_msg_sync_ts')
                                    else None,
            'messages': list(m.to_json() for m in unser['messages']) \
                            if unser.get('messages') else None,
        }
        return {key: value
                    for key, value in result.iteritems() if value is not None}


    @classmethod
    @contract_epydoc
    def deserialize_inbox_update(cls, ser):
        """
        @type ser: dict
        """
        result = {
            'last_msg_read_ts': ts_to_dt(ser['last_msg_read_ts'])
                                    if 'last_msg_read_ts' in ser
                                    else None,
            'last_msg_sync_ts': ts_to_dt(ser['last_msg_sync_ts'])
                                    if 'last_msg_sync_ts' in ser
                                    else None,
            'messages': list(LocalizedUserMessage.from_json(m)()
                                 for m in ser['messages'])
                            if 'messages' in ser else None,
        }
        return {key: value
                    for key, value in result.iteritems() if value is not None}
