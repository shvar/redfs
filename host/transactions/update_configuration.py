#!/usr/bin/python
"""
UPDATE_CONFIGURATION transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from datetime import datetime

from common.abstractions import unpauses

from protocol import transactions

from host import db
from host.db import HostQueries

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)
logger_status_settings_modified = logging.getLogger('status.settings.modified')



#
# Classes
#

class UpdateConfigurationTransaction_Host(
          transactions.UpdateConfigurationTransaction,
          AbstractHostTransaction):
    """UPDATE_CONFIGURATION transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the UPDATE_CONFIGURATION transaction on the Host."""

        __slots__ = ()

        name = 'UPDATE_CONFIGURATION'



    @unpauses
    def on_begin(self):
        if self.message.good:
            _now = datetime.utcnow()
            _settings = self.message.settings
            logger.debug('On %s, received the configuration data %r',
                         _now, _settings)

            for name, (value, time) in _settings.iteritems():
                # Ignore the time received from node.
                with db.RDB() as rdbw:
                    HostQueries.HostSettings.set(name, value, _now,
                                                 direct=True,
                                                 rdbw=rdbw)

                parsed_value = HostQueries.HostSettings.get(name, value)

                logger_status_settings_modified.info(
                    'Setting %r modified to %r',
                    name, parsed_value,
                    extra={'s_name': name,
                           's_value': parsed_value})

            self.manager.app.last_settings_sync_time = _now

        else:
            logger.error('Empty message %r', self.message)


    def on_end(self):
        """
        In fact, it is not used, as the UPDATE_CONFIGURATION_ACK is being
        sent manually, after the configuration message body is received.
        """
        self.message_ack = self.message.reply()
        self.manager.post_message(self.message_ack)
