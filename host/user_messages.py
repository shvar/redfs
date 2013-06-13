#!/usr/bin/python
"""
The human-readable messages from the system to the particular user.
"""

#
# Imports
#

from __future__ import absolute_import
from datetime import datetime
import logging



#
# Constants
#

logger_status_inbox_last_msg_read_ts_updates = \
    logging.getLogger('status.inbox.last_msg_read_ts_updates')
logger_status_inbox_new_messages = \
    logging.getLogger('status.inbox.new_messages')



#
# Classes
#

class Inbox(object):
    """The inbox for Host-Node communication."""

    __slots__ = ('__last_msg_read_ts', '__last_msg_sync_ts')


    def __init__(self):
        self.__last_msg_read_ts = None
        self.__last_msg_sync_ts = None


    @property
    def last_msg_read_ts(self):
        return self.__last_msg_read_ts


    @last_msg_read_ts.setter
    def last_msg_read_ts(self, ts):
        if self.__last_msg_read_ts or datetime.min < ts:
            self.__last_msg_read_ts = ts
            self.__emit_new_last_msg_read_ts()


    @property
    def last_msg_sync_ts(self):
        return self.__last_msg_sync_ts


    @last_msg_sync_ts.setter
    def last_msg_sync_ts(self, ts):
        if self.__last_msg_sync_ts or datetime.min < ts:
            self.__last_msg_sync_ts = ts


    def update(self, last_msg_read_ts=None, messages=None):
        if last_msg_read_ts is not None:
            self.last_msg_read_ts = last_msg_read_ts

        if messages is not None:
            d = {message.key: message
                    for message in messages
                    if self.last_msg_read_ts or datetime.min < message.ts}
            if d:
                messages = sorted(d.values(), key=lambda m: m.ts)
                self.last_msg_sync_ts = messages[-1].ts
                self.__emit_new_messages(messages)


    def __emit_new_messages(self, messages):
        logger_status_inbox_new_messages.info(
            'Inbox: new messages',
            extra={'messages': messages})


    def __emit_new_last_msg_read_ts(self):
        logger_status_inbox_last_msg_read_ts_updates.info(
            'Inbox: new last_msg_read_ts',
            extra={'last_msg_read_ts': self.last_msg_read_ts})
