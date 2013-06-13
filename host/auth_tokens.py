#!/usr/bin/python
"""
The host-specific code for authentication tokens.
"""

#
# Imports
#

from __future__ import absolute_import
import Queue
from datetime import datetime
from time import time as _time



#
# Classes
#

class AuthTokenQueue(Queue.LifoQueue):
    """Variant of LifoQueue that retrieves most recently added entries first
    and silently removes least recently added entry on new entry addition,
    if queue is full.
    """

    def put(self, item, block=True, timeout=None):
        """
        look at Queue.LifoQueue doc; instead of raising Queue.Full exception,
        removes least recently added item and puts new item.
        """
        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() == self.maxsize:
                        # this replaces "raise Queue.Full"
                        self.queue.pop(0)
                elif timeout is None:
                    while self._qsize() == self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a positive number")
                else:
                    endtime = _time() + timeout
                    while self._qsize() == self.maxsize:
                        remaining = endtime - _time()
                        if remaining <= 0.0:
                            # this replaces "raise Queue.Full"
                            self.queue.pop(0)
                        self.not_full.wait(remaining)
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()


    def _put(self, item):
        """Keeps sort order from oldest to newest.

        @todo: there is no need in better implementation, len(self.queue) <=~10
        """
        self.queue.append(item)
        self.queue.sort(key=lambda x: x.ts)



class AuthTokenStore(object):

    __slots__ = ('__queue', '__last_auth_token_sync_ts')

    def __init__(self, tokens=None, maxsize=8):
        if tokens is None:
            tokens = []
        self.__queue = AuthTokenQueue(maxsize)
        self.__last_auth_token_sync_ts = None
        for token in tokens:
            self.put(token)


    @property
    def last_auth_token_sync_ts(self):
        return self.__last_auth_token_sync_ts


    @last_auth_token_sync_ts.setter
    def last_auth_token_sync_ts(self, ts):
        assert isinstance(ts, datetime), repr(ts)
        if self.__last_auth_token_sync_ts or datetime.min < ts:
            self.__last_auth_token_sync_ts = ts


    def put(self, item):
        """
        @type item: datatypes.AuthToken
        """
        self.__queue.put_nowait(item)
        self.last_auth_token_sync_ts = item.ts


    def get(self, default=None):
        try:
            return self.__queue.get_nowait()
        except Queue.Empty:
            return default
