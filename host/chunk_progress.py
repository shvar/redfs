#!/usr/bin/python
"""
This class incapsulates the delayed sending of PROGRESS messages
for every chunks sent out.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from datetime import datetime, timedelta
from threading import RLock

from twisted.internet.defer import Deferred

from contrib.dbc import contract_epydoc, consists_of

from common.chunks import Chunk
from common.datatypes import (ProgressNotificationPerHost,
                              ProgressNotificationPerHostWithDeferred)
from common.http_transport import HTTPClientTransport
from common.inhabitants import Host
from common.utils import exceptions_logged, in_main_thread
from common.twisted_utils import callLaterInThread



#
# Constants
#

logger = logging.getLogger(__name__)

WAIT_TO_COLLECT_PROGRESS_BEFORE_SEND = 10.0  # seconds



#
# Classes
#

class ChunkProgressNotificator(object):
    """
    A special class collecting the progress notifications to specific hosts
    and launching the appropriate PROGRESS transactions with some delay,
    being able to pass most notifications to the same host simultaneously.

    @type __notification_storage: dict
    @invariant __notification_storage:
        consists_of(__notification_storage.iterkeys(),
                    Host)
    @invariant __notification_storage:
        consists_of(__notification_storage.itervalues(),
                    ProgressNotificationPerHostWithDeferred)
    @type __notification_storage_lock: RLock
    @ivar __waiting_for_transaction: The boolean flag indicating that we've
        initiated the delay to start the PROGRESS transaction and
        collecting the data, but haven't issued the transaction yet.
    """
    __slots__ = ('app',
                 '__notification_storage', '__notification_storage_lock',
                 '__waiting_for_transaction')


    class NotificationGetter(object):
        """
        This class is able to get the notification data which is used
        in the PROGRESS transaction, and then, when the transaction
        is completed, it can run the callbacks of the associated deferreds.
        """
        __slots__ = ('__notificator', '__notification_data')


        @contract_epydoc
        def __init__(self, notificator):
            """
            @type notificator: ChunkProgressNotificator
            """
            self.__notificator = notificator
            self.__notification_data = None


        @exceptions_logged(logger)
        def __call__(self):
            """
            Fetch the up-to-date notification data from the notificator,
            but only once in (the object's) lifetime.

            @rtype: dict
            @postcondition: consists_of(result.iterkeys(), Host)
            @postcondition: all(consists_of(v, ProgressNotificationPerHost)
                                    for v in result.itervalues())
            """
            assert self.__notification_data is None, \
                   repr(self.__notification_data)
            self.__notification_data = self.__notificator \
                                           ._get_actual_host_chunks_map()
            return self.__notification_data


        @contract_epydoc
        def __run_deferred_method(self, deferred_method, *args, **kwargs):
            assert self.__notification_data is not None, \
                   repr(self.__notification_data)
            for notifications in self.__notification_data.itervalues():
                for notification in notifications:
                    deferred_method(notification.deferred, *args, **kwargs)


        @contract_epydoc
        def run_callbacks(self, *args, **kwargs):
            """
            For all the notification sent out in a message, run the callbacks.
            """
            self.__run_deferred_method(Deferred.callback, *args, **kwargs)


        @contract_epydoc
        def run_errbacks(self, *args, **kwargs):
            """
            For all the notification sent out in a message, run the errbacks.
            """
            self.__run_deferred_method(Deferred.errback, *args, **kwargs)


    @contract_epydoc
    def __init__(self, app):
        """Constructor.

        @type app: HTTPClientTransport
        """
        self.app = app
        self.__notification_storage = {}
        self.__notification_storage_lock = RLock()
        self.__waiting_for_transaction = False


    @contract_epydoc
    def _get_actual_host_chunks_map(self):
        """
        This method is able to get the latest/up-to-date chunks mapping,
        i.e. the information to be passed in the PROGRESS message.

        The method has "friends-only" visibility and should be used only by
        C{NotificationGetter}.

        @rtype: dict
        @postcondition: consists_of(result.iterkeys(), Host)
        @postcondition: all(consists_of(v, ProgressNotificationPerHost)
                                for v in result.itervalues())
        """
        with self.__notification_storage_lock:
            self.__waiting_for_transaction = False

            result = self.__notification_storage
            self.__notification_storage = {}

        logger.verbose('Got the actual chunks map for PROGRESS: %r', result)
        return result


    @exceptions_logged(logger)
    def __try_start_transaction(self):
        """Try to start the PROGRESS transaction.

        It is possible that you don't need to start it actually,
        if the data is empty.

        @note: Deferred callback: exceptions logged.
        """
        assert not in_main_thread()
        logger.debug('Trying to start chunks progress...')

        should_launch_tr = False
        with self.__notification_storage_lock:
            if self.__notification_storage and self.__waiting_for_transaction:
                logger.debug('Starting chunks progress!')
                should_launch_tr = True  # and leave the lock asap!
                # NotificationGetter will deal with the lock properly itself.
                self.__waiting_for_transaction = False

        if should_launch_tr:
            notif_getter = ChunkProgressNotificator.NotificationGetter(self)
            tr = self.app.tr_manager.create_new_transaction(
                     name='PROGRESS',
                     src=self.app.host,
                     dst=self.app.primary_node,
                     parent=None,
                     # PROGRESS-specific
                     host_chunks_map_getter=notif_getter)


    @contract_epydoc
    def add(self, dst_peer, chunks, end_ts, duration):
        """
        Add a notification that some chunks were successfully uploaded
        to the dst_peer on end_ts, taking duration.

        @type chunks: (list, set)
        @precondition: consists_of(chunks, Chunk)

        @param dst_peer: The Host,
                         to which the chunks were uploaded before.
        @type dst_peer: Host

        @param end_ts: When the upload of these chunks was completed?
        @type end_ts: datetime

        @param duration: How much time did the upload of the chunks took?
        @type duration: timedelta

        @rtype: Deferred
        """
        assert not in_main_thread()
        result_deferred = Deferred()
        _notification = \
            ProgressNotificationPerHostWithDeferred(chunks=chunks,
                                                    end_ts=end_ts,
                                                    duration=duration,
                                                    deferred=result_deferred)

        logger.debug('Adding progress... ')

        should_launch_calllater = False

        with self.__notification_storage_lock:
            # First, create and add the information about the group of chunks.
            self.__notification_storage.setdefault(dst_peer, []) \
                                     .append(_notification)

            # Then, if the transaction is not yet pending,
            # let's plan to run the transaction in several seconds.
            if not self.__waiting_for_transaction:
                should_launch_calllater = self.__waiting_for_transaction = True
                # ... and leave the lock ASAP!

                # Since this moment, self.__notification_storage_lock
                # is redundant.

        # Outside of the lock!!!
        if should_launch_calllater:
            callLaterInThread(WAIT_TO_COLLECT_PROGRESS_BEFORE_SEND,
                              self.__try_start_transaction)

        logger.debug('Will wait for %i chunks to %r with %r',
                     len(chunks), dst_peer, result_deferred)

        return result_deferred
