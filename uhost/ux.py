#!/usr/bin/python
"""
Classes in this module used to hold host_process states, to handle its events,
and to provide aggregated and adapted information about inner processes
in host_process in more human-readable form.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from collections import namedtuple
from functools import partial
from heapq import heapify, heappush, heappop

from twisted.internet import task, reactor

from common.fsm import (TransitionError, Transition,
                        FSM, Event, AbstractEventHandler,
                        EventHandlerWithCallbacks)
from common.utils import coalesce, exceptions_logged, NULL_UUID, in_main_thread
from common.twisted_utils import callFromThread
from common.inhabitants import Node

from untrusted.logger import StatusEventsOnlyFilter


#
# Constants
#

logger = logging.getLogger(__name__)
logger_ux_host_idle_state = logging.getLogger('ux.host_idle_state')
logger_ux_host_topmost_occupation = \
    logging.getLogger('ux.host_topmost_occupation')
logger_ux_restore_status = logging.getLogger('ux.restore_status')
logger_ux_backup_status = logging.getLogger('ux.backup_status')

DATA_TRANSFER_STATUS_ANNOUNCE_TIMEOUT = 5  # seconds
DATA_TRANSFERS_MAINTENANCE_TIMEOUT = 1 * 60  # seconds
IDLE_FSM_MAINTENANCE_TIMEOUT = 1 * 60  # seconds
OCCUPATION_ANNOUNCE_TIMEOUT = 10  # seconds


#
# Classes
#

class AdaptationError(Exception):
    pass


class UXException(Exception):
    pass



LogEvent = namedtuple('LogEvent', ('logger', 'message', 'extra'))
OccupationWithPriority = namedtuple('OccupationWithPriority',
                                    ('priority', 'name'))


class LoggingHandlerWithCallbackInReactor(logging.Handler):
    """
    Forwards received logrecords to callback to be executed in reactor thread.
    """
    __slots__ = ('callback',)

    def __init__(self, callback=None):
        """Constructor.

        @param callback: This callback will receive events from this handler.
        @type callback: col.Callable
        """
        super(LoggingHandlerWithCallbackInReactor, self).__init__()

        dummy_callback = lambda ev_name, ev_data: None
        self.callback = coalesce(callback, dummy_callback)


    def emit(self, record):
        """Forwards log record executing C{self.callback} in reactor thread."""
        callFromThread(self.callback, record)



class AbstractLogRecordHandler(AbstractEventHandler):
    """
    Base class for event handlers that can handle logrecords
    for wich adapter is specfied.
    """

    __slots__ = ('logrecord_event_adapters', 'logrecord_event_adapters')


    def __init__(self, logrecord_event_adapters=None, *args, **kwargs):
        """
        @param logrecord_event_adapters: logrecord name -> function
            function converts logrecord to event
        @type logrecord_event_adapters: col.Mapping
        """
        super(AbstractLogRecordHandler, self).__init__(*args, **kwargs)
        if logrecord_event_adapters is None:
            logrecord_event_adapters = {}
        self.logrecord_event_adapters = logrecord_event_adapters
        self.listened_log_names = self.logrecord_event_adapters.viewkeys()


    def get_logrecord_event_adapter(self, record_name):
        """
        @type record_name: basestring

        @rtype: col.Callable
        """
        return self.logrecord_event_adapters[record_name]


    def handle_log_record(self, record):
        """Handles logrecord.

        @type record: logging.LogRecord
        """
        try:
            adapt = self.get_logrecord_event_adapter(record.name)
        except KeyError:
            raise TransitionError('Has no adapter for log record {!r}'
                                      .format(record))
        else:
            try:
                event = adapt(record)
            except AdaptationError as e:
                logger.debug('Can not adapt %r: %r', record, e)
            else:
                self.handle_event(event)



class UX(object):
    r"""
    Used for communication between registered event handlers (and fsm's).
    This class is not thread-safe.
    >>> class DummyHelloFSM(object):
    ...     def __init__(self, state_callback):
    ...         self.state_callback = state_callback
    ...         self.listened_event_names = {'hello', 'fsm_goodbye'}
    ...     def handle_event(self, event):
    ...         if event.name == 'hello':
    ...             self.state_callback(Event('fsm_hello'))
    ...         elif event.name == 'fsm_goodbye':
    ...             self.state_callback(Event('fsm_received_goodbye'))
    ...
    >>> class DummyGoodbyeFSM(object):
    ...     def __init__(self, state_callback):
    ...         self.state_callback = state_callback
    ...         self.listened_event_names = {'fsm_hello'}
    ...     def handle_event(self, event):
    ...         if event.name == 'fsm_hello':
    ...             self.state_callback(Event('fsm_goodbye'))
    >>> ux = UX()
    >>> intercepted_events = []
    >>> def wrapped_ux_handle_event(event):
    ...     intercepted_events.append(event)
    ...     ux.handle_event(event)
    >>> hello_fsm = DummyHelloFSM(wrapped_ux_handle_event)
    >>> goodbye_fsm = DummyGoodbyeFSM(wrapped_ux_handle_event)
    >>> ux.add_handler(hello_fsm)
    >>> ux.add_handler(goodbye_fsm)
    >>> ux.add_handler(object())  # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
        ...
    AttributeError: 'object' object has no attribute 'listened_event_names'
    >>> ux.handle_event(Event(name='hello'))
    >>> intercepted_events  # doctest: +NORMALIZE_WHITESPACE
    [Event(name='fsm_hello'),
     Event(name='fsm_goodbye'),
     Event(name='fsm_received_goodbye')]
    """

    def __init__(self):
        assert in_main_thread()
        self.log_name_to_handlers = {}
        self.event_name_to_handlers = {}
        self._init_log_handler()


    def _init_log_handler(self):
        root_logger = logging.getLogger()
        self.log_handler = \
            LoggingHandlerWithCallbackInReactor(self._on_log_record)
        self.log_handler.setLevel(logging.DEBUG)  # TODO: maybe logging.INFO?
        self.log_handler.addFilter(StatusEventsOnlyFilter())
        root_logger.addHandler(self.log_handler)


    @exceptions_logged(logger)
    def _on_log_record(self, record):
        assert in_main_thread()
        self.handle_log_record(record)


    @exceptions_logged(logger)
    def _on_event(self, event):
        assert in_main_thread()
        self.handle_event(event)


    def handle_log_record(self, record):
        """Forward log records to appropriate event handlers.

        @type record: logging.LogRecord
        """
        assert in_main_thread()
        handlers = self.log_name_to_handlers.get(record.name)
        if handlers is not None:
            for handler in handlers:
                handler.handle_log_record(record)


    def handle_event(self, event):
        """Forward events to appropriate event handlers.

        @type event: Event
        """
        assert in_main_thread()
        try:
            handlers = self.event_name_to_handlers[event.name]
        except KeyError:
            # Well, UX is just not interested in it then.
            pass
        else:
            for handler in handlers:
                handler.handle_event(event)


    def add_handler(self, handler):
        """Register ievent handler for events it interested in.

        If handler is instance of C{AbstractLogRecordHandler}, registers it for
        logrecords too.

        @type handler: AbstractEventHandler
        """
        assert in_main_thread()
        logger.debug('UX adding %r as event handler', handler)
        for event_name in handler.listened_event_names:
            self.event_name_to_handlers.setdefault(event_name, set()) \
                                       .add(handler)

        if isinstance(handler, AbstractLogRecordHandler):
            for log_name in handler.listened_log_names:
                self.log_name_to_handlers.setdefault(log_name, set()) \
                                         .add(handler)


    def remove_handler(self, handler):
        """Unregister event handler.

        @type handler: AbstractEventHandler
        """
        assert in_main_thread()
        handler.event_announce_callback = None

        listened_log_names = getattr(handler, 'listened_log_names')
        if listened_log_names is not None:
            for log_name in listened_log_names:
                self.log_name_to_handlers[log_name].remove(handler)

        listened_event_names = getattr(handler, 'listened_event_names')
        if listened_event_names is not None:
            for event_name in listened_event_names:
                self.event_name_to_handlers[event_name].remove(handler)



class UXEventForwarder(AbstractEventHandler):
    r"""
    Forwards received events (if know how to convert them to logrecord)
    to 'ux.' prefixed logs.
    >>> forwarder = UXEventForwarder()
    >>> forwarder.handle_event(Event(name='host_working'))
    >>> forwarder.handle_event(Event(name='host_idle'))
    >>> forwarder.handle_event(Event(name='restore_status',
    ...                              data={'of_bytes': 31, 'num_chunks': 1,
    ...                                    'of_chunks': 7, 'num_bytes': 1}))
    >>> forwarder.handle_event(Event(name='restore_idle',
    ...                              data={'of_bytes': 0, 'num_chunks': 0,
    ...                                    'of_chunks': 0, 'num_bytes': 0}))
    >>> forwarder.handle_event(Event(name='backup_status',
    ...                              data={'of_bytes': 10, 'num_chunks': 0,
    ...                                    'of_chunks': 2, 'num_bytes': 0}))
    >>> forwarder.handle_event(Event(name='backup_idle',
    ...                              data={'of_bytes': 0, 'num_chunks': 0,
    ...                                    'of_chunks': 0, 'num_bytes': 0}))
    >>> forwarder.handle_event(Event(name='sync_idle',
    ...                              data={'of_bytes': 0, 'num_chunks': 0,
    ...                                    'of_chunks': 0, 'num_bytes': 0}))
    """

    def __init__(self):
        super(UXEventForwarder, self).__init__()
        # Event -> LogEvent
        self.event_logevent_adapters = {
            'host_idle': self.adapt_host_idle_state,
            'host_working': self.adapt_host_idle_state,
            'host_topmost_occupation': self.adapt_host_topmost_occupation,
            'restore_status': self.adapt_restore_status,
            'backup_status': self.adapt_backup_status,
        }
        self.listened_event_names = self.event_logevent_adapters.viewkeys()

    def adapt_host_topmost_occupation(self, event):
        return LogEvent(logger_ux_host_topmost_occupation,
                        'Topmost host occupation: {occupation!r}',
                        extra={'occupation': event.data})

    def adapt_host_idle_state(self, event):
        (state, flag) = ('idle', True) if event.name == 'host_idle' \
                                       else ('working', False)
        return LogEvent(logger_ux_host_idle_state,
                        'Current host state: {!r}'.format(state),
                        extra={'flag': flag})


    def adapt_restore_status(self, event):
        return self._adapt_data_transfer_status(event, 'restore',
                                                logger_ux_restore_status)


    def adapt_backup_status(self, event):
        return self._adapt_data_transfer_status(event, 'backup',
                                                logger_ux_backup_status)


    def _adapt_data_transfer_status(self, event, name, _logger):
        extra = dict(event.data)
        return LogEvent(_logger,
                        'Status {name!s}'.format(name=name)
                        + ' chunks: {num_chunks!s}/{of_chunks!s}'
                        + ' bytes: {num_bytes!s}/{of_bytes!s}',
                        extra=extra)


    def get_event_logevent_adapter(self, event_name):
        return self.event_logevent_adapters[event_name]


    def handle_event(self, event):
        try:
            adapt = self.get_event_logevent_adapter(event.name)
        except KeyError:
            logger.debug('Handler has no event -> logevent'
                           'adapters for %r', event)
        else:
            _logger, message, extra = adapt(event)
            _logger.info(message.format(**extra), extra=extra)



class UXFSM(FSM, EventHandlerWithCallbacks, AbstractLogRecordHandler):
    r"""Used as FSM in UX.

    Callback execution order:
        1. event_callback
        2. transition_test_callback
        3. transition_callback
        4. event_announce_callback

    >>> from common.logger import add_verbose_level; add_verbose_level()
    >>> states = ['green', 'yellow', 'red']
    >>> initial_state = 'green'

    >>> class DummyLogRecord(object):
    ...     def __init__(self, name, extra=None):
    ...         self.name = name
    ...         if not extra:
    ...             extra = {}
    ...         for key, value in extra.iteritems():
    ...             self.__setattr__(key, value)
    >>> def adapt_action_log_record(record):
    ...     return Event(record.action)
    >>> logrecord_event_adapters = {'status.action': adapt_action_log_record}

    >>> transitions = [
    ...     Transition(event_names=['warn', 'increase'],
    ...                src_or_srcs='green',
    ...                dst='yellow'),
    ...     Transition(event_names=['panic', 'increase'],
    ...                src_or_srcs='yellow',
    ...                dst='red'),
    ...     Transition(event_names=['calm', 'decrease'],
    ...                src_or_srcs='yellow',
    ...                dst='green'),
    ...     Transition(event_names=['clear'],
    ...                src_or_srcs=['green', 'yellow', 'red'],
    ...                dst='green')
    ... ]

    >>> fsm = UXFSM(
    ...     states, transitions, initial_state,
    ...     logrecord_event_adapters=logrecord_event_adapters, silent=False)

    >>> warn = DummyLogRecord('status.action', extra={'action': 'warn'})
    >>> increase = DummyLogRecord('status.action',
    ...                           extra={'action': 'increase'})
    >>> clear = DummyLogRecord('status.action', extra={'action': 'clear'})

    >>> fsm.current_state
    'green'

    >>> fsm.handle_log_record(warn)
    >>> fsm.current_state
    'yellow'

    >>> fsm.handle_log_record(warn)  # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    TransitionError: Can not find transition for
        Event(name='warn') while in state 'yellow'
    >>> fsm.handle_log_record(increase)
    >>> fsm.current_state
    'red'

    >>> fsm.handle_log_record(clear)
    >>> fsm.current_state
    'green'
    """

    __slots__ = ()



class DataTransferFSM(UXFSM):
    """
    This is a base class for FSMs, that handle restore/backup/other kind of
    events and stores some state and aggregations about restore/backup/other.

    Periodicaly announces its state and aggregated data.
    """

    def __init__(self, fsm_name, dummy_event, event_announce_callback,
                 status_announce_timeout=DATA_TRANSFER_STATUS_ANNOUNCE_TIMEOUT,
                 maintenance_timeout=DATA_TRANSFERS_MAINTENANCE_TIMEOUT,
                 logrecord_event_adapters=None, eventname_callback_map=None):
        """Constructor.

        @param fsm_name: Name for this fsm (used as prefix for event names).
        @type fsm_name: basestring

        @param dummy_event: This event used for periodic cleanup.
            of aggregations.
        @type dummy_event: Event

        @param event_announce_callback: Check C{FSM}.
        @type event_announce_callback: col.Mapping

        @param status_announce_timeout: Timeout in seconds for status announce.
        @type status_announce_timeout: int

        @param maintenance_timeout: Timeout in seconds for maintenance.
        @type maintenance_timeout: int

        @param logrecord_event_adapters: Check C{AbstractLogRecordHandler}.
        @type logrecord_event_adapters: col.Mapping

        @param eventname_callback_map: Check C{EventHandlerWithCallbacks}.
        @type eventname_callback_map: col.Mapping
        """
        if eventname_callback_map is None:
            eventname_callback_map = {}
        progress_event_name = fsm_name + '_progress'
        status_event_name = fsm_name + '_status'
        progress_event_names = eventname_callback_map.keys()
        progress_event_names.append(progress_event_name)
        states = ['idle', 'working']
        initial_state = 'idle'
        transitions = [
            Transition(src_or_srcs='idle', dst='working',
                       test_func=self.can_transit_to_working,
                       event_names=progress_event_names),
            Transition(src_or_srcs='working', dst='idle',
                       test_func=self.can_transit_to_idle,
                       callback=self.on_transit_to_idle,
                       event_names=progress_event_names),
        ]
        eventname_callback_map.update({
            progress_event_name: self.on_progress_event
        })
        state_event_adapters = {
            'idle': self.state_to_event,
            'working': self.state_to_event
        }

        super(DataTransferFSM, self).__init__(states, transitions,
                                              initial_state,
                                              event_announce_callback,
                                              state_event_adapters,
                                              eventname_callback_map,
                                              logrecord_event_adapters,
                                              silent=True)
        self.fsm_name = fsm_name
        self.dummy_event = dummy_event
        self.progress_event_name = progress_event_name
        self.status_event_name = status_event_name
        self.running_transfers = {}
        self.done_transfers = {}
        self.last_update_ts = datetime.utcnow()

        self.aggr_of_chunks = 0
        self.aggr_num_chunks = 0
        self.aggr_of_bytes = 0
        self.aggr_num_bytes = 0

        if status_announce_timeout is not None:
            self.status_announce_timer = task.LoopingCall(
                                            self._on_status_announce_timeout)
            self.status_announce_timer.start(status_announce_timeout)
        if maintenance_timeout is not None and dummy_event is not None:
            self.running_maintenance_timer = \
                task.LoopingCall(self._on_running_maintenance)
            self.running_maintenance_timer.start(maintenance_timeout)


    def can_transit_to_working(self, event):
        if self.aggr_num_chunks < self.aggr_of_chunks:
            return True
        else:
            return False


    def can_transit_to_idle(self, event):
        if self.aggr_num_chunks >= self.aggr_of_chunks:
            return True
        else:
            return False


    def announce_status(self):
        ev_name = self.status_event_name
        extra_data = {'num_chunks': self.aggr_num_chunks,
                      'of_chunks': self.aggr_of_chunks,
                      'num_bytes': self.aggr_num_bytes,
                      'of_bytes': self.aggr_of_bytes}
        self.event_announce_callback(Event(ev_name, extra_data))


    def on_transit_to_idle(self, event):
        self._done()


    def state_to_event(self, state):
        return Event(name=self.fsm_name + '_switched_to_' + state)


    def store_progress_data(self, uuid, num_chunks, of_chunks,
                            num_bytes, of_bytes):
        """
        Stores and calculates data from progress events.
        """
        if num_bytes > of_bytes:
            num_bytes = of_bytes
        if num_chunks > of_chunks:
            num_chunks = of_chunks
        self.last_update_ts = datetime.utcnow()
        if uuid in self.running_transfers:
            transfer = self.running_transfers[uuid]
            self.aggr_of_chunks += of_chunks - transfer['of_chunks']
            self.aggr_num_chunks += num_chunks - transfer['num_chunks']
            self.aggr_of_bytes += of_bytes - transfer['of_bytes']
            self.aggr_num_bytes += num_bytes - transfer['num_bytes']

            transfer.update({'of_chunks': of_chunks,
                             'num_chunks': num_chunks,
                             'of_bytes': of_bytes,
                             'num_bytes': num_bytes})
        else:
            self.running_transfers[uuid] = {'num_chunks': num_chunks,
                                            'of_chunks': of_chunks,
                                            'num_bytes': num_bytes,
                                            'of_bytes': of_bytes}
            self.aggr_of_chunks += of_chunks
            self.aggr_num_chunks += num_chunks
            self.aggr_of_bytes += of_bytes
            self.aggr_num_bytes += num_bytes


    def _done(self):
        """
        Do some cleanup after transition to idle state.
        """
        self.aggr_num_chunks = self.aggr_of_chunks = 0
        self.aggr_num_bytes = self.aggr_of_bytes = 0
        self.done_transfers = self.running_transfers
        self.done_transfers.pop(NULL_UUID, None)
        self.running_transfers = {}


    @exceptions_logged(logger)
    def _on_running_maintenance(self):
        """
        Do some mantenance cleanup. Removes data about old transactions,
        that were not updated for a long time.
        """
        timeout = timedelta(minutes=5)
        if self.last_update_ts is not None and \
                self.last_update_ts < datetime.utcnow() - timeout:
            self._done()
            self.done_transfers = {}

        self.handle_event(self.dummy_event)


    @exceptions_logged(logger)
    def _on_status_announce_timeout(self):
        self.announce_status()


    def on_progress_event(self, event):
        pass



class RestoreFSM(DataTransferFSM):
    """
    Handles events about restore progress, stores data about it.
    Check C{DataTransferFSM}.

    >>> from common.logger import add_verbose_level; add_verbose_level()
    >>> class DummyLogRecord(object):
    ...     def __init__(self, name, extra=None):
    ...         self.name = name
    ...         if not extra:
    ...             extra = {}
    ...         for key, value in extra.iteritems():
    ...             self.__setattr__(key, value)
    >>> announced_events = []
    >>> def dummy_callback(event):
    ...     announced_events.append(event)
    >>> restore_fsm = RestoreFSM(dummy_callback)
    >>> test_progress1 = DummyLogRecord('status.restore.progress',
    ...     extra={'progresses': [{'uuid':'asd1', 'num': 0, 'of': 2,
    ...                            'num_bytes': 0, 'of_bytes': 10},
    ...                           {'uuid':'asd2', 'num': 0, 'of': 2,
    ...                            'num_bytes': 0, 'of_bytes': 10},
    ...                           {'uuid':'asd3', 'num': 0, 'of': 2,
    ...                            'num_bytes': 0, 'of_bytes': 10}]})
    >>> test_progress2 = DummyLogRecord('status.restore.progress',
    ...     extra={'progresses': [{'uuid':'asd1', 'num': 3, 'of': 2,
    ...                            'num_bytes': 12, 'of_bytes': 10},
    ...                           {'uuid':'asd2', 'num': 1, 'of': 2,
    ...                            'num_bytes': 5, 'of_bytes': 10},
    ...                           {'uuid':'asd3', 'num': 0, 'of': 2,
    ...                            'num_bytes': 0, 'of_bytes': 10}]})
    >>> test_progress3 = DummyLogRecord('status.restore.progress',
    ...     extra={'progresses': [{'uuid':'asd1', 'num': 2, 'of': 2,
    ...                            'num_bytes': 10, 'of_bytes': 10},
    ...                           {'uuid':'asd2', 'num': 2, 'of': 2,
    ...                            'num_bytes': 10, 'of_bytes': 10},
    ...                           {'uuid':'asd3', 'num': 3, 'of': 2,
    ...                            'num_bytes': 12, 'of_bytes': 10}]})
    >>> test_progress4 = DummyLogRecord('status.restore.progress',
    ...     extra={'progresses': [{'uuid':'asd4', 'num': 2, 'of': 2,
    ...                            'num_bytes': 10, 'of_bytes': 10},
    ...                           {'uuid':'asd5', 'num': 2, 'of': 2,
    ...                            'num_bytes': 10, 'of_bytes': 10},
    ...                           {'uuid':'asd1', 'num': 0, 'of': 2,
    ...                            'num_bytes': 0, 'of_bytes': 10}]})
    >>> test_progress5 = DummyLogRecord('status.restore.progress',
    ...     extra={'progresses': [{'uuid':'asd5', 'num': 1, 'of': 2,
    ...                            'num_bytes': 5, 'of_bytes': 10},
    ...                           {'uuid':'asd6', 'num': 2, 'of': 2,
    ...                            'num_bytes': 10, 'of_bytes': 10},
    ...                           {'uuid':'asd7', 'num': 0, 'of': 2,
    ...                            'num_bytes': 0, 'of_bytes': 10}]})
    >>> restore_fsm.handle_log_record(test_progress1)
    >>> restore_fsm.current_state
    'working'
    >>> restore_fsm.handle_log_record(test_progress2)
    >>> restore_fsm.current_state
    'working'
    >>> restore_fsm.handle_log_record(test_progress3)
    >>> restore_fsm.current_state
    'idle'
    >>> # ignored, due this progresses have done recently
    >>> restore_fsm.handle_log_record(test_progress1)
    >>> restore_fsm.current_state
    'idle'
    >>> restore_fsm.handle_log_record(test_progress4)
    >>> restore_fsm.current_state
    'idle'
    >>> restore_fsm.handle_log_record(test_progress5)
    >>> restore_fsm.current_state
    'working'
    >>> announced_events  # doctest: +NORMALIZE_WHITESPACE
    [Event(name='restore_status', data={'of_bytes': 0, 'num_chunks': 0,
                                        'of_chunks': 0, 'num_bytes': 0}),
     Event(name='restore_switched_to_working'),
     Event(name='restore_switched_to_idle'),
     Event(name='restore_switched_to_working')]
    """

    def __init__(self, event_announce_callback=None):
        logrecord_event_adapters = {
            'status.restore.progress': self.adapt_progress,
        }
        fsm_name = 'restore'
        dummy_event = Event('restore_progress',
                            data=[{'uuid': NULL_UUID,
                                   'num': 0,
                                   'of': 0,
                                   'num_bytes': 0,
                                   'of_bytes': 0}])
        super(RestoreFSM, self).__init__(
            fsm_name, dummy_event, event_announce_callback,
            logrecord_event_adapters=logrecord_event_adapters)


    def adapt_progress(self, record):
        for progress in record.progresses:
            assert all(key in progress
                           for key in ['uuid', 'num', 'of',
                                       'num_bytes', 'of_bytes']), \
                   repr(key)
        return Event(self.progress_event_name, data=record.progresses)


    def _adapt_event_data(self, data):
        return (data['uuid'], data['num'], data['of'],
                data['num_bytes'], data['of_bytes'])


    def on_progress_event(self, event):
        for progress_data in event.data:
            uuid, num_chunks, of_chunks, num_bytes, of_bytes = \
                self._adapt_event_data(progress_data)
            if any((not of_chunks, not of_bytes, uuid in self.done_transfers)):
                return
            self.store_progress_data(uuid, num_chunks, of_chunks,
                                     num_bytes, of_bytes)



class BackupFSM(DataTransferFSM):
    """
    Handles events about backup progress, stores data about it.
    Check C{DataTransferFSM}.

    >>> from common.logger import add_verbose_level; add_verbose_level()
    >>> class DummyLogRecord(object):
    ...     def __init__(self, name, extra=None):
    ...         self.name = name
    ...         if not extra:
    ...             extra = {}
    ...         for key, value in extra.iteritems():
    ...             self.__setattr__(key, value)
    >>> announced_events = []
    >>> def dummy_callback(event):
    ...     announced_events.append(event)
    >>> backup_fsm = BackupFSM(dummy_callback)
    >>> test_progress1 = DummyLogRecord('status.backup.progress',
    ...                                  {'ds_uuid':'asd1', 'num': 10, 'of': 2,
    ...                                   'num_bytes': 12, 'of_bytes': 10})
    >>> test_progress2 = DummyLogRecord('status.backup.progress',
    ...                                  {'ds_uuid':'asd2', 'num': 1, 'of': 2,
    ...                                   'num_bytes': 5, 'of_bytes': 10})
    >>> test_progress3 = DummyLogRecord('status.backup.progress',
    ...                                  {'ds_uuid':'asd2', 'num': 0, 'of': 2,
    ...                                   'num_bytes': 0, 'of_bytes': 10})
    >>> test_progress4 = DummyLogRecord('status.backup.progress',
    ...                                  {'ds_uuid':'asd2', 'num': 2, 'of': 2,
    ...                                   'num_bytes': 10, 'of_bytes': 10})
    >>> test_progress5 = DummyLogRecord('status.backup.progress',
    ...                                  {'ds_uuid':'asd3', 'num': 0, 'of': 2,
    ...                                   'num_bytes': 0, 'of_bytes': 10})
    >>> test_progress6 = DummyLogRecord('status.backup.progress',
    ...                                  {'ds_uuid':'asd3', 'num': 2, 'of': 2,
    ...                                   'num_bytes': 10, 'of_bytes': 10})
    >>> backup_fsm.handle_log_record(test_progress1)
    >>> backup_fsm.current_state
    'idle'
    >>> backup_fsm.handle_log_record(test_progress2)
    >>> backup_fsm.current_state
    'working'
    >>> backup_fsm.handle_log_record(test_progress3)
    >>> backup_fsm.current_state
    'working'
    >>> backup_fsm.handle_log_record(test_progress4)
    >>> backup_fsm.current_state
    'idle'
    >>> # ignored, since this progresses have done recently
    >>> backup_fsm.handle_log_record(test_progress1)
    >>> backup_fsm.current_state
    'idle'
    >>> backup_fsm.handle_log_record(test_progress5)
    >>> backup_fsm.current_state
    'working'
    >>> backup_fsm.handle_log_record(test_progress6)
    >>> backup_fsm.current_state
    'idle'
    >>> announced_events  # doctest: +NORMALIZE_WHITESPACE
    [Event(name='backup_status', data={'of_bytes': 0, 'num_chunks': 0,
                                       'of_chunks': 0, 'num_bytes': 0}),
     Event(name='backup_switched_to_working'),
     Event(name='backup_switched_to_idle'),
     Event(name='backup_switched_to_working'),
     Event(name='backup_switched_to_idle')]
    """

    def __init__(self, event_announce_callback=None):
        logrecord_event_adapters = {
            'status.backup.progress': self.adapt_progress,
            'status.backup': self.adapt_status,
        }
        fsm_name = 'backup'
        dummy_event = Event('backup_progress',
                             data={'uuid': NULL_UUID,
                                   'num': 0,
                                   'of': 0,
                                   'num_bytes': 0,
                                   'of_bytes': 0})
        super(BackupFSM, self).__init__(
            fsm_name, dummy_event, event_announce_callback,
            logrecord_event_adapters=logrecord_event_adapters)


    def adapt_progress(self, record):
        return Event(self.progress_event_name,
                     data={'uuid': record.ds_uuid,
                           'num': record.num,
                           'of': record.of,
                           'num_bytes': record.num_bytes,
                           'of_bytes': record.of_bytes})


    def adapt_status(self, record):
        if record.status in ['fail', 'ok']:
            num = of = num_bytes = of_bytes = 0
            return Event(self.progress_event_name,
                         data={'uuid': record.ds_uuid,
                               'num': num,
                               'of': of,
                               'num_bytes': num_bytes,
                               'of_bytes': of_bytes})
        else:
            raise AdaptationError('Do not know how to adapt')


    def _adapt_event_data(self, data):
        return (data['uuid'], data['num'], data['of'],
                data['num_bytes'], data['of_bytes'])


    def on_progress_event(self, event):
        uuid, num_chunks, of_chunks, num_bytes, of_bytes = \
                self._adapt_event_data(event.data)
        if uuid in self.done_transfers:
            return
        if of_chunks != 0:
            self.store_progress_data(uuid, num_chunks, of_chunks,
                                     num_bytes, of_bytes)



class SyncFSM(UXFSM):
    """
    Handles events about sync progress, shows if there something going on,
    related to data syncronization between host and cloud.
    Check C{DataTransferFSM}.

    >>> from common.logger import add_verbose_level; add_verbose_level()
    >>> class DummyLogRecord(object):
    ...     def __init__(self, name, extra=None):
    ...         self.name = name
    ...         if not extra:
    ...             extra = {}
    ...         for key, value in extra.iteritems():
    ...             self.__setattr__(key, value)
    >>> cooling_down_to_store_started = DummyLogRecord(
    ...     'status.cooling_down_to_store', {'status': True})
    >>> cooling_down_to_store_ended = DummyLogRecord(
    ... 'status.cooling_down_to_store', {'status': False})
    >>> cooling_down_decayed = Event('sync_did_not_started_after_cooling_down')
    >>> restore_working = Event('restore_switched_to_working')
    >>> restore_idle = Event('restore_switched_to_idle')
    >>> backup_working = Event('backup_switched_to_working')
    >>> backup_idle = Event('backup_switched_to_idle')

    >>> announced_events = []
    >>> def dummy_callback(event):
    ...     announced_events.append(event)
    >>> sync_fsm = SyncFSM(dummy_callback)
    >>> sync_fsm.current_state
    'idle'
    >>> sync_fsm.handle_log_record(cooling_down_to_store_started)
    >>> sync_fsm.current_state
    'preparing_sync'
    >>> sync_fsm.handle_log_record(cooling_down_to_store_ended)
    >>> sync_fsm.current_state
    'preparing_sync'
    >>> sync_fsm.handle_event(restore_working)
    >>> sync_fsm.current_state
    'syncing'
    >>> sync_fsm.handle_event(backup_working)
    >>> sync_fsm.current_state
    'syncing'
    >>> sync_fsm.handle_event(restore_idle)
    >>> sync_fsm.current_state
    'syncing'
    >>> sync_fsm.handle_event(backup_idle)
    >>> sync_fsm.current_state
    'idle'
    >>> sync_fsm.handle_log_record(cooling_down_to_store_ended)
    >>> sync_fsm.current_state
    'preparing_sync'
    >>> sync_fsm.handle_event(cooling_down_decayed)
    >>> sync_fsm.current_state
    'idle'

    >>> announced_events  # doctest: +NORMALIZE_WHITESPACE
    [Event(name='sync_switched_to_preparing_sync'),
     Event(name='sync_switched_to_syncing'),
     Event(name='sync_switched_to_idle'),
     Event(name='sync_switched_to_preparing_sync'),
     Event(name='sync_switched_to_idle')]
    """

    def __init__(self, event_announce_callback=None,
                 maintenance_timeout=IDLE_FSM_MAINTENANCE_TIMEOUT):
        states = ['idle', 'preparing_sync', 'syncing']
        initial_state = 'idle'
        transitions = [
            Transition(src_or_srcs='idle', dst='preparing_sync',
                       callback=self.on_transit_to_preparing_to_sync,
                       event_names=['cooling_down_to_store_started',
                                    'cooling_down_to_store_ended'],),
            Transition(src_or_srcs=['idle', 'preparing_sync'], dst='syncing',
                       test_func=self.can_transit_to_syncing,
                       callback=self.on_transit_to_syncing,
                       event_names=['restore_switched_to_working',
                                    'backup_switched_to_working']),
            Transition(src_or_srcs='preparing_sync', dst='idle',
                       test_func=self.can_transit_to_idle,
                       callback=self.on_transit_to_idle,
                       event_names=
                           ['sync_did_not_started_after_cooling_down']),
            Transition(src_or_srcs='syncing', dst='idle',
                       test_func=self.can_transit_to_idle,
                       callback=self.on_transit_to_idle,
                       event_names=['restore_switched_to_idle',
                                    'backup_switched_to_idle']),
        ]
        logrecord_event_adapters = {
            'status.cooling_down_to_store': self.adapt_cooling_down_to_store,
        }

        eventname_callback_map = {
            'restore_switched_to_working':
                self.on_restore_switched_to_working,
            'restore_switched_to_idle':
                self.on_restore_switched_to_idle,
            'backup_switched_to_working':
                self.on_backup_switched_to_working,
            'backup_switched_to_idle':
                self.on_backup_switched_to_idle,
        }
        state_event_adapters = {
            'idle':
                lambda state: Event('sync_switched_to_idle'),
            'syncing':
                lambda state: Event('sync_switched_to_syncing'),
            'preparing_sync':
                lambda state: Event('sync_switched_to_preparing_sync'),
        }

        super(SyncFSM, self).__init__(
            states, transitions, initial_state, event_announce_callback,
            state_event_adapters, eventname_callback_map,
            logrecord_event_adapters=logrecord_event_adapters,
            silent=True)

        self.transfers_idle_state = {}
        self.preparing_sync_start_ts = None
        if maintenance_timeout is not None:
            self.maintenance_timer = \
                task.LoopingCall(self._on_maintenance_timeout)
            self.maintenance_timer.start(maintenance_timeout)


    @exceptions_logged(logger)
    def _on_maintenance_timeout(self):
        logger.debug('Sync did not started after cooling down,'
                     ' changing state to idle')
        timeout = timedelta(minutes=5)
        if self.preparing_sync_start_ts is not None and \
                self.preparing_sync_start_ts < datetime.utcnow() - timeout:
            self.handle_event(Event('sync_did_not_started_after_cooling_down'))

    def adapt_cooling_down_to_store(self, record):
        if record.status == True:
            return Event('cooling_down_to_store_started')
        elif record.status == False:
            return Event('cooling_down_to_store_ended')
        else:
            raise AdaptationError('Do not know what to do with {!r}'
                                      .format(record))

    def on_restore_switched_to_working(self, event):
        self.transfers_idle_state['restore'] = False

    def on_restore_switched_to_idle(self, event):
        self.transfers_idle_state['restore'] = True

    def on_backup_switched_to_working(self, event):
        self.transfers_idle_state['backup'] = False

    def on_backup_switched_to_idle(self, event):
        self.transfers_idle_state['backup'] = True

    def can_transit_to_idle(self, event):
        return all(self.transfers_idle_state.itervalues())

    def can_transit_to_syncing(self, event):
        return not all(self.transfers_idle_state.itervalues())

    def on_transit_to_preparing_to_sync(self, event):
        self.preparing_sync_start_ts = datetime.utcnow()

    def on_transit_to_syncing(self, event):
        self.preparing_sync_start_ts = None

    def on_transit_to_idle(self, event):
        self.preparing_sync_start_ts = None



class IdleFSM(UXFSM):
    r"""
    Handles events about host process processes, changes state based on them.
    Its current state holds info about - if host process now working on
    something or idle?

    >>> from common.logger import add_verbose_level; add_verbose_level()

    >>> sync_switched_to_preparing_sync = Event(
    ...     'sync_switched_to_preparing_sync')
    >>> sync_switched_to_syncing = Event('sync_switched_to_syncing')
    >>> sync_switched_to_idle = Event('sync_switched_to_idle')

    >>> announced_events = []
    >>> def dummy_event_announce_callback(state_event):
    ...     announced_events.append(state_event)

    >>> fsm = IdleFSM(dummy_event_announce_callback)
    >>> fsm.current_state
    'idle'
    >>> fsm.handle_event(sync_switched_to_preparing_sync)
    >>> fsm.current_state
    'working'
    >>> fsm.handle_event(sync_switched_to_syncing)
    >>> fsm.current_state
    'working'
    >>> fsm.handle_event(sync_switched_to_idle)
    >>> fsm.current_state
    'idle'
    >>> fsm.handle_event(sync_switched_to_syncing)
    >>> fsm.current_state
    'working'
    >>> fsm.handle_event(sync_switched_to_idle)
    >>> fsm.current_state
    'idle'
    >>> announced_events  # doctest: +NORMALIZE_WHITESPACE
    [Event(name='host_working'), Event(name='host_idle'),
     Event(name='host_working'), Event(name='host_idle')]
    """

    def __init__(self, event_announce_callback=None, silent=True,
                 maintenance_timeout=IDLE_FSM_MAINTENANCE_TIMEOUT):
        states = ['idle', 'working']
        initial_state = 'idle'
        transitions = [
            Transition(src_or_srcs='idle', dst='working',
                       event_names=['sync_switched_to_syncing',
                                    'sync_switched_to_preparing_sync']
                       ),
            Transition(src_or_srcs='working', dst='idle',
                       test_func=self.can_transit_to_idle,
                       event_names=['sync_switched_to_idle']
                       ),
        ]
        eventname_callback_map = {
            'sync_switched_to_preparing_sync':
                self.on_sync_switched_to_preparing_sync,
            'sync_switched_to_syncing':
                self.on_sync_switched_to_syncing,
            'sync_switched_to_idle':
                self.on_sync_switched_to_idle
        }
        state_event_adapters = {'idle': lambda state: Event('host_idle'),
                                'working': lambda state: Event('host_working')}

        super(IdleFSM, self).__init__(states, transitions, initial_state,
                                      event_announce_callback,
                                      state_event_adapters,
                                      eventname_callback_map,
                                      silent=silent)
        self.idle_flags = {}

    def can_transit_to_idle(self, event):
        return all(self.idle_flags.itervalues())

    def on_sync_switched_to_preparing_sync(self, event):
        self.idle_flags['sync'] = False

    def on_sync_switched_to_syncing(self, event):
        self.idle_flags['sync'] = False

    def on_sync_switched_to_idle(self, event):
        self.idle_flags['sync'] = True



class NetworkConnectionFSM(UXFSM):
    """
    Status of network connection.

    >>> from common.logger import add_verbose_level; add_verbose_level()
    >>> class DummyLogRecord(object):
    ...     def __init__(self, name, extra=None):
    ...         self.name = name
    ...         if not extra:
    ...             extra = {}
    ...         for key, value in extra.iteritems():
    ...             self.__setattr__(key, value)
    >>> network_ok = DummyLogRecord(
    ...     'status.network_connection', {'status': 'ok'})
    >>> network_fail = DummyLogRecord(
    ...     'status.network_connection', {'status': 'fail'})
    >>> announced_events = []
    >>> def dummy_callback(event):
    ...     announced_events.append(event)
    >>> fsm = NetworkConnectionFSM(dummy_callback)
    >>> fsm.current_state
    'ok'
    >>> fsm.handle_log_record(network_fail)
    >>> fsm.current_state
    'failed'
    >>> fsm.handle_log_record(network_fail)
    >>> fsm.current_state
    'failed'
    >>> fsm.handle_log_record(network_ok)
    >>> fsm.current_state
    'ok'
    >>> announced_events  # doctest: +NORMALIZE_WHITESPACE
    [Event(name='network_switched_to_failed'),
     Event(name='network_switched_to_ok')]
    """
    def __init__(self, event_announce_callback=None):
        initial_state = 'ok'
        states = ['ok', 'failed']
        transitions = [
            Transition(src_or_srcs='failed', dst='ok',
                       event_names=['network_ok']),
            Transition(src_or_srcs='ok', dst='failed',
                       event_names=['network_fail']),
        ]
        logrecord_event_adapters = {
            'status.network_connection': self.adapt_network_connection,
        }
        state_event_adapters = {
            'ok':
                lambda state: Event('network_switched_to_ok'),
            'failed':
                lambda state: Event('network_switched_to_failed'),
        }
        super(NetworkConnectionFSM, self).__init__(
            states, transitions, initial_state, event_announce_callback,
            state_event_adapters,
            logrecord_event_adapters=logrecord_event_adapters,
            silent=True)

    def adapt_network_connection(self, record):
        if record.status == 'ok':
            return Event('network_ok')
        elif record.status == 'fail':
            return Event('network_fail')
        else:
            raise AdaptationError('Can not adapt record {!r}'.format(record))



class OccupationHandler(EventHandlerWithCallbacks, AbstractLogRecordHandler):
    """
    Handles events about processes taking place in host process,
    forwards events about currently most important occupation (task) on host.

    >>> from common.logger import add_verbose_level; add_verbose_level()

    >>> sync_switched_to_preparing_sync = Event(
    ...     'sync_switched_to_preparing_sync')
    >>> sync_switched_to_syncing = Event('sync_switched_to_syncing')
    >>> sync_switched_to_idle = Event('sync_switched_to_idle')
    >>> network_switched_to_ok = Event('network_switched_to_ok')
    >>> network_switched_to_failed = Event('network_switched_to_failed')

    >>> announced_events = []
    >>> def dummy_event_announce_callback(state_event):
    ...     announced_events.append(state_event)
    >>> occupation_handler = OccupationHandler(dummy_event_announce_callback)
    >>> occupation_handler.handle_event(sync_switched_to_preparing_sync)
    >>> occupation_handler.handle_event(network_switched_to_failed)

    >>> announced_events  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [Event(... data='preparing_sync'),
     Event(... data='network_connection_failed')]

    >>> occupation_handler.handle_event(sync_switched_to_syncing)
    >>> announced_events  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [Event(... data='preparing_sync'),
     Event(... data='network_connection_failed')]

    >>> occupation_handler.handle_event(network_switched_to_ok)
    >>> announced_events  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [Event(... data='preparing_sync'),
     Event(... data='network_connection_failed'),
     Event(... data='syncing')]

    >>> occupation_handler.handle_event(sync_switched_to_idle)
    >>> announced_events  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [Event(... data='preparing_sync'),
     Event(... data='network_connection_failed'),
     Event(... data='syncing'),
     Event(... data='sync_done')]
    """

    def __init__(self, event_announce_callback=None, silent=True,
                 occupation_announce_timeout=OCCUPATION_ANNOUNCE_TIMEOUT):
        """
        @param event_announce_callback: callable, that would receive events
            from this handler (topmost_occupation).
        @type: col.Callable

        @param occupation_announce_timeout: Timeout in seconds for current
            topmost occupation announce.
        @type occupation_announce_timeout: int

        """
        eventname_callback_map = {
            'sync_switched_to_preparing_sync':
                self.on_sync_switched_to_preparing_sync,
            'sync_switched_to_syncing':
                self.on_sync_switched_to_syncing,
            'sync_switched_to_idle':
                self.on_sync_switched_to_idle,
            'network_switched_to_ok':
                self.on_network_switched_to_ok,
            'network_switched_to_failed':
                self.on_network_switched_to_failed,
            'login_start':
                self.on_login_start,
            'login_failure':
                self.on_login_failure,
            'login_success':
                self.on_login_success,
        }
        logrecord_event_adapters = {
            'status.login_success': lambda r: Event('login_success'),
            'status.login_failure': lambda r: Event('login_failure'),
            'status.login_start': lambda r: Event('login_start'),
        }
        listened_event_names = eventname_callback_map.keys()
        super(OccupationHandler, self).__init__(
            eventname_callback_map, logrecord_event_adapters,
            listened_event_names, silent=silent)
        self.event_announce_callback = event_announce_callback
        self.current_occupations_heapq = []
        # less value - higher priority
        self.occupation_to_priority = {
            'idle': 20,
            'logged_in': 10,
            'sync_done': 5,
            'preparing_sync': 0,
            'syncing': -5,
            'login_failed': -9,
            'logging_in': -10,
            'network_connection_failed': -20,
        }
        self.current_occupations_heapq = [OccupationWithPriority(20, 'idle')]

    def on_sync_switched_to_preparing_sync(self, event):
        self.add_occupation('preparing_sync')

    def on_sync_switched_to_syncing(self, event):
        self.add_occupation('syncing')
        try:
            self.del_occupation('preparing_sync')
        except ValueError:
            pass

    def on_sync_switched_to_idle(self, event):
        self.add_obsolescent_occupation('sync_done')
        try:
            self.del_occupation('preparing_sync')
        except ValueError:
            pass
        try:
            self.del_occupation('syncing')
        except ValueError:
            pass


    def on_login_start(self, event):
        self.add_occupation('logging_in')
        try:
            self.del_occupation('logged_in')
        except ValueError:
            pass
        try:
            self.del_occupation('login_failed')
        except ValueError:
            pass

    def on_login_success(self, event):
        self.add_obsolescent_occupation('logged_in')
        try:
            self.del_occupation('logging_in')
        except ValueError:
            pass

    def on_login_failure(self, event):
        self.add_obsolescent_occupation('login_failed', seconds=3)
        try:
            self.del_occupation('logging_in')
        except ValueError:
            pass

    def on_network_switched_to_failed(self, event):
        self.add_occupation('network_connection_failed')

    def on_network_switched_to_ok(self, event):
        try:
            self.del_occupation('network_connection_failed')
        except ValueError:
            pass

    @property
    def topmost_occupation(self):
        return self.current_occupations_heapq[0].name

    def add_occupation(self, name):
        """
        Adds occupation to list of current occupations.
        """
        logger.verbose('Adding occupation %r', name)
        assert name in self.occupation_to_priority, repr(name)

        occupation_with_priority = \
                OccupationWithPriority(self.occupation_to_priority[name], name)
        if occupation_with_priority not in self.current_occupations_heapq:
            before = self.topmost_occupation
            heappush(self.current_occupations_heapq, occupation_with_priority)
            after = self.topmost_occupation

            if before != after:
                self.announce_topmost_occupation()

    def add_obsolescent_occupation(self, name, seconds=10):
        """
        Adds occupation, and schedules it's removal.
        """
        @exceptions_logged(logger)
        def delete_if_exist(name):
            try:
                self.del_occupation(name)
            except ValueError:
                pass

        self.add_occupation(name)
        reactor.callLater(seconds, delete_if_exist, name)
        logger.verbose('Added obsolescent occupation: %r,'
                       ' obsoletes in %r seconds', name, seconds)

    def del_occupation(self, name):
        """
        Removes occupation from list of current occupations.
        """
        assert name in self.occupation_to_priority, repr(name)
        #  do not remove `idle` from current_occupations
        assert name != 'idle'
        # do nothing for now
        occupation_with_priority = \
            OccupationWithPriority(self.occupation_to_priority[name], name)
        before = self.topmost_occupation
        self.current_occupations_heapq.remove(occupation_with_priority)
        heapify(self.current_occupations_heapq)
        after = self.topmost_occupation

        if before != after:
            self.announce_topmost_occupation()

    def announce_topmost_occupation(self):
        self.event_announce_callback(Event('host_topmost_occupation',
                                           data=self.topmost_occupation))
