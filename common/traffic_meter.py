#!/usr/bin/python
"""
The traffic meter is capable of monitoring the traffic passing
to some direction (either incoming or outgoing),
calculate momentary throughput and log it via some status logger.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import numbers
from collections import deque, namedtuple
from datetime import datetime, timedelta
from threading import Lock

from twisted.application.internet import TimerService

from contrib.dbc import contract_epydoc

from .datetime_ex import TimeDeltaEx
from .utils import exceptions_logged


#
# Constants
#

logger = logging.getLogger(__name__)
logger_traffic_meter = logging.getLogger('status.traffic.meter')

TRAFFIC_NOTIFICATION_PERIOD = timedelta(seconds=1)
"""
How often does a status log report about the throughput?
"""

THROUGHPUT_SMA_PERIOD = TimeDeltaEx(seconds=7.0)
"""
# What is the window of the Simple Moving Average which smooths
# the traffic peaks?
"""



#
# Classes
#

TrafficTick = namedtuple('TrafficTick', ('time', 'bytes'))


class TrafficMeter(object):
    """
    Traffic meter, measuring all the traffic to some direction.

    @note: At the moment, restart of the service (after it has been started)
           is not supported.

    @ivar __start_time: the time of the first registered traffic "tick".
    @type __start_time: datetime

    @ivar __ticks: the sequence of TrafficTick objects
                   (always sorted by the time, increasing!)
                   registering each successfully passed bunch of bytes.
    @invariant: consists_of(self.__ticks, TrafficTick)
    """
    __slots__ = ('__direction_incoming', '__total_bytes', '__ticks',
                 '__ticks_lock', '__start_time', '__timer_service',
                 '__started')


    def __init__(self, is_direction_incoming):
        """
        @param is_direction_incoming: whether the measured direction
            is incoming (otherwise outgoing).
        @type is_direction_incoming: bool
        """
        self.__direction_incoming = is_direction_incoming
        self.__total_bytes = 0
        self.__ticks = deque()
        self.__ticks_lock = Lock()
        self.__start_time = None
        self.__timer_service = TimerService(
                                   TRAFFIC_NOTIFICATION_PERIOD.total_seconds(),
                                   self.__on_timer)
        self.__started = False


    @property
    def started(self):
        return self.__started


    def start(self):
        """
        Start monitoring and reporting.
        """
        with self.__ticks_lock:
            assert not self.__started
            self.__timer_service.startService()
            self.__started = True
            logger.debug('%s traffic meter started', self._name)


    @property
    def _name(self):
        return 'Inbound' if self.__direction_incoming else 'Outbound'


    def stop(self):
        """
        Stop monitoring and reporting.
        """
        with self.__ticks_lock:
            assert self.__started
            self.__started = False
            self.__timer_service.stopService()
            logger.debug('%s traffic meter stopped', self._name)


    @exceptions_logged(logger)
    def __on_timer(self):
        _now = datetime.utcnow()
        _oldest_time_to_consider = _now - THROUGHPUT_SMA_PERIOD

        # Even if the timer is started, we don't report throughput
        # until the first ticks are registered.
        if self.__ticks:
            with self.__ticks_lock:
                # Remove (leftmost) ticks which are too old to consider.
                while (self.__ticks and
                       self.__ticks[0].time < _oldest_time_to_consider):
                    self.__ticks.popleft()

                # All the remaining ticks now serve for the throughput
                # calculation, and comprise the current SMA window.
                sma_kilobytes = sum(i.bytes for i in self.__ticks) / 1000.0

                # (btw, done with the lock, remaining calculation
                # can be unlocked)

            # How much time passed since the traffic meter has been started?
            uptime_td = TimeDeltaEx.from_timedelta(_now - self.__start_time)

            # If the meter has just been started,
            # we calculate the throughput dividing to the actual uptime
            # rather than the period window.
            sma_td = min(uptime_td, THROUGHPUT_SMA_PERIOD)
            sma_seconds = sma_td.in_seconds()

            kBps = sma_kilobytes / sma_seconds

            # Measurement unit is "kilobytes per second"
            # (not "kibi", not "bits"!)
            logger_traffic_meter.info('%s: %.02f kBps',
                                      'In' if self.__direction_incoming
                                           else 'Out',
                                      kBps,
                                      extra={'is_incoming':
                                                 self.__direction_incoming,
                                             'kBps': kBps})


    @contract_epydoc
    def add_tick(self, bytes):
        """
        Call this function manually to register a new data tick
        passing C{bytes} bytes.

        @param bytes: how many bytes passed in the tick.
        @type bytes: numbers.Number
        """
        assert self.__started
        assert isinstance(bytes, numbers.Number), repr(bytes)

        _now = datetime.utcnow()
        # logger.debug("%s traffic %i bytes", self._name, bytes)
        with self.__ticks_lock:
            self.__ticks.append(TrafficTick(time=_now, bytes=bytes))
            self.__total_bytes += bytes
            if self.__start_time is None:
                self.__start_time = _now
