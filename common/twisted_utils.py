#!/usr/bin/python
"""Twisted specific utilitary functions."""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from threading import Lock

from twisted.application import service
from twisted.internet import error as internet_error, reactor

from .utils import exceptions_logged, in_main_thread



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Functions
#

def callFromThread(callable, *args, **kwargs):
    """Cause a function to be executed by the reactor thread.

    @note: this function most definitely can be called from any thread,
    either reactor one or a thread from threadpool.
    @note: the callable argument is automatically wrapped with
        C{@exceptions_logged(logger)}.
    """
    _callFromThread = reactor.callFromThread  # pylint:disable=E1101,C0103

    f = exceptions_logged(logger)(callable)
    _callFromThread(f, *args, **kwargs)


def callInThread(callable, *args, **kwargs):
    """Run the callable object in a separate thread.

    @note: this function most definitely can be called from any thread,
    either reactor one or a thread from threadpool.
    @note: the callable argument is automatically wrapped with
        C{@exceptions_logged(logger)}.
    """
    # pylint:disable=E1101,C0103
    _callFromThread = reactor.callFromThread
    _callInThread = reactor.callInThread
    # pylint:enable=E1101,C0103

    f = exceptions_logged(logger)(callable)
    _callFromThread(_callInThread, f, *args, **kwargs)


def callLater(delay, callable, *args, **kwargs):
    """Cause a function to be executed in the reactor thread after some delay.

    @note: this function most definitely can be called from any thread,
    either reactor one or a thread from threadpool.
    @note: the callable argument is automatically wrapped with
        C{@exceptions_logged(logger)}.
    """
    # pylint:disable=E1101,C0103
    _callFromThread = reactor.callFromThread
    _callLater = reactor.callLater
    # pylint:enable=E1101,C0103

    f = exceptions_logged(logger)(callable)
    _callFromThread(_callLater, delay,
                    f, *args, **kwargs)


def callLaterInThread(delay, callable, *args, **kwargs):
    """Cause a function to be executed in the threadpool after some delay.

    @note: this function most definitely can be called from any thread,
    either reactor one or a thread from threadpool.
    @note: the callable argument is automatically wrapped with
        C{@exceptions_logged(logger)}.
    """
    # pylint:disable=E1101,C0103
    _callFromThread = reactor.callFromThread
    _callInThread = reactor.callInThread
    _callLater = reactor.callLater
    # pylint:enable=E1101,C0103

    f = exceptions_logged(logger)(callable)
    _callFromThread(_callLater, delay,
                    _callInThread, f, *args, **kwargs)



#
# Classes
#

class DelayService(service.Service, object):
    """
    The hybrid of C{TimerService} and C{callLater}, relaunching the iterations
    after a specified period has elapsed since the previous iteration
    B{was completed} (rather than, as C{TimerService} does, when
    the previous iteration was started).

    It tries to mimic the C{t.a.s.IService}, in particular supporting
    C{.startService()} and C{.stopService()} methods.

    Every subclass must contain a C{_do_iteration} method that performs
    the desired actions executed regularly.

    Assume the code in C{_do_iteration} will be called in the thread pool
        (rather than in the reactor thread).
    Any exception raised in C{_do_iteration} will be logged, but won't stop
    the future iterations.
    """

    __slots__ = ('period', '__launched', '__next_iteration_lock',
                 '__next_iteration_delayed_call')

    __metaclass__ = ABCMeta


    def __init__(self, period):
        """Constructor.

        @type period: timedelta
        """
        self.period = period
        self.__launched = False
        self.__next_iteration_lock = Lock()
        self.__next_iteration_delayed_call = None


    def startService(self):
        """To mimic C{t.a.s.IService} interface."""
        assert in_main_thread()

        logger.debug('Starting service...')

        assert not self.__launched
        self.__launched = True

        self.__run_next_iteration()


    def stopService(self):
        """To mimic C{t.a.s.IService} interface."""
        assert in_main_thread()

        logger.debug('Stopping service...')

        assert self.__launched
        self.__launched = False

        if self.__next_iteration_delayed_call is not None:
            try:
                self.__next_iteration_delayed_call.cancel()
            except internet_error.AlreadyCalled:
                pass  # we wanted exactly that!


    def __run_next_iteration(self):
        """Go to the next iteration of the app."""
        assert in_main_thread()

        with self.__next_iteration_lock:
            if self.__launched:
                # pylint:disable=E1101,C0103
                _callLater = reactor.callLater
                _callInThread = reactor.callInThread
                # pylint:enable=E1101,C0103
                self.__next_iteration_delayed_call = \
                    _callLater(self.period.total_seconds(),
                               lambda: _callInThread(self.__on_next_iteration))
            else:
                logger.debug('Emergency stop!')


    @exceptions_logged(logger)
    def __on_next_iteration(self):
        """Handle the next iteration of the app."""
        assert not in_main_thread()

        # Do something on every iteration.
        logger.verbose('Next iteration of %r...', self)

        try:
            self._do_iteration()
        except:
            logger.exception('Problem during the iteration!')
        finally:
            # ... N. Whatever happens, we'll go to another iteration anyway.
            callFromThread(self.__run_next_iteration)


    @abstractmethod
    def _do_iteration(self):
        """Override me!"""
        pass



class DelayServiceForCallback(DelayService):
    """An implementation of C{DelayService} calling a given callback regularly.
    """

    __slots__ = ('__callback',)


    def __init__(self, callback, *args, **kwargs):
        """Constructor.

        @param callback: a callable which will be called by the service
            on every iteration.
        @type callback: col.Callable
        """
        super(DelayServiceForCallback, self).__init__(*args, **kwargs)
        self.__callback = callback


    def _do_iteration(self):
        """Implements the interface from C{DelayService}."""
        self.__callback()
