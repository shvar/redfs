#!/usr/bin/python
"""Finite State Machine backend.

@note: thread-safe.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from types import NoneType
from threading import RLock

from contrib.dbc import contract_epydoc

from common.utils import coalesce



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class TransitionError(Exception):
    pass



class EventHandlingError(Exception):
    pass



class Event(object):
    """
    Used for communication with instances of subclasses
    of C{AbstractEventHandler}.
    """

    __slots__ = ('name', 'data')


    @contract_epydoc
    def __init__(self, name, data=None):
        r"""Constructor.

        >>> Event('hello', [{'from': 'Sara'}])
        Event(name='hello', data=[{'from': 'Sara'}])

        @type name: basestring

        @param data: Any extra data.
        """
        self.name = name
        self.data = data


    def __repr__(self):
        attr_str = ', '.join(s.format(attr)
                                 for s, attr in [('name={!r}', self.name),
                                                 ('data={!r}', self.data)]
                                 if attr is not None)
        return u'{}({})'.format(self.__class__.__name__, attr_str)



class Transition(object):
    """Describes transitions between states of C{FSM}."""

    __slots__ = ('srcs', 'dst', 'event_names', 'test_func', 'callback')


    @contract_epydoc
    def __init__(self, src_or_srcs, dst, event_names,
                 test_func=None, callback=None):
        r"""Constructor.

        >>> Transition('a', 'b', ['ev1','ev2','ev3'],
        ...            lambda event: True, callback=lambda event: None) \
        ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
        Transition(src_or_srcs=['a'], dst='b',
                   event_names=['ev1', 'ev2', 'ev3'],
                   test_func=<function ...>, callback=<function ...>)

        >>> Transition('a', 'b', ['ev1','ev2','ev3']) \
        ... # doctest: +NORMALIZE_WHITESPACE
        Transition(src_or_srcs=['a'], dst='b',
                   event_names=['ev1', 'ev2', 'ev3'],
                   test_func=None, callback=None)

        @param src_or_srcs: source state or states.
        @type src_or_srcs: basestring, col.Iterable

        @param dst: destination state.
        @type dst: basestring

        @param event_names: names of events which can trigger this transition.
        @type event_names: col.Iterable

        @param test_func: A function that used for checking if this transition
            can be done. Receives C{Event} as argument.
        @type test_func: col.Callable, NoneType

        @param callback: A function to be called when the transition complete.
        @type callback: col.Callable, NoneType
        """
        self.srcs = [src_or_srcs] if isinstance(src_or_srcs, basestring) \
                                  else list(src_or_srcs)
        self.dst = dst
        self.event_names = list(event_names)
        self.test_func = test_func
        self.callback = callback


    def __repr__(self):
        return '{!s}(src_or_srcs={!r}, dst={!r}, event_names={!r}, '\
               'test_func={!r}, callback={!r})' \
                   .format(self.__class__.__name__,
                           self.srcs, self.dst, self.event_names,
                           self.test_func, self.callback)



class AbstractEventHandler(object):
    """
    Base class for event handlers, to use with C{Event}.
    Drops all except events with names contained
    in C{self.listened_event_names}.
    This class is not thread-safe, and its ancestors not thread-safe too,
    unless stated otherwise.
    """
    __metaclass__ = ABCMeta


    def __init__(self, listened_event_names=None,
                 silent=True, *args, **kwargs):
        """Constructor.

        @param listened_event_names: names of events that would not be dropped.
        @type listened_event_names: col.Iterable, NoneType

        @param silent: should handler be silent about some errors?
        @type silent: bool
        """
        super(AbstractEventHandler, self).__init__(*args, **kwargs)
        self.silent = silent
        self.listened_event_names = set(coalesce(listened_event_names, []))


    @abstractmethod
    def handle_event(self, event):
        """
        This checks if name of event in self.listened_event_names
        and if it there - performs defined actions.

        @param event: Event to handle.
        @type event: Event
        """
        assert isinstance(event, Event), repr(event)

        if event.name not in self.listened_event_names:
            if self.silent:
                return
            else:
                raise EventHandlingError('Can not handle unknown {!r}'
                                             .format(event))
        logger.debug('%r handles event %r', self, event)



class EventHandlerWithCallbacks(AbstractEventHandler):
    r"""
    Base class for event handlers with callbacks defined for some of events.
    Should be one of the last classes in mro.
    >>> from common.logger import add_verbose_level; add_verbose_level()

    >>> clear = Event('clear')
    >>> increase = Event('increase')
    >>> warn = Event('warn')
    >>> event_callback_results = []
    >>> def dummy_event_callback(event):
    ...     event_callback_results.append(event)
    >>> eventname_callback_map = {
    ...     'warn': dummy_event_callback,
    ...     'clear': dummy_event_callback}
    >>> handler = EventHandlerWithCallbacks(eventname_callback_map,
    ...                                     {'warn', 'clear'})
    >>> handler.handle_event(clear)
    >>> handler.handle_event(warn)
    >>> handler.handle_event(increase)

    >>> handler.silent = False
    >>> handler.handle_event(increase)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    EventHandlingError: Can not handle unknown Event(name='increase')
    >>> handler.silent = True
    >>> handler.handle_event(increase)

    >>> handler.handle_event(clear)

    >>> event_callback_results
    [Event(name='clear'), Event(name='warn'), Event(name='clear')]
    """

    def __init__(self, eventname_callback_map=None, *args, **kwargs):
        """Constructor.

        @param eventname_callback_map: event name -> callback,
            callback receives event as argument.
        @type eventname_callback_map: col.Mapping, NoneType
        """
        super(EventHandlerWithCallbacks, self).__init__(*args, **kwargs)

        self.eventname_callback_map = \
            dict(coalesce(eventname_callback_map, {}))


    def handle_event(self, event):
        """
        Check C{AbstractEventHandler}.
        Calls appropriate callback for event, if it defined in
        C{self.eventname_callback_map}.
        """
        super(EventHandlerWithCallbacks, self).handle_event(event)
        try:
            event_callback = self.eventname_callback_map[event.name]
        except KeyError:
            pass
        else:
            logger.verbose('%r calls callback for %r', self, event)
            event_callback(event)



class FSM(AbstractEventHandler):
    r"""A Finite State Machine instance.

    Changes state on C{Event} if there is appropriate C{Transition},
    and test_func of C{Transition} succeeds.

    >>> from common.logger import add_verbose_level; add_verbose_level()

    >>> states = ['green', 'yellow', 'red']
    >>> initial_state = 'green'
    >>> clear = Event('clear')
    >>> increase = Event('increase')
    >>> warn = Event('warn')
    >>> panic = Event('panic')

    >>> tested_transitions_results = []
    >>> transition_callback_results = []
    >>> def dummy_test(event):
    ...     tested_transitions_results.append(event)
    ...     return True
    >>> def dummy_transition_callback(event):
    ...     transition_callback_results.append(event)
    >>> transitions = [
    ...     Transition(event_names=['warn', 'increase'],
    ...                src_or_srcs='green',
    ...                dst='yellow', test_func=dummy_test),
    ...     Transition(event_names=['panic', 'increase'],
    ...                src_or_srcs='yellow',
    ...                dst='red', callback=dummy_transition_callback),
    ...     Transition(event_names=['calm', 'decrease'],
    ...                src_or_srcs='yellow',
    ...                dst='green', test_func=dummy_test),
    ...     Transition(event_names=['clear'],
    ...                src_or_srcs=['green', 'yellow', 'red'],
    ...                dst='green', callback=dummy_transition_callback)
    ... ]

    >>> announced_events = []
    >>> def dummy_event_announce_callback(state_event):
    ...     announced_events.append(state_event)
    >>> state_event_adapters = {'yellow': lambda state: Event('fsm_yellow'),
    ...                         'red': lambda state: Event('fsm_red')}
    >>> fsm = FSM(states, transitions, initial_state,
    ...           dummy_event_announce_callback,
    ...           state_event_adapters, silent=False)
    >>> fsm.current_state
    'green'

    >>> fsm.handle_event(clear)
    >>> fsm.current_state
    'green'

    >>> fsm.handle_event(increase)
    >>> fsm.handle_event(warn)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
    ...
    TransitionError: Can not find transition for ...
    >>> fsm.current_state
    'yellow'

    >>> fsm.handle_event(panic)
    >>> fsm.current_state
    'red'

    >>> fsm.handle_event(clear)
    >>> fsm.current_state
    'green'

    >>> transition_callback_results
    [Event(name='clear'), Event(name='panic'), Event(name='clear')]
    >>> tested_transitions_results
    [Event(name='increase')]
    >>> announced_events
    [Event(name='fsm_yellow'), Event(name='fsm_red')]

    @ivar __transitions_storage: the mapping from C{StorageTRKey} to
        C{StorageTRValue}.
    """

    StorageTRKey = namedtuple('StorageTRKey', ('src', 'event_name'))


    def __init__(self, states, transitions, initial_state,
                 event_announce_callback=None,
                 state_event_adapters=None,
                 *args, **kwargs):
        """Constructor.

        @param states: supported states.
        @type: col.Iterable

        @param transitions: transitions possible in FSM.
        @type: col.Iterable

        @param initial_state: initial state of FSM.
        @type: basestring

        @param event_announce_callback: callable, that would receive events
            from FSM (this can be event about state change or something else).
        @type: col.Callable

        @param state_event_adapters: state -> function
            functon transforms state to event (used, when FSM changes state,
            as argument to event_announce_callback).
        @type: col.Mapping, NoneType
        """
        super(FSM, self).__init__(*args, **kwargs)
        self.states = frozenset(states)
        assert initial_state in self.states, (initial_state, self.states)
        self.__current_state = initial_state

        self.__transitions_storage = {}
        self.__transitions_lock = RLock()

        for transition in transitions:
            self.__add_transition(transition)

        self.event_announce_callback = event_announce_callback
        self.state_event_adapters = dict(coalesce(state_event_adapters, {}))


    def __add_transition(self, transition):
        """Adds transition to FSM.

        @type transition: Transition
        """
        cls = self.__class__
        assert isinstance(transition, Transition), repr(transition)

        # Ugly; todo: rework so it doesn't depend on particular types.
        if __debug__:
            for state in transition.srcs:
                assert state in self.states, (state, self.states)
            assert transition.dst in self.states, \
                   (transition.dst, self.states)

        new_transitions = \
            {cls.StorageTRKey(src=src, event_name=event_name): transition
                 for src in transition.srcs
                 for event_name in transition.event_names}
        # Btw, the code below is called from constructor only,
        # thus we could've avoid getting a lock.
        with self.__transitions_lock:
            self.__transitions_storage.update(new_transitions)
            self.listened_event_names.update(transition.event_names)


    @property
    def current_state(self):
        """Read-only property!"""
        return self.__current_state


    def handle_event(self, event):
        """
        Check C{AbstractEventHandler}.
        Performs appropriate transition for current state and provided event.
        Announces state transition using C{event_announce_callback}.

        @type event: Event

        @raises TransitionError: if transition failed.
        """
        cls = self.__class__
        super(FSM, self).handle_event(event)

        # Most important lock is here!
        # It uses self.current_state, but this property may easily change
        # during the course of the check.
        with self.__transitions_lock:
            _key = cls.StorageTRKey(src=self.__current_state,
                                    event_name=event.name)
            transition = self.__transitions_storage.get(_key)

            # Bail out if no transition at all.
            if transition is None:
                if self.silent:
                    logger.verbose(
                        '%r can not find appropriate transition for '
                            'event %r at state %r',
                        self, event, self.current_state)
                    return
                else:
                    raise TransitionError(
                              u'Can not find transition for {!r} '
                              u'while in state {!r}'
                                  .format(event, self.current_state))

            # Bail out if test_func fails.
            if transition.test_func is not None and \
               not transition.test_func(event):
                if self.silent:
                    logger.verbose('Transition on %r aborted due to '
                                       'test fail: %r',
                                   event, transition)
                    return
                else:
                    raise TransitionError(u'Aborted due to test fail: {!r}'
                                              .format(transition))

            # Yes, the event causes the FSM traversal.

            if self.__current_state != transition.dst:
                # Arrow is not a loop
                do_state_switch_announce = True
                logger.debug('%r changing state from %r to %r',
                             self, self.current_state, transition.dst)
                self.__current_state = transition.dst
            else:
                do_state_switch_announce = False

            final_state = self.__current_state

        # Leave the lock, finally, and do what is left to do
        # when the transition occurs.

        if transition.callback is not None:
            transition.callback(event)

        if do_state_switch_announce:
            self.__announce_state_switch(final_state)


    def __announce_state_switch(self, to_state):
        """
        Call a callback (if it exists) to notify that the FSM just has switched
        to the C{to_state}.
        """
        if self.event_announce_callback is not None:
            try:
                adapter = self.state_event_adapters[to_state]
            except KeyError:
                pass  # EAFP!
            else:
                self.event_announce_callback(adapter(self.current_state))
