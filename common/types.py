#!/usr/bin/python
"""
Enhancements to basic data types.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from threading import Lock
from types import NoneType



#
# Classes
#

class EventfulDict(dict):
    """
    A dictionary, that can call some callbacks on various events, eg:
      * when (after) the dictionary becomes non-empty;
      * when (after) the dictionary becomes empty.

    @note: the callbacks are NOT called when the dictionary
    @note: thread-safe.
    """

    __slots__ = ('__on_non_empty_cb', '__on_empty_cb', '__write_lock')


    def __init__(self,
                 *args, **kwargs):
        """Constructor.

        >>> def f1(): print('f1()')
        >>> def f2(): print('f2()')
        >>> EventfulDict()  # smoke test
        {}
        >>> EventfulDict(_on_non_empty_cb=f1, _on_empty_cb=f2, k=5, x=7)
        {'x': 7, 'k': 5}

        @param _on_non_empty_cb: a callback function that should be called
            (if provided) when (after) the dictionary becomes non-empty.
        @type _on_non_empty_cb: col.Callable, NoneType
        @param _on_empty_cb: a callback function that should be called
            (if provided) when (after) the dictionary becomes empty.
        @type _on_empty_cb: col.Callable, NoneType
        """
        if '_on_non_empty_cb' in kwargs:
            self.__on_non_empty_cb = kwargs['_on_non_empty_cb']
            del kwargs['_on_non_empty_cb']
        else:
            self.__on_non_empty_cb = None

        if '_on_empty_cb' in kwargs:
            self.__on_empty_cb = kwargs['_on_empty_cb']
            del kwargs['_on_empty_cb']
        else:
            self.__on_empty_cb = None

        super(EventfulDict, self).__init__(*args, **kwargs)

        self.__write_lock = Lock()


    def __setitem__(self, index, value):
        r"""Enhance implementation from C{dict}.

        >>> def f1(): print('f1()')
        >>> def f2(): print('f2()')
        >>> d = EventfulDict(_on_non_empty_cb=f1, _on_empty_cb=f2)
        >>> d
        {}
        >>> d[5] = 7  # on-non-empty callback is called
        f1()
        >>> d
        {5: 7}
        >>> d[6] = 8  # on-non-empty callback is not called
        >>> d
        {5: 7, 6: 8}
        """
        with self.__write_lock:
            empty_before_set = not bool(self)
            super(EventfulDict, self).__setitem__(index, value)
        if empty_before_set and self.__on_non_empty_cb is not None:
            self.__on_non_empty_cb()


    def __delitem__(self, index):
        r"""Enhance implementation from C{dict}.

        >>> def f1(): print('f1()')
        >>> def f2(): print('f2()')
        >>> d = EventfulDict(_on_non_empty_cb=f1, _on_empty_cb=f2)
        >>> d
        {}
        >>> d[5] = 7  # on-non-empty callback is called
        f1()
        >>> d[6] = 8  # on-non-empty callback is not called
        >>> del d[5]  # on-empty callback is not called
        >>> del d[6]  # on-empty callback is called
        f2()
        """
        with self.__write_lock:
            super(EventfulDict, self).__delitem__(index)
            empty_after_del = not bool(self)
        if empty_after_del and self.__on_empty_cb is not None:
            self.__on_empty_cb()



#
# Advanced tests
#

if __debug__:
    __test__ = {
        'EventfulDict.no_callbacks':
            r"""
            >>> def f1(): print('f1()')
            >>> def f2(): print('f2()')
            >>> d = EventfulDict()
            >>> d
            {}
            >>> d[5] = 7  # here a on-non-empty callback should've fired
            >>> d[6] = 8  # here a on-non-empty callback should've not fired
            >>> d
            {5: 7, 6: 8}
            >>> del d[5]  # here a on-empty callback should've not fired
            >>> del d[6]  # here a on-empty callback should've fired
            >>> d
            {}
            """,
        'EventfulDict.partial_callbacks_on_non_empty_cb':
            r"""
            >>> def f1(): print('f1()')
            >>> d = EventfulDict(_on_non_empty_cb=f1)
            >>> d
            {}
            >>> d[5] = 7  # on-non-empty callback
            f1()
            >>> d[6] = 8  # here a on-non-empty callback should've not fired
            >>> d
            {5: 7, 6: 8}
            >>> del d[5]  # here a on-empty callback should've not fired
            >>> del d[6]  # here a on-empty callback should've fired
            >>> d
            {}
            """,
        'EventfulDict.partial_callbacks_on_empty_cb':
            r"""
            >>> def f2(): print('f2()')
            >>> d = EventfulDict(_on_empty_cb=f2)
            >>> d
            {}
            >>> d[5] = 7  # here a on-non-empty callback should've fired
            >>> d[6] = 8  # here a on-non-empty callback should've not fired
            >>> d
            {5: 7, 6: 8}
            >>> del d[5]  # here a on-empty callback should've not fired
            >>> del d[6]  # on-empty callback
            f2()
            >>> d
            {}
            """,
        'EventfulDict.dict_compatibility':
            r"""
            >>> def f1(): print('f1()')
            >>> def f2(): print('f2()')

            >>> # Empty
            >>> EventfulDict()
            {}
            >>> EventfulDict(_on_non_empty_cb=f1, _on_empty_cb=f2)
            {}

            >>> # With given named arguments
            >>> EventfulDict(k=5, x=7)
            {'x': 7, 'k': 5}
            >>> EventfulDict(k=5, x=7, _on_non_empty_cb=f1, _on_empty_cb=f2)
            {'x': 7, 'k': 5}

            >>> # From a dictionary
            >>> d1 = EventfulDict({'k': 5, 'x': 7})
            >>> d1
            {'x': 7, 'k': 5}
            >>> d2 = EventfulDict({'k': 5, 'x': 7},
            ...                   _on_non_empty_cb=f1,
            ...                   _on_empty_cb=f2)
            >>> d2
            {'x': 7, 'k': 5}

            >>> # From an iterable of tuples
            >>> d1 = EventfulDict([(5, 7), (6, 8)])
            >>> d1
            {5: 7, 6: 8}
            >>> d2 = EventfulDict([(5, 7), (6, 8)],
            ...                   _on_non_empty_cb=f1,
            ...                   _on_empty_cb=f2)
            >>> d2
            {5: 7, 6: 8}
            """,
        'EventfulDict.callbacks_are_not_oneshot':
            r"""
            >>> def f1(): print('f1()')
            >>> def f2(): print('f2()')
            >>> d = EventfulDict(_on_non_empty_cb=f1, _on_empty_cb=f2)
            >>> d
            {}
            >>> # Loop 1
            >>> d[5] = 7  # on-non-empty callback is called
            f1()
            >>> d[6] = 8  # on-non-empty callback is not called
            >>> d
            {5: 7, 6: 8}
            >>> del d[5]  # on-empty callback is not called
            >>> del d[6]  # on-empty callback is called
            f2()
            >>> d
            {}
            >>> # Loop 2
            >>> d['a'] = 1  # on-non-empty callback is called
            f1()
            >>> d['b'] = 2  # on-non-empty callback is not called
            >>> d['c'] = 3  # on-non-empty callback is not called
            >>> d
            {'a': 1, 'c': 3, 'b': 2}
            >>> del d['b']  # on-empty callback is not called
            >>> del d['a']  # on-empty callback is not called
            >>> del d['c']  # on-empty callback is called
            f2()
            >>> d
            {}
            """,
    }
