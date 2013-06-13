#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Various tools for iterators and generators."""

#
# Imports
#

from __future__ import absolute_import, division
import sys
import types
from functools import partial
from itertools import chain, groupby, ifilter, imap, islice, izip, tee

from contrib.dbc import contract_epydoc

from .utils import on_exception, round_down_to_multiply, _nottest



#
# Functions
#

def filterfalse(function, iterable):
    r"""
    The function missing from the builtins and itertools:
    returns elements of iterable for which function returns false.

    @param function: validation function
    @param iterable: the iterable being scanned
    @return: returns elements of iterable for which function returns false.

    >>> filter(lambda x: x > 5, [3, 4, 6, 7, 8])
    [6, 7, 8]
    >>> filterfalse(lambda x: x > 5, [3, 4, 6, 7, 8])
    [3, 4]
    """
    return [i for i in iterable if not function(i)]

def __unzip(iterable, seq_constr):
    r"""Auxillary function for C{iunzip}/C{unzip}.

    >>> data2 = [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')]
    >>> data3 = [(1, 'a', 'A'), (2, 'b', 'B'), (3, 'c', 'C'), (4, 'd', 'D')]

    >>> # unzip
    >>> unzip(data2)
    ([1, 2, 3, 4, 5], ['a', 'b', 'c', 'd', 'e'])
    >>> unzip(data3)
    ([1, 2, 3, 4], ['a', 'b', 'c', 'd'], ['A', 'B', 'C', 'D'])

    >>> # iunzip
    >>> # * result structure
    >>> iunzip(data2)  # doctest:+ELLIPSIS
    (<itertools.imap object at 0x...>, <itertools.imap object at 0x...>)
    >>> iunzip(data3)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
    (<itertools.imap object at 0x...>,
     <itertools.imap object at 0x...>,
     <itertools.imap object at 0x...>)
    >>> # * result values
    >>> i1, i2 = iunzip(data2)
    >>> list(i1)
    [1, 2, 3, 4, 5]
    >>> list(i2)
    ['a', 'b', 'c', 'd', 'e']
    >>> i1, i2, i3 = iunzip(data3)
    >>> list(i1)
    [1, 2, 3, 4]
    >>> list(i2)
    ['a', 'b', 'c', 'd']
    >>> list(i3)
    ['A', 'B', 'C', 'D']
    """
    iterable = iter(iterable)
    first = iterable.next()
    return tuple(seq_constr(imap(lambda item, i=index: item[i],
                                 iter_tee))
                     for index, iter_tee in enumerate(tee(chain([first],
                                                                iterable),
                                                          len(first))))

iunzip = partial(__unzip, seq_constr=iter)
"""Convert an iterable of tuples to the tuple of iterables.

@note: kind of, opposite to C{izip}.

@param iterable: an iterable, containing tuples of the same cardinality
    (e.g. all tuples have the same number of elements).
@return: a tuple containing the iterables.
    The first iterable contains all the first items in the tuples yielded by
    C{iterable}, the second iterable contains all the second items in
    the tuples yielded by C{iterable}, and so on.
"""

unzip = partial(__unzip, seq_constr=list)
"""Convert an iterable of tuples to the tuple of lists.

@note: kind of, opposite to C{zip}.

@param iterable: an iterable, containing tuples of the same cardinality
    (e.g. all tuples have the same number of elements).
@return: a tuple containing the lists.
    The first list contains all the first items in the tuples yielded by
    C{iterable}, the second list contains all the second items in
    the tuples yielded by C{iterable}, and so on.
"""


def take(count, iterable):
    """
    Take C{count} first elements from C{iterable}, or less if C{iterable}
    is shorter.

    >>> take(3, [3, 14, 15, 92, 65]) # doctest:+ELLIPSIS
    <itertools.islice object at ...>
    >>> list(take(3, [3, 14, 15, 92, 65]))
    [3, 14, 15]
    >>> list(take(5, [3, 14, 15]))
    [3, 14, 15]
    """
    return islice(iterable, count)


def ilen(iterable):
    """Calculate the length of the iterable.

    >>> ilen(iter(range(5)))
    5
    >>> ilen(1 for i in xrange(10))
    10
    """
    return sum(1 for i in iterable)


@contract_epydoc
def inonempty(iterable):
    r"""Test whether the iterable is empty.

    >>> (inonempty(xrange(4, 6))[0], list(inonempty(xrange(4, 6))[1]))
    (True, [4, 5])

    >>> (inonempty(xrange(4, 4))[0], list(inonempty(xrange(4, 4))[1]))
    (False, [])

    @warning: this function corrupts the original iterable (in particular,
        moves one iteration further), so use the new iterable from the
        result to avoid corruption.

    @type iterable: col.Iterable

    @returns: a tuple containing the test result
        (C{True}: iterable is non-empty,
        C{False}: iterable is empty) and the new iterable which should be
            used instead of the original iterable (as the original one is
            corrupted now).
    @rtype: tuple
    """
    # Let's test whether at least a single item exists
    new_iter = iter(iterable)
    try:
        head = new_iter.next()
    except StopIteration:
        exists = False
        res_iter = iter([])
    else:
        exists = True
        res_iter = chain([head], new_iter)
    return (exists, res_iter)


if (sys.version_info.major >= 3 and sys.version_info.minor >= 3):
    from itertools import accumulate as __accumulate
    accumulate = __accumulate
else:
    import operator

    def accumulate(iterable, func=operator.add):
        r"""Return running totals. Cloned from Python 3.3.

        >>> accumulate([1,2,3,4,5])  # doctest:+ELLIPSIS
        <generator object accumulate at 0x...>
        >>> list(accumulate([1,2,3,4,5]))
        [1, 3, 6, 10, 15]
        >>> list(accumulate([1,2,3,4,5], operator.mul))
        [1, 2, 6, 24, 120]
        """
        it = iter(iterable)
        total = next(it)
        yield total
        for element in it:
            total = func(total, element)
            yield total


def consume(iterable):
    r"""Loops over the iterable doing nothing.

    If the iterable was non-reiterable, it will be fully consumed.

    >>> iterable = (i for i in xrange(10))
    >>> consume(iterable)
    >>> list(iterable)
    []

    @type iterable: col.Iterable
    """
    for i in iterable:
        pass


def repr_long_sized_iterable(collection, max_items=3):
    r"""Get a representation of a collection, safe for too long ones.

    >>> repr_long_sized_iterable([])
    '[]'

    >>> repr_long_sized_iterable(set([]))
    'set([])'

    >>> repr_long_sized_iterable({13, 15, 17})
    'set([17, 13, 15])'

    >>> repr_long_sized_iterable([13, 15, 17, 19])
    'list(13, 15, 17, ...)'

    >>> repr_long_sized_iterable([13, 15, 17, 19], max_items=1)
    'list(13, ...)'

    >>> repr_long_sized_iterable([13, 15, 17, 19], max_items=0)
    'list(...)'

    >>> # Test various Unicode combinations
    >>> repr_long_sized_iterable(['абв', 'где'])
    "['\\xd0\\xb0\\xd0\\xb1\\xd0\\xb2', '\\xd0\\xb3\\xd0\\xb4\\xd0\\xb5']"
    >>> print(repr_long_sized_iterable(['абв', 'где']))
    ['\xd0\xb0\xd0\xb1\xd0\xb2', '\xd0\xb3\xd0\xb4\xd0\xb5']

    >>> repr_long_sized_iterable([u'абв', 'где'])
    "[u'\\xd0\\xb0\\xd0\\xb1\\xd0\\xb2', '\\xd0\\xb3\\xd0\\xb4\\xd0\\xb5']"
    >>> print(repr_long_sized_iterable([u'абв', 'где']))
    [u'\xd0\xb0\xd0\xb1\xd0\xb2', '\xd0\xb3\xd0\xb4\xd0\xb5']

    >>> # Let's even test the cases when it fails.
    >>> class C1(object):
    ...     def __init__(self, v):
    ...         self.v = v
    ...     def __repr__(self):
    ...         '''Not well-reprable'''
    ...         return u'«{!r}»'.format(self.v)
    >>> c1, c2, c3 = C1(1), C1(2), C1(3)

    >>> # This is how bad this class is!
    >>> c1  # doctest:+NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    UnicodeEncodeError: 'ascii' codec can't encode characters in position 0-1:
        ordinal not in range(128)
    >>> [c1, c2, c3]  # doctest:+NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    UnicodeEncodeError: 'ascii' codec can't encode characters in position 0-1:
        ordinal not in range(128)

    >>> # But still make sure that C{repr_long_sized_iterable} won't fail.
    >>> repr_long_sized_iterable([c1, c2, c3])
    '<bad Unicode value>'

    >>> repr_long_sized_iterable([c1, c2, c3], max_items=2)
    'list(<bad Unicode value>, <bad Unicode value>, ...)'

    @param collection: col.Iterable and col.Sized simultaneously.
        Note: must be reiterable, otherwise consumes up to C{max_items} items
        from C{collection}.

    @rtype: str
    """
    safe_repr = on_exception(exc=UnicodeError,
                             default='<bad Unicode value>')(repr)

    if len(collection) > max_items:
        return '{!s}({!s})'.format(
                   collection.__class__.__name__,
                   ', '.join(chain((safe_repr(item) for i, item
                                           in izip(xrange(max_items),
                                                   collection)),
                                   ['...'])))
    else:
        return safe_repr(collection)


def only_one_of(*args):
    r"""
    Given boolean (or boolean-castable) arguments, check
    whether only one of them is valid.

    >>> only_one_of(5, 0, 3, 'd')
    False

    >>> only_one_of('', 0, -2, None, {})
    True

    @return: whether only one of the arguments is logically C{True}.
    @rtype: bool
    """
    return ilen(ifilter(bool, args)) == 1


def sorted_groupby(iterable, key):
    r"""
    Combined C{sorted} and C{groupby}, thus making sure that the groups
    are unique.

    >>> from collections import namedtuple
    >>> from operator import attrgetter
    >>> T = namedtuple('T', ('k', 'v'))
    >>> makedata = lambda: [T(5, 1), T(5, 3), T(6, 8), T(5, 4), T(6, 2),
    ...                     T(7, 13), T(6, 14), T(5, 15), T(7, 5)]

    >>> i1 = sorted_groupby(makedata(), attrgetter('k'))
    >>> i1  # doctest:+ELLIPSIS
    <itertools.groupby object at 0x...>
    >>> # This is how you should NOT use any groupby().
    >>> l1 = list(i1); l1  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
    [(5, <itertools._grouper object at 0x...>),
     (6, <itertools._grouper object at 0x...>),
     (7, <itertools._grouper object at 0x...>)]
    >>> list(l1[0][1])
    []
    >>> list(l1[1][1])
    []
    >>> list(l1[2][1])
    [T(k=7, v=5)]

    >>> i2 = sorted_groupby(makedata(), attrgetter('k'))
    >>> i2  # doctest:+ELLIPSIS
    <itertools.groupby object at 0x...>
    >>> # This is how you SHOULD use groupby().
    >>> [(k, list(v)) for k, v in i2]  # doctest:+NORMALIZE_WHITESPACE
    [(5, [T(k=5, v=1), T(k=5, v=3), T(k=5, v=4), T(k=5, v=15)]),
     (6, [T(k=6, v=8), T(k=6, v=2), T(k=6, v=14)]),
     (7, [T(k=7, v=13), T(k=7, v=5)])]

    @type iterable: col.Iterable
    @type key: col.Callable
    @return: similarly to C{groupby}, returns a (non-reiterable) iterable
        of two-item tuples, where the first item is the key value
        and the second item is the (non-reiterable!) iterable over items
        in this group.
    @rtype: col.Iterable
    @warning: not only the second items are non-reiterable; similarly
        to C{groupby}, the second items share the same iterable beneath
        (and even share the iterable with the group generator),
        so not only the second "grouper" items must be iterated in turn,
        they must be iterated within the iteration for that particular group!
    """
    return groupby(sorted(iterable, key=key), key=key)


def groupby_to_dict(iterable, key):
    r"""Group all the items of the iterable to the convenient dictionary.

    >>> from collections import namedtuple
    >>> from operator import attrgetter
    >>> T = namedtuple('T', ('k', 'v'))
    >>> makedata = lambda: [T(5, 1), T(5, 3), T(6, 8), T(5, 4), T(6, 2),
    ...                     T(7, 13), T(6, 14), T(5, 15), T(7, 5)]

    >>> groupby_to_dict(makedata(), attrgetter('k'))  \
    ... # doctest:+NORMALIZE_WHITESPACE
    {5: [T(k=5, v=1), T(k=5, v=3), T(k=5, v=4), T(k=5, v=15)],
     6: [T(k=6, v=8), T(k=6, v=2), T(k=6, v=14)],
     7: [T(k=7, v=13), T(k=7, v=5)]}

    @type iterable: col.Iterable
    @type key: col.Callable
    @return: a mapping from the value of the key to the list of all items
        with this key.
    @rtype: col.Mapping
    """
    return {k: list(v) for k, v in sorted_groupby(iterable, key)}


def reshape_bytestream(iterable, chunk_size, allow_multiples=True):
    r"""
    Take an C{iterable} returning the byte strings, and produce an iterable
    that yields the same byte strings but in chunks either exactly
    C{chunk_size} long (if C{allow_multiples = False}), or if chunks which
    size is a multiple of C{chunk_size} (if C{allow_multiples = True}).
    The last chunk is always yielded as is, so may be of a size non-divisable
    by C{chunk_size}.

    >>> data = ['abcda', 'bcdabc', 'd', 'a', 'b', '', 'c', 'da', 'bc']

    >>> # 4
    >>> list(reshape_bytestream(iter(data), 4))
    ['abcd', 'abcd', 'abcd', 'abcd', 'abc']

    >>> # 3
    >>> list(reshape_bytestream(iter(data), 3))
    ['abc', 'dabcda', 'bcd', 'abc', 'dab', 'c']
    >>> list(reshape_bytestream(iter(data), 3, allow_multiples=False))
    ['abc', 'dab', 'cda', 'bcd', 'abc', 'dab', 'c']

    >>> # 2
    >>> list(reshape_bytestream(iter(data), 2))
    ['abcd', 'abcdab', 'cd', 'ab', 'cd', 'ab', 'c']
    >>> list(reshape_bytestream(iter(data), 2, allow_multiples=False))
    ['ab', 'cd', 'ab', 'cd', 'ab', 'cd', 'ab', 'cd', 'ab', 'c']

    @type iterable: col.Iterable
    @type chunk_size: numbers.Integral
    @type allow_multiples: bool
    @rtype: col.Iterable
    """
    buf = ''
    for in_chunk in iterable:
        buf += in_chunk
        l = len(buf)
        while l >= chunk_size:
            out_chunk_size = round_down_to_multiply(l, chunk_size) \
                                 if allow_multiples \
                                 else chunk_size
            out_chunk, buf = buf[:out_chunk_size], buf[out_chunk_size:]
            yield out_chunk

            l = len(buf)
    if buf:
        yield buf


def co_reshape_bytestream(consumer, chunk_size, allow_multiples=True):
    r"""Coroutine-version of C{reshape_bytestream}.

    Assumes that C{consumer} is a generator-based coroutine consumer,
    which yields the callable generating some result. Returns a generator
    with the same properties; the yielded callable should be called only once,
    it will push the remaining data to the C{consumer}, get the C{consumer}'s
    callable, call it and return its result.

    >>> data = ['abcdE', 'FGHABC', 'D', '1', '2', '', '3', '4a', 'bc']

    >>> # 4
    >>> tst_gen_rshpd = co_reshape_bytestream(co_test_consumer(), 4)
    >>> tst_gen_rshpd.next()  # doctest:+ELLIPSIS
    <function finalizer at 0x...>
    >>> for i in data:
    ...     res = tst_gen_rshpd.send(i)
    Consume: abcd
    Consume: EFGH
    Consume: ABCD
    Consume: 1234
    >>> res()
    Consume: abc
    19

    >>> # 3, multiples
    >>> tst_gen_rshpd = co_reshape_bytestream(co_test_consumer(), 3)
    >>> tst_gen_rshpd.next()  # doctest:+ELLIPSIS
    <function finalizer at 0x...>
    >>> for i in data:
    ...     res = tst_gen_rshpd.send(i)
    Consume: abc
    Consume: dEFGHA
    Consume: BCD
    Consume: 123
    Consume: 4ab
    >>> res()
    Consume: c
    19

    >>> # 3, no multiples
    >>> tst_gen_rshpd = co_reshape_bytestream(
    ...                     co_test_consumer(), 3, allow_multiples=False)
    >>> tst_gen_rshpd.next()  # doctest:+ELLIPSIS
    <function finalizer at 0x...>
    >>> for i in data:
    ...     res = tst_gen_rshpd.send(i)
    Consume: abc
    Consume: dEF
    Consume: GHA
    Consume: BCD
    Consume: 123
    Consume: 4ab
    >>> res()
    Consume: c
    19

    >>> # 2, multiples
    >>> tst_gen_rshpd = co_reshape_bytestream(co_test_consumer(), 2)
    >>> tst_gen_rshpd.next()  # doctest:+ELLIPSIS
    <function finalizer at 0x...>
    >>> for i in data:
    ...     res = tst_gen_rshpd.send(i)
    Consume: abcd
    Consume: EFGHAB
    Consume: CD
    Consume: 12
    Consume: 34
    Consume: ab
    >>> res()
    Consume: c
    19

    >>> # 2, no multiples
    >>> tst_gen_rshpd = co_reshape_bytestream(
    ...                     co_test_consumer(), 2, allow_multiples=False)
    >>> tst_gen_rshpd.next()  # doctest:+ELLIPSIS
    <function finalizer at 0x...>
    >>> for i in data:
    ...     res = tst_gen_rshpd.send(i)
    Consume: ab
    Consume: cd
    Consume: EF
    Consume: GH
    Consume: AB
    Consume: CD
    Consume: 12
    Consume: 34
    Consume: ab
    >>> res()
    Consume: c
    19

    @type consumer: types.Generator
    @type chunk_size: numbers.Integral
    @type allow_multiples: bool
    @rtype: col.Iterable
    """
    def finalizer():
        output = consumer.send(buf)
        return output()

    output = consumer.next()
    buf = ''
    while True:
        in_chunk = yield finalizer

        buf += in_chunk
        l = len(buf)
        while l >= chunk_size:
            out_chunk_size = round_down_to_multiply(l, chunk_size) \
                                 if allow_multiples \
                                 else chunk_size
            out_chunk, buf = buf[:out_chunk_size], buf[out_chunk_size:]
            output = consumer.send(out_chunk)

            l = len(buf)



if __debug__:
    @_nottest
    def co_test_consumer():
        """Coroutine consumer to test coroutine combinators.

        >>> tst_gen = co_test_consumer(); tst_gen.next()  # doctest:+ELLIPSIS
        <function <lambda> at 0x...>
        >>> for c in 'abc123':
        ...     res = tst_gen.send(c)
        Consume: a
        Consume: b
        Consume: c
        Consume: 1
        Consume: 2
        Consume: 3
        >>> res()
        6
        """
        total_len = 0
        while True:
            _input = yield lambda: total_len
            print('Consume: {}'.format(_input))
            total_len += len(_input)

