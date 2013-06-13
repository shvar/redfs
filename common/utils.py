#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Common-purpose utilitary functions.

@requires: import collections as col, sys, os; from threading import RLock
@requires: from threading import RLock
"""

#
# Imports
#

from __future__ import absolute_import, division
import collections as col
import gc
import inspect
import logging
import numbers
import os
import posixpath
import pprint as pprint_module
import re
import sys
import threading
import unicodedata
from calendar import timegm
from datetime import datetime, timedelta, time
from fnmatch import fnmatch
from functools import wraps, reduce
from itertools import imap
from random import randint, randrange
from time import sleep
from uuid import UUID, uuid4
from types import NoneType

from twisted.python import threadable

from contrib.dbc import contract_epydoc

from . import version
from .datetime_ex import TimeDeltaEx



#
# Constants
#

logger = logging.getLogger(__name__)
logger_duration = logging.getLogger('duration')
logger_long_operation = logging.getLogger('long_operation')

# Up to 2^64
POWERS_OF_TWO_SET = frozenset(2 ** i for i in xrange(64))

# Used to represent a fake host
NULL_UUID = UUID('00000000-0000-0000-0000-000000000000')
# Used to represent a fake node
MAX_UUID = UUID('FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF')
# Datetime format used to store the datetime in a stable way.
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
__DATETIME_FORMAT_FALLBACK = '%Y-%m-%d %H:%M:%S'

HEAPY = None

LONG_DURATION = timedelta(seconds=1)

_FORCE_SPOOF_AS_SECONDARY_THREAD = False
"""
If this constant is set to C{True}, the C{in_main_thread()} function always
returns C{True}. Thus this constant must be altered ONLY in unit tests.
"""



#
# Functions
#

def typed(var, types):
    """
    Ensure that the C{var} argument is among the types passed
    as the C{types} argument.

    @param var: The argument to be typed.
    @param types: A tuple of types to check.
    @type types: tuple
    @return: The var argument.
    """
    assert isinstance(var, types), \
           u'Variable {} of type {} not among the allowed types: {!r}' \
               .format(var, type(var), types)
    return var


def ntyped(var, types):
    """
    Ensure that the C{var} argument is among the types passed
    as the C{types} argument, or is C{None}.

    @param var: The argument to be typed.
    @param types: A tuple of types to check.
    @type types: tuple
    @return: The var argument.
    """
    assert var is None or isinstance(var, types), \
           u'Variable {} of type {} not among the allowed types: ' \
               u'NoneType, {!r}' \
               .format(var, type(var), types)
    return var


def shorten_uuid(uuid):
    """
    Given the UUID object, return its shorter version,
    sufficient for general debugging or overview.

    @param uuid: UUID to shorten
    @type uuid: UUID

    @return: The shorter UUID string.
    @rtype: str
    """
    assert isinstance(uuid, UUID), repr(uuid)
    return '{}-...-{}'.format(uuid.hex[:8], uuid.hex[-8:])


def coalesce(*args):
    """
    Given some arguments, return the first of them which is not None,
    or None.

    The name and the concept is inspired by the SQL standard COALESCE function.

    >>> coalesce(None, 5)
    5
    >>> coalesce(None, '', None)
    ''
    >>> coalesce(None, None, None, None) # Returns None
    """
    for arg in args:
        if arg is not None:
            return arg
    return None


def rpdb2():
    """Make a breakpoing using rpdb2 interactive debugger."""
    try:
        import rpdb2 as rpdb2_module

        if __debug__:
            try:
                import pygame
                pygame.init()
                pickUpSound = pygame.mixer.Sound(
                                  './contrib/_extra/tos-comm.ogg')
                pickUpSound.play()
                sleep(2.0)
                pygame.quit()
            except Exception:
                print('For sound notifications about waiting rpdb2(), '
                          'please run:\n'
                      'sudo aptitude install python-pygame')

        print('rpdb2')
        rpdb2_module.start_embedded_debugger(version.project_internal_codename
                                                    .lower(),
                                             fAllowRemote=True)
    except ImportError:
        raise ImportError("""
For advanced features, please use:
    sudo aptitude install winpdb
""")


def size_code_to_size(size_code):
    r"""Given a size code, convert it to the actual size.

    >>> size_code_to_size(0), size_code_to_size(1), size_code_to_size(2)
    (1048576, 2097152, 4194304)
    >>> size_code_to_size(3), size_code_to_size(4), size_code_to_size(5)
    (8388608, 16777216, 33554432)

    @param size_code: size code to convert.
    @type size_code: int

    @return: the actual size related to this size code.
    @rtype: int
    """
    return 0x100000 * (2 ** size_code)


OPEN_WB_LOCK = threading.Lock()


def open_wb(file_path):
    """Open a file for binary writing, creating all the missing directories.

    Also available with the context manager protocol.

    @returns: File handle
    """
    global OPEN_WB_LOCK
    dirs = os.path.dirname(file_path)

    with OPEN_WB_LOCK:
        if not os.path.exists(dirs):
            os.makedirs(dirs)

    return open(file_path, 'w+b')


def touch(file_path):
    """
    "Touch" the file similarly to the touch unix command.
    If necessary, create all the directories on the path.
    """
    with open_wb(file_path):
        pass


@contract_epydoc
def strftime(dt):
    r"""
    An universal function to convert a datetime object to the string
    in a stable way. Left here for consistency, together with the strptime().

    >>> dt = datetime(2010, 3, 4, 5, 1, 2, 123456)
    >>> strftime(dt)
    '2010-03-04 05:01:02.123456'

    @param dt: The datetime object
    @type dt: datetime
    @precondition: dt.tzinfo is None # dt

    @returns: The stable representation of a datetime object as a string.
    @rtype: str
    """
    global DATETIME_FORMAT
    # Funny, but builtin .strftime() method does not work
    # with years before 1900.
    return dt.strftime(DATETIME_FORMAT) if dt != datetime.min \
                                        else '0001-01-01 00:00:00.000000'


@contract_epydoc
def strptime(dt_str):
    r"""
    An universal function to convert a string to the datetime object
    in a stable way.

    >>> strptime('2010-03-04 05:01:02.123456') == \
    ... datetime(2010, 3, 4, 5, 1, 2, 123456)
    True

    >>> strptime('2010-03-04 05:01:02.12345') == \
    ... strptime('2010-03-04 05:01:02.123450') == \
    ... datetime(2010, 3, 4, 5, 1, 2, 123450)
    True

    >>> strptime('2010-03-04 05:01:02.1234') == \
    ... strptime('2010-03-04 05:01:02.123400') == \
    ... datetime(2010, 3, 4, 5, 1, 2, 123400)
    True

    >>> strptime('2010-03-04 05:01:02.123') == \
    ... strptime('2010-03-04 05:01:02.123000') == \
    ... datetime(2010, 3, 4, 5, 1, 2, 123000)
    True

    >>> strptime('2010-03-04 05:01:02.12') == \
    ... strptime('2010-03-04 05:01:02.120000') == \
    ... datetime(2010, 3, 4, 5, 1, 2, 120000)
    True

    >>> strptime('2010-03-04 05:01:02.1') == \
    ... strptime('2010-03-04 05:01:02.100000') == \
    ... datetime(2010, 3, 4, 5, 1, 2, 100000)
    True

    >>> strptime('2010-03-04 05:01:02') == \
    ... strptime('2010-03-04 05:01:02.000000') == \
    ... datetime(2010, 3, 4, 5, 1, 2, 0)
    True

    @param dt_str: The string containing a datetime representation
    @type dt_str: basestring

    @returns: The datetime object as parsed from the string representation.
    @rtype: datetime
    """
    global DATETIME_FORMAT, __DATETIME_FORMAT_FALLBACK
    try:
        result = datetime.strptime(dt_str, DATETIME_FORMAT)
    except ValueError:
        # If we couldn't have deserialized the datetime with the microseconds
        # (new format), try deserializing without the microseconds
        # (old format).
        result = datetime.strptime(dt_str, __DATETIME_FORMAT_FALLBACK)

    return result


def fix_strptime(dt_maybe_str):
    """For SQLite3, SQLAlchemy requires it.

    @todo: get rid of C{fix_strptime} (and remove it in all callers)
        after fixing C{sqlite3.PARSE_DECLTYPES}.
    """
    return strptime(dt_maybe_str) if isinstance(dt_maybe_str, basestring) \
                                  else dt_maybe_str


def exceptions_logged(logger_obj, ignore=None):
    """
    The function decorator that generates an error log in case if
    the decorated function raises an exception during the execution.

    @note: DO NOT WRAP with C{@contract_epydoc}!
    That's the first step to the "decorated decorators" hell.

    @param logger_obj: The logger object to use for logging.
    @type logger_obj: logging.Logger

    @param ignore: The single exception or the list/tuple of exceptions which
                   should not be logged, as it is assumed they are handled
                   properly by the caller.
    """
    # DO NOT CHANGE to @contract_epydoc!
    assert isinstance(logger_obj, logging.Logger), \
           repr(logger_obj)

    if ignore is None:
        ignore = []
    assert ((isinstance(ignore, (list, tuple)) and
              all(issubclass(i, Exception) for i in ignore)) or
             (isinstance(ignore, type) and
              issubclass(ignore, Exception))), \
           repr(ignore)

    def wrap(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as e:
                if not (ignore and isinstance(e, ignore)):
                    logger_obj.exception('During %r, '
                                             'the following error occured: %s',
                                         f, e)
                raise

        return wrapper

    return wrap


__EARLIEST_TS = None


def duration_logged(comment=''):
    """
    The function decorator that generates a debug log
    containing the duration of the function execution.
    """

    def wrap(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            global __EARLIEST_TS
            try:
                start_time = datetime.utcnow()
                if __EARLIEST_TS is None:
                    __EARLIEST_TS = start_time
                return f(*args, **kwargs)
            finally:
                end_time = datetime.utcnow()
                _diff = TimeDeltaEx.from_timedelta(end_time - start_time)

                if _diff >= LONG_DURATION:
                    _start = TimeDeltaEx.from_timedelta(start_time
                                                        - __EARLIEST_TS)
                    f_name = \
                        '{}{}{}{}'.format('{}.'.format(f.__module__)
                                              if hasattr(f, '__module__')
                                              else "",
                                          '{}.'.format(f.__class__.__name__)
                                              if hasattr(f, '__class__')
                                              else '',
                                          f.__name__,
                                          '_({})'.format(comment) if comment
                                                                  else '')

                    logger_duration.warning('[%s %s %f %f], tooks %s',
                                            f_name, f_name,
                                            _start.in_seconds(),
                                            _diff.in_seconds(),
                                            _diff)

        return wrapper

    return wrap


def get_pid_path():
    return os.path.join('.' if __debug__ else '/var/run',
                        '{}.pid'.format(version.project_name))


def update_var_run_ppid(append=True):
    """Write the process id into the /var/run ppid file.

    @param append: If True, appends to the file.
                   If False, resets the file and writes since its beginning.
    @type append: bool

    @raises: IOError
    """
    path = get_pid_path()
    with open(path, 'a' if append else 'w', 0) as fh:
        fh.write('{}\n'.format(os.getpid()))


def get_executable_dir():
    """
    This function returns the directory where the executable directory
    is stored.
    """
    return os.path.dirname(os.path.abspath(sys.argv[0]))


def dt_to_ts(dt):
    r"""Given an UTC datetime, get the proper timestamp.

    >>> dt_to_ts(datetime(2010, 9, 5, 20, 42, 58, 629148))
    1283719378.629148
    >>> # dt_to_ts(datetime.min)

    @deprecated: Use datetime_ex module!

    @type dt: datetime

    @rtype: float
    """
    assert isinstance(dt, datetime), repr(dt)
    assert dt != datetime.min

    return timegm(dt.timetuple()) + dt.microsecond / 1000000.0


def ts_to_dt(ts):
    r"""Given a timestamp (int/float), get the proper UTC datetime.

    >>> ts_to_dt(1283719378.629148)
    datetime.datetime(2010, 9, 5, 20, 42, 58, 629148)

    @deprecated: Use datetime_ex module!

    @type ts: numbers.Real

    @rtype: datetime
    """
    assert isinstance(ts, numbers.Real), repr(ts)
    assert ts >= 0, repr(ts)

    try:
        return datetime.utcfromtimestamp(ts)
    except:
        logger.error('Wrong ts %r', ts)
        raise


def td_to_sec(td):
    r"""Given a timedelta, get the number of seconds.

    >>> str(td_to_sec(timedelta(1, 6, 415238)))
    '86406.415238'

    @deprecated: Use datetime_ex module!

    @type td: timedelta

    @rtype: float
    """
    assert isinstance(td, timedelta), repr(td)
    return td.days * 86400 + td.seconds + td.microseconds / 1000000.0


def sec_to_td(sec):
    r"""Given a duration in seconds (int/float), get the timedelta.

    >>> sec_to_td(86406.415238)
    datetime.timedelta(1, 6, 415238)
    >>> sec_to_td(86406.415238000001)
    datetime.timedelta(1, 6, 415238)

    @deprecated: Use datetime_ex module!

    @type sec: numbers.Real

    @rtype: timedelta
    """
    assert isinstance(sec, numbers.Real), repr(sec)
    return timedelta(seconds=sec)


def td_to_ms(td):
    r"""Given a timedelta, get the number of microseconds.

    >>> td_to_ms(timedelta(1, 2, 3))
    86402000003

    @deprecated: Use datetime_ex module!

    @type td: timedelta

    @rtype: numbers.Integral
    """
    assert isinstance(td, timedelta), repr(td)
    return td.days * 86400000000 + td.seconds * 1000000 + td.microseconds


def ms_to_td(ms):
    r"""Given a duration in microseconds (int/float), get the timedelta.

    >>> ms_to_td(86402000003)
    datetime.timedelta(1, 2, 3)

    @deprecated: Use datetime_ex module!

    @type ms: numbers.Real

    @rtype: timedelta
    """
    assert isinstance(ms, numbers.Real), repr(ms)
    return timedelta(microseconds=ms)


def time_to_ms(time_):
    r"""Given a time object, get the number of microseconds.

    >>> time_to_ms(time(1, 2, 3, 4))
    3723000004

    @deprecated: Use datetime_ex module!

    @type td: time

    @rtype: numbers.Integral
    """
    assert isinstance(time_, time), repr(time_)
    return (time_.hour * 3600000000 +
            time_.minute * 60000000 +
            time_.second * 1000000 +
            time_.microsecond)


def ms_to_time(ms):
    r"""Given a duration in microseconds (int/float), get the time object.

    >>> ms_to_time(3723000004)
    datetime.time(1, 2, 3, 4)

    @deprecated: Use datetime_ex module!

    @type ms: numbers.Real

    @rtype: time
    """
    assert isinstance(ms, numbers.Real), repr(ms)
    _ms = ms % 1000000

    s = (ms - _ms) // 1000000
    _s = s % 60

    m = (s - _s) // 60
    _m = m % 60

    h = (m - _m) // 60
    _h = h % 24

    return time(hour=_h, minute=_m, second=_s, microsecond=_ms)


def repr_maybe_binary(value):
    r"""The repr-like view of an object, with special handling of binaries.
    >>> repr_maybe_binary((1, 2, 'abc'))
    "(1, 2, 'abc')"
    >>> repr_maybe_binary(buffer(os.urandom(45)))
    '<binary 45 bytes>'

    @return: the "repr"-like representation of an object; if the object
        has a binary contents like buffer, give better representation.
    @rtype: str
    """
    return '<binary {:d} bytes>'.format(len(value)) if isinstance(value,
                                                                  buffer) \
                                                    else repr(value)


def gen_uuid():
    """Generate UUID, according to the current algorithm strategy.

    @rtype: UUID
    """
    return uuid4()


def round_up_to_multiply(numerator, denominator):
    r"""Round up a C{numerator} to be divisable by C{denominator}.

    >>> [(i, round_up_to_multiply(i, 4)) for i in xrange(0, 9)]
    [(0, 0), (1, 4), (2, 4), (3, 4), (4, 4), (5, 8), (6, 8), (7, 8), (8, 8)]

    @type numerator: numbers.Integral
    @type denominator: numbers.Integral
    @return: when given a numerator and a denominator, round up (if needed)
        the numerator to become a proper multiplication of a denominator
    @rtype: numbers.Integral
    """
    d, m = divmod(numerator, denominator)
    return denominator * (d + (1 if m else 0))


def round_down_to_multiply(numerator, denominator):
    r"""Round down a C{numerator} to be divisable by C{denominator}.

    >>> [(i, round_down_to_multiply(i, 4)) for i in xrange(0, 9)]
    [(0, 0), (1, 0), (2, 0), (3, 0), (4, 4), (5, 4), (6, 4), (7, 4), (8, 8)]

    @type numerator: numbers.Integral
    @type denominator: numbers.Integral
    @return: when given a numerator and a denominator, round up (if needed)
        the numerator to become a proper multiplication of a denominator
    @rtype: numbers.Integral
    """
    return numerator - numerator % denominator


def identity(a):
    """Identity function.

    >>> identity(5) == 5
    True

    >>> identity({5: 'abc', 'a': 8}) == {5: 'abc', 'a': 8}
    True

    @returns: Its sole argument.
    """
    return a


def fnmatchany(filename, patterns):
    """
    Given a filename and a sequence of patterns, check
    if the filename matches any of the patterns
    (in an fnmatch()-compatible manner).

    >>> fnmatchany('abc.txt', ('*tx', '*.txt'))
    True

    >>> fnmatchany('abc.txt', ('*doc', '*.doc'))
    False
    """
    return any(fnmatch(filename, f) for f in patterns)


def get_mem_usage_dump():
    """Get a string with the memory usage, for use in the debug environments.

    @precondition: __debug__ is True
    """
    global HEAPY
    result = ''

    if __debug__:
        # You must have python-guppy installed to use it!
        try:
            from guppy import hpy

        except ImportError:
            logger.warning('Install python-guppy '
                               'to see detailed memory usage dumps!')
            result = ''

        else:
            if HEAPY is None:
                # First run: initialization
                HEAPY = hpy()

            else:
                # Any subsequent run: check heap
                heap = HEAPY.heap()
                heapu = HEAPY.heapu()

                result = ('** Reachable memory overview **\n'
                          '* basic *\n'
                          '{heap!s}\n'
                          '* Reference patterns for main memory hog*\n'
                          '{rp!s}\n'
                          '.....\n'
                          '{shpaths!s}\n'
                          '-----\n'
                          '{heapu!s}\n'
                              .format(heap=heap,
                                      rp=heap[0].get_rp(),
                                      shpaths=heap[0].get_rp(),
                                      heapu=heapu))

            HEAPY.setref()

    return result


@contract_epydoc
def on_exception(exc=Exception, default=None, callback=None):
    r"""The decorator around some function C{f},
    such as when function C{f} is called and if it raises exception defined
    as C{exc} (which may be a single exception or a sequence of exceptions),
    then, the exception is ignored, and the result of the function will be
    either the result of the call of C{callback} (if one is present),
    or the C{default} value.

    In general, it is a rough equivalent of C{.get()} dict method, but for a
    function call raising some exception rather than for a dictionary
    item lookup.

    >>> @on_exception(ZeroDivisionError, 'BAD')
    ... def div1(x, y):
    ...     return x / y

    >>> div1(42, 0)
    'BAD'

    >>> def div2(x, y):
    ...     return x // y

    >>> div2(5, 0)
    Traceback (most recent call last):
       ...
    ZeroDivisionError: integer division or modulo by zero

    >>> on_exception((ZeroDivisionError, ValueError), 'BAD')(div2)(5, 0)
    'BAD'

    >>> def cb(exc):
    ...    print('Oops: {!s}'.format(exc))
    >>> on_exception((ZeroDivisionError, ValueError),
    ...              'BAD2',
    ...              lambda e: 'Oops: {!s}'.format(e))(div2)(5, 0)
    'Oops: integer division or modulo by zero'

    @type callback: col.Callable, NoneType
    """

    def wrap(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as exc:
                return callback(exc) if callable(callback) else default

        return wrapper

    return wrap


@contract_epydoc
def random_string(length):
    """Generate a random string of C{length} bytes.

    This function is not cryptographically safe!
    If you need a cryptographically safe function to generate an AES key,
    use C{crypto.gen_rand_key()} instead.

    @rtype: str
    @postcondition: len(result) == length
    """
    return ''.join(imap(chr,
                        (randint(0, 255)
                             for i in xrange(length))))


def ifdef(identifier):
    """
    Hacky low-level function to test whether some identifier is defined,
    by its name.
    In fact, may test any expression for whether it is computable
    in the current context.

    >>> defined_var = 5; ifdef('defined_var')
    True
    >>> ifdef('undefined_var')
    False
    >>> import os; ifdef('os.path')
    True
    >>> ifdef('5 + 7')
    True
    >>> ifdef('5 +.* 7')
    False
    """
    frame = inspect.stack()[1][0]
    try:
        eval(identifier, frame.f_globals, frame.f_locals)
    except:
        return False
    else:
        return True


def memoized(obj):
    """
    The decorator that caches a return value of the function (if it is
    cacheable).
    If called later with the exactly the same arguments,
    the cached value is returned (not reevaluated).
    The new C{_memoized_cache} field is added to the decorated object
    to store the cached values.

    @note: the arguments of the function must be hasheable.
    """
    cache = obj._memoized_cache = {}

    @wraps(obj)
    def memoizer(*args, **kwargs):
        hashed_kwargs = hash(tuple(sorted(kwargs.iteritems())))
        key = (args, hashed_kwargs)
        # Explicit if/else is a performance optimization.
        if key in cache:
            result = cache[key]
        else:
            result = cache[key] = obj(*args, **kwargs)
        return result

    return memoizer


def randevent(num, of):
    """Return a random event with a probability calculated as C{num}/C{of}.

    @rtype: bool
    """
    return num < randrange(of)


def get_wider_locales(locale):
    """
    Given a locale name, return all the locale names which generally can
    be used as wider (or equal) definitions than the original locale.

    >>> get_wider_locales('en_US')
    ['en_US', 'en', 'c']
    >>> get_wider_locales('ru_RU.UTF-8')
    ['ru_RU', 'ru', 'c']
    >>> get_wider_locales('en')
    ['en', 'c']
    >>> get_wider_locales('c')
    ['c']

    @postcondition: result[-1] == 'c'

    @returns: the locales which are wider (or equal) than the input one,
        sorted from the narrowest one to the widest one.
    """
    return sorted({'c',
                   locale.split('_')[0],
                   locale.split('.')[0]},
                  key=len,
                  reverse=True)


def in_main_thread():
    """Whether the code is running in the main ("reactor" thread).

    >>> in_main_thread()  # more tests in TestInMainThread
    True

    @note: assumes the threads were not ever renamed.

    @note: global variable/constant C{_FORCE_SPOOF_AS_SECONDARY_THREAD}
        can affect the behaviour of this function, but only for the unit tests.

    @rtype: bool
    """
    return False if _FORCE_SPOOF_AS_SECONDARY_THREAD \
                 else (threadable.isInIOThread() or
                       threading.current_thread().name == 'MainThread')


def audit_leaks(prefix):
    """Perform an iteration of memory leak audits."""
    import random

    import objgraph

    dt = datetime.utcnow()
    dt_str = str(dt) + '\n'

    # g_with_size = [(sys.getsizeof(g), g) for g in gc.garbage]
    # g_sorted = sorted(g_with_size,
    #                   reverse=True,
    #                   cmp=lambda x, y: cmp(x[0], y[0]))[:5]

    logger.debug('Leak audits started')

    tear = '-------------------------------------------\n'

    # import meliae.scanner
    # meliae.scanner.dump_all_objects('memdump-{}-{}.json'.format(prefix, dt))

    # if g_sorted:
    #     with open('memdump-{}-{}.txt'.format(prefix, dt), 'a') as fh:
    #         fh.writelines((tear,
    #                        dt_str,
    #                        pformat(g_sorted)))

    #     with open('referrers-{}-{}.txt'.format(prefix, dt), 'a') as fh:
    #         fh.writelines((tear,
    #                        dt_str,
    #                        pformat(gc.get_referrers(g_sorted[0]))))

    # del gc.garbage[:]

    # with open('memusage-{}.txt'.format(prefix), 'a') as fh:
    #     fh.writelines((tear,
    #                    dt_str,
    #                    get_mem_usage_dump()))

    # # objgraph.show_growth()

    # roots = objgraph.get_leaking_objects()
    # with open('leaking-{}-{}.txt'.format(prefix, dt), 'a') as fh:
    #     txt = '\n'.join('{!r}: {!r}'.format(*row)
    #                         for row
    #                             in objgraph.most_common_types(objects=roots))
    #     fh.writelines((tear,
    #                    dt_str,
    #                    pformat(g_sorted),
    #                    txt))

    # objgraph.show_refs(roots[:3],
    #                    refcounts=True,
    #                    filename='showrefs-{}-{}.png'.format(prefix, dt))

    # by_type = objgraph.by_type('list')
    # objgraph.show_chain(objgraph.find_backref_chain(
    #                         random.choice(by_type),
    #                         inspect.ismodule),
    #                     filename='chain-{}-{}.png'.format(prefix, dt))

    # objgraph.show_backrefs(objgraph.by_type('Nondestructible'),
    #                        filename='finalizers-{}-{}.png'.format(prefix, dt))

    logger.debug('Leak audits stopped')


def reprefix(what, old_prefix, new_prefix):
    r"""
    Given a string C{what} with some prefix C{old_prefix}, change
    the prefix to C{new_prefix}.

    >>> reprefix('abcdefgh', 'abc', '01234')
    '01234defgh'

    >>> reprefix('abcdefgh', '', '01234')
    '01234abcdefgh'

    >>> reprefix('abcdefgh', '', '01234')
    '01234abcdefgh'

    @type what: basestring
    @type old_prefix: basestring
    @type new_prefix: basestring
    @rtype: basestring

    @raises ValueError: if C{what} doesn't start with C{old_prefix}.
    """
    if not what.startswith(old_prefix):
        raise ValueError(u"{!r} doesn't start with {!r}!"
                             .format(what, old_prefix))
    old_prefix_length = len(old_prefix)
    return new_prefix + what[old_prefix_length:]


def compose(*fs):
    r"""When given f1, f2…, fn, returns new function f = f1 ° f2 ° … ° fn.

    f(*args, **kwargs) = f1(f2(….fn(*args, *kwargs)))

    >>> inc = lambda i: i + 1; dec = lambda i: i - 1
    >>> twice = lambda i: i * 2; half = lambda i: i // 2
    >>> compose(inc, twice)(3)
    7
    >>> compose(inc, twice, dec)(3)
    5
    >>> compose(inc, inc, twice, inc)(3)
    10
    >>> compose(inc, twice, inc, inc)(3)
    11
    >>> compose(dec, half, inc)(3)
    1

    @note: f1, f2,... f(n-1) must be one-argument functions.
    """
    assert fs, repr(fs)
    assert all(callable(f) for f in fs), repr(fs)


    @wraps(fs[0])
    def composed_function(*args, **kwargs):
        return reduce(lambda value, f: f(value),
                      fs[-2::-1],
                      fs[-1](*args, **kwargs))


    return composed_function


def antisymmetric_comparison(cls):
    r"""
    A class decorator to add a missing C{__ne__} or C{__eq__} class method
    to enforce antisymmetric comparison on this class (and its subclasses).

    Implemented similarly to the C{functools.total_ordering}.

    >>> class EQ(object):
    ...     def __init__(self, a): self.a = a
    ...     def __eq__(self, other): return self.a == other.a
    >>> class NE(object):
    ...     def __init__(self, a): self.a = a
    ...     def __ne__(self, other): return self.a != other.a

    >>> # How these dummy classes work...
    >>> EQ(5) == EQ(5), EQ(5) == EQ(7), NE(5) != NE(7), NE(5) != NE(5)
    (True, False, True, False)

    >>> # And how they don't:
    >>> EQ(5) != EQ(5), EQ(5) != EQ(7), NE(5) == NE(7), NE(5) == NE(5)
    (True, True, False, False)
    >>> # ... though they should be (False, True, False, True)!

    >>> # But if we decorate them?
    >>> f = antisymmetric_comparison
    >>> f(EQ)(5) != f(EQ)(5), f(EQ)(5) != f(EQ)(7)
    (False, True)
    >>> f(NE)(5) == f(NE)(7), f(NE)(5) == f(NE)(5)
    (False, True)
    >>> # ... works perfect!

    >>> # And what about subclasses?

    >>> class EQ1(EQ):
    ...     def __init__(self, b, *args, **kwargs):
    ...         super(EQ1, self).__init__(*args, **kwargs)
    ...         self.b = b
    ...     def __eq__(self, other):
    ...         return super(EQ1, self).__eq__(other) and self.b == other.b
    >>> class NE1(NE):
    ...     def __init__(self, b, *args, **kwargs):
    ...         super(NE1, self).__init__(*args, **kwargs)
    ...         self.b = b
    ...     def __ne__(self, other):
    ...         return super(NE1, self).__ne__(other) or self.b != other.b

    >>> EQ1(5, 5) == EQ1(5, 5), EQ1(5, 5) == EQ1(5, 6), EQ1(5, 6) == EQ1(7, 8)
    (True, False, False)

    >>> NE1(5, 6) != NE1(7, 8), NE1(5, 5) != NE1(5, 5), NE1(5, 5) != NE1(5, 6)
    (True, False, True)

    >>> # But does the subclassed code works antisymmetrically?
    >>> EQ1(5, 5) != EQ1(5, 5), EQ1(5, 5) != EQ1(5, 6), EQ1(5, 6) != EQ1(7, 8)
    (False, True, True)
    >>> NE1(5, 6) == NE1(7, 8), NE1(5, 5) == NE1(5, 5), NE1(5, 5) == NE1(5, 6)
    (False, True, False)
    >>> # ... yes. Problem solved!

    >>> # Another test. What if the types cannot be compared?
    >>> EQ(5) == EQ('A'), NE(5) != NE('A'), EQ(5) != EQ('A'), NE(5) == NE('A')
    (False, True, True, False)
    >>> f(EQ)(5) == f(EQ)('A'), f(NE)(5) != f(NE)('A')
    (False, True)
    >>> f(EQ)(5) != f(EQ)('A'), f(NE)(5) == f(NE)('A')
    (True, False)
    """
    convert = {
        '__eq__': [('__ne__', lambda self, other: not self.__eq__(other))],
        '__ne__': [('__eq__', lambda self, other: not self.__ne__(other))],
    }
    roots = set(dir(cls)) & set(convert)
    if not roots:
        raise ValueError('must define at least one comparison operation: '
                         '__eq__ or __ne__')

    root = max(roots)  # prefer __eq__ to __ne__
    for opname, opfunc in convert[root]:
        if opname not in roots:
            opfunc.__name__ = opname
            opfunc.__doc__ = getattr(int, opname).__doc__
            setattr(cls, opname, opfunc)
    return cls


COMPILED_REGEX = re.compile(
                     r'^(?P<base_name>.+) \(copy (?P<copy_number>\d+)\)$')
"""This compiled regex is used to rename uploading files"""

def suggest_file_copy_name(filename):
    """Suggest a new filename for a copy of the file.

    >>> suggest_file_copy_name('file with some image.png')
    u'file with some image (copy 1).png'

    >>> suggest_file_copy_name('file with some image (copy 1).png')
    u'file with some image (copy 2).png'

    >>> suggest_file_copy_name('some_another_file (copy 2) (copy 4).png')
    u'some_another_file (copy 2) (copy 5).png'

    >>> suggest_file_copy_name('file without extension (copy 3) (copy 0)')
    u'file without extension (copy 3) (copy 1)'

    >>> suggest_file_copy_name(' (copy 3) (copy 8)')
    u' (copy 3) (copy 9)'

    @param filename: some name if the file 0_0
    @type filename: basestring

    @return: name for a copy of a file
    """
    assert filename, repr(filename)

    root, ext = posixpath.splitext(filename)
    match = COMPILED_REGEX.match(root)
    if match is not None:
        match_dict = match.groupdict()
        base_name, copy_number = match_dict['base_name'],\
                                 int(match_dict['copy_number']) + 1
    else:
        base_name, copy_number = root, 1
    return u'{} (copy {}){}'.format(base_name, copy_number, ext)


def normalize_unicode(text):
    """Normalize any unicode string to its normal composed form.

    >>> normalize_unicode(u'\u0401-\u043c\u043e\u0451')
    u'\u0401-\u043c\u043e\u0451'
    >>> normalize_unicode(u'\u0415\u0308-\u043c\u043e\u0435\u0308')
    u'\u0401-\u043c\u043e\u0451'

    @type text: unicode
    @rtype: unicode
    """
    return unicodedata.normalize('NFC', text)



#
# Unit tests
#

if __debug__:
    if 'nose' in sys.modules:
        from twisted.trial import unittest

        class TestInMainThread(unittest.TestCase):
            """Test C{in_main_thread()} function."""

            __slots__ = ('__watchdog',
                         '__reactor',
                         '__callInThread_success',
                         '__blockingCallFromThread_success',
                         '__deferToThread_success')


            def test_in_main_thread(self):
                from twisted.internet import reactor, defer
                self.assertTrue(in_main_thread(), msg='__main__')

                self.__reactor = reactor

                # Prepare bool flags that store what succeeded and what not.
                self.__callInThread_success = False
                self.__blockingCallFromThread_success = False
                self.__deferToThread_success = False

                # Prepare watchdog that stops the unit test in 5 seconds
                # unconditionally
                self.__watchdog = \
                    self.__reactor.callLater(5.0, self.__watchdog_cb)

                # Start the chain of thread-swaps
                d = defer.Deferred()
                self.__reactor.callInThread(self.__callInThread, d)
                return d


            def __watchdog_cb(self):
                self.__final_check()


            def __callInThread(self, d):
                self.__callInThread_success = True

                from twisted.internet import threads
                self.assertFalse(in_main_thread(),
                                 msg='reactor.callInThread()')

                result = threads.blockingCallFromThread(
                             self.__reactor, self.__blockingCallFromThread, d)
                self.assertEqual(result, 42)


            def __blockingCallFromThread(self, d):
                self.__blockingCallFromThread_success = True

                from twisted.internet import threads

                self.assertTrue(in_main_thread(),
                                msg='threads.blockingCallFromThread()')

                d2 = threads.deferToThread(self.__deferToThread)
                d2.addCallback(d.callback)
                return 42


            def __deferToThread(self):
                self.__deferToThread_success = True

                self.assertFalse(in_main_thread(),
                                 msg='threads.deferToThread()')

                self.__watchdog.cancel()
                self.__final_check()


            def __final_check(self):
                self.assertTrue(self.__callInThread_success,
                                msg='__callInThread_success')
                self.assertTrue(self.__blockingCallFromThread_success,
                                msg='__blockingCallFromThread_success')
                self.assertTrue(self.__deferToThread_success,
                                msg='__deferToThread_success')


def _nottest(func):
    """Decorator to mark a function or method as *not* a test (for nosetests).

    Cloned from nose.tools."""
    func.__test__ = False
    return func



#
# Main
#

__pp = pprint_module.PrettyPrinter(indent=4, width=120, depth=4)
pformat = __pp.pformat

# Never use pprint in release code!
if __debug__:
    pprint = __pp.pprint

    if __name__ == '__main__':
        import doctest
        doctest.testmod()
