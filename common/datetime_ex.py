#!/usr/bin/python
"""
Various extensions to the standard Python C{datetime} module.
"""
from __future__ import absolute_import
from __future__ import division
import numbers
from datetime import datetime, time, timedelta



#
# Constants
#

MICROSECONDS_IN_DAY = 86400000000
MICROSECONDS_IN_HOUR = 3600000000
MICROSECONDS_IN_MINUTE = 60000000
MICROSECONDS_IN_SECOND = 1000000



#
# Classes
#

class TimeDeltaEx(timedelta):
    """
    An extended version of standard Python timedelta class,
    with various additional operations such as conversion from/time microseconds,
    """

    def __repr__(self):
        """
        >>> TimeDeltaEx.from_timedelta(timedelta(0))
        TimeDeltaEx(0)
        >>> TimeDeltaEx.from_timedelta(timedelta(1))
        TimeDeltaEx(1)
        >>> TimeDeltaEx.from_timedelta(timedelta(1, 2))
        TimeDeltaEx(1, 2)
        >>> TimeDeltaEx.from_timedelta(timedelta(1, 2, 3))
        TimeDeltaEx(1, 2, 3)
        """
        if not self.microseconds:
            if not self.seconds:
                return "TimeDeltaEx(%i)" % (self.days,)
            else:
                return "TimeDeltaEx(%i, %i)" % (self.days, self.seconds)
        else:
            return "TimeDeltaEx(%i, %i, %i)" % (self.days, self.seconds, self.microseconds)


    def timedelta(self):
        """
        >>> TimeDeltaEx(1, 2, 3).timedelta()
        datetime.timedelta(1, 2, 3)
        """
        return timedelta(self.days, self.seconds, self.microseconds)


    @classmethod
    def from_timedelta(cls, td):
        """
        >>> TimeDeltaEx.from_timedelta(timedelta(1, 2, 3))
        TimeDeltaEx(1, 2, 3)
        """
        assert isinstance(td, timedelta)
        return cls(td.days, td.seconds, td.microseconds)


    def in_microseconds(self):
        """
        Get the number of microseconds in the time difference object.

        @rtype: numbers.Integral

        >>> TimeDeltaEx(1, 2, 3).in_microseconds()
        86402000003
        """
        return (self.days    * MICROSECONDS_IN_DAY    +
                self.seconds * MICROSECONDS_IN_SECOND +
                self.microseconds)


    def in_seconds(self):
        """
        Get the number of seconds in the time difference object.
        Note, may be inexact!

        @rtype: float

        >>> str(TimeDeltaEx(1, 2, 3).in_seconds())
        '86402.000003'
        """
        return self.in_microseconds() / 1000000.0


    @classmethod
    def from_microseconds(cls, microseconds):
        """
        Given a duration in microseconds (int/float),
        get the appropriate TimeEx object.

        @type microseconds: numbers.Real

        @rtype: TimeDeltaEx

        >>> TimeDeltaEx.from_microseconds(86402000003)
        TimeDeltaEx(1, 2, 3)
        """
        assert isinstance(microseconds, numbers.Real), repr(microseconds)

        return cls(microseconds = microseconds)


    def __div__(self, other):
        """
        @type other: timedelta, numbers.Real
        @rtype: TimeDeltaEx, numbers.Real

        >>> TimeDeltaEx(seconds = 5) / timedelta(seconds = 2)
        2.5
        >>> TimeDeltaEx(seconds = 5) / 4
        TimeDeltaEx(0, 1, 250000)
        >>> TimeDeltaEx(microseconds = 75) / 2.5
        TimeDeltaEx(0, 0, 30)
        """
        assert isinstance(other, (timedelta, numbers.Real)), repr(other)

        if isinstance(other, timedelta):
            return self.in_microseconds() / TimeDeltaEx.from_timedelta(other).in_microseconds()
        elif isinstance(other, numbers.Real):
            return TimeDeltaEx.from_microseconds(self.in_microseconds() / other)
        else:
            raise TypeError("Unsupported divisor in %r / %r!" % (self, other))

    __truediv__ = __div__


    def __rdiv__(self, other):
        """
        @type other: timedelta
        @rtype: TimeDeltaEx

        >>> timedelta(seconds = 5) / TimeDeltaEx(seconds = 2)
        2.5
        """
        assert isinstance(other, (timedelta, numbers.Real))

        return TimeDeltaEx.from_timedelta(other).in_microseconds() / self.in_microseconds()

    __rtruediv__ = __rdiv__


    def __floordiv__(self, other):
        """
        @type other: timedelta, numbers.Real
        @rtype: TimeDeltaEx, numbers.Real

        >>> TimeDeltaEx(seconds = 5) // timedelta(seconds = 2)
        2
        >>> TimeDeltaEx(seconds = 5) // 4
        TimeDeltaEx(0, 1, 250000)
        >>> TimeDeltaEx(microseconds = 75) // 2.6
        TimeDeltaEx(0, 0, 28)
        """
        assert isinstance(other, (timedelta, numbers.Real)), repr(other)

        if isinstance(other, timedelta):
            return self.in_microseconds() // TimeDeltaEx.from_timedelta(other).in_microseconds()
        elif isinstance(other, numbers.Real):
            return TimeDeltaEx.from_microseconds(self.in_microseconds() // other)
        else:
            raise TypeError("Unsupported divisor in %r // %r!" % (self, other))


    def __rfloordiv__(self, other):
        """
        @type other: timedelta
        @rtype: int

        >>> timedelta(seconds = 5) // TimeDeltaEx(seconds = 2)
        2
        """
        assert isinstance(other, timedelta)

        return TimeDeltaEx.from_timedelta(other).in_microseconds() // self.in_microseconds()


    def __mod__(self, other):
        """
        @type other: timedelta,int, float, long
        @rtype: TimeDeltaEx, int, float, long

        >>> TimeDeltaEx(seconds = 42) % timedelta(seconds = 11)
        TimeDeltaEx(0, 9)
        """
        assert isinstance(other, (timedelta, int, float, long))

        if isinstance(other, timedelta):
            return TimeDeltaEx.from_microseconds(self.in_microseconds() % TimeDeltaEx.from_timedelta(other).in_microseconds())
        elif isinstance(other, (int, float, long)):
            raise NotImplementedError()
        else:
            raise TypeError("Unsupported divisor in %r %% %r!" % (self, other))


    def __rmod__(self, other):
        """
        @type other: timedelta
        @rtype: TimeDeltaEx

        >>> timedelta(seconds = 42) % TimeDeltaEx(seconds = 11)
        TimeDeltaEx(0, 9)
        """
        assert isinstance(other, timedelta)

        return TimeDeltaEx.from_microseconds(TimeDeltaEx.from_timedelta(other).in_microseconds() % self.in_microseconds())


    def __divmod__(self, other):
        """
        @type other: timedelta, int, float, long
        @rtype: TimeDeltaEx, int, float, long

        >>> divmod(TimeDeltaEx(seconds = 42), timedelta(seconds = 11))
        (3, TimeDeltaEx(0, 9))
        """
        assert isinstance(other, (timedelta, int, float, long))
        self_microseconds = self.in_microseconds()
        if isinstance(other, timedelta):
            other_microseconds = TimeDeltaEx.from_timedelta(other).in_microseconds()

            return (self_microseconds // other_microseconds,
                    TimeDeltaEx.from_microseconds(self_microseconds % other_microseconds))
        elif isinstance(other, (int, float, long)):
            raise NotImplementedError()
        else:
            raise TypeError("Unsupported divisor in divmod(%r, %r)!" % (self, other))


    def __rdivmod__(self, other):
        """
        @type other: timedelta
        @rtype: TimeDeltaEx

        >>> divmod(TimeDeltaEx(seconds = 42), timedelta(seconds = 11))
        (3, TimeDeltaEx(0, 9))

        >>> divmod(timedelta(seconds = 42), TimeDeltaEx(seconds = 11))
        (3, TimeDeltaEx(0, 9))
        """
        assert isinstance(other, timedelta)

        assert isinstance(other, timedelta)
        (self_microseconds,
         other_microseconds) = (self.in_microseconds(),
                                TimeDeltaEx.from_timedelta(other).in_microseconds())

        return (other_microseconds // self_microseconds,
                TimeDeltaEx.from_microseconds(other_microseconds % self_microseconds))


    def __mul__(self, n):
        """
        @type other: numbers.Real
        @rtype: TimeDeltaEx

        >>> TimeDeltaEx(seconds = 5) * 5
        TimeDeltaEx(0, 25)
        >>> 5 * TimeDeltaEx(seconds = 5)
        TimeDeltaEx(0, 25)
        >>> TimeDeltaEx(microseconds = 50) * 0.77
        TimeDeltaEx(0, 0, 39)
        >>> 0.77 * TimeDeltaEx(microseconds = 50)
        TimeDeltaEx(0, 0, 39)
        """
        assert isinstance(n, (int, float, long))

        return TimeDeltaEx.from_microseconds(self.in_microseconds() * n)

    __rmul__ = __mul__



class TimeEx(time):

    def __repr__(self):
        """
        >>> TimeEx(0)
        TimeEx(0)
        >>> TimeEx(1)
        TimeEx(1)
        >>> TimeEx(1, 2)
        TimeEx(1, 2)
        >>> TimeEx(1, 2, 3)
        TimeEx(1, 2, 3)
        >>> TimeEx(1, 2, 3, 4)
        TimeEx(1, 2, 3, 4)
        """
        if not self.microsecond:
            if not self.second:
                if not self.minute:
                    return "TimeEx(%i)" % (self.hour,)
                else:
                    return "TimeEx(%i, %i)" % (self.hour, self.minute)
            else:
                return "TimeEx(%i, %i, %i)" % (self.hour, self.minute, self.second)
        else:
            return "TimeEx(%i, %i, %i, %i)" % (self.hour, self.minute, self.second, self.microsecond)


    FORMAT_STRING = "%H:%M:%S.%f"

    def sqlite3_adapt(self):
        """
        Use as::

          sqlite3.register_adapter(TimeEx, TimeEx.sqlite3_adapt)

        >>> TimeEx(15, 16, 17, 18).sqlite3_adapt()
        '15:16:17.000018'
        """
        return self.strftime(TimeEx.FORMAT_STRING)


    @classmethod
    def sqlite3_convert(cls, value):
        """
        Use as::

          sqlite3.register_converter('time', TimeEx.sqlite3_convert)

        >>> TimeEx.sqlite3_convert('15:16:17.000018')
        TimeEx(15, 16, 17, 18)
        """
        return TimeEx.from_time(datetime.strptime(value, TimeEx.FORMAT_STRING).time())


    def time(self):
        """
        >>> TimeEx(1, 2, 3, 4).time()
        datetime.time(1, 2, 3, 4)
        """
        return time(self.hour, self.minute, self.second, self.microsecond)


    @classmethod
    def from_time(cls, t):
        """
        >>> TimeEx.from_time(time(1, 2, 3, 4))
        TimeEx(1, 2, 3, 4)
        """
        return cls(t.hour, t.minute, t.second, t.microsecond, t.tzinfo)


    def in_microseconds(self):
        """
        Return the amount of microseconds from the midnight.

        >>> TimeEx.from_microseconds(3723000004).in_microseconds()
        3723000004
        """
        return (self.hour   * MICROSECONDS_IN_HOUR   +
                self.minute * MICROSECONDS_IN_MINUTE +
                self.second * MICROSECONDS_IN_SECOND +
                self.microsecond)

    @classmethod
    def from_microseconds(cls, microseconds):
        """
        Given the amount of microseconds from the midnight,
        return the appropriate TimeEx object.

        @type microseconds: numbers.Real

        @rtype: TimeEx

        >>> TimeEx.from_microseconds(3723000004)
        TimeEx(1, 2, 3, 4)
        """
        assert isinstance(microseconds, numbers.Real), repr(microseconds)
        ms = microseconds % MICROSECONDS_IN_DAY

        _ms = ms % MICROSECONDS_IN_SECOND

        s = (ms - _ms) // MICROSECONDS_IN_SECOND
        _s = s % 60

        m = (s - _s) // 60
        _m = m % 60

        h = (m - _m) // 60
        _h = h % 24

        return cls(hour = _h, minute = _m, second = _s, microsecond = _ms)


    def __add__(self, other):
        """
        >>> TimeEx(23, 44, 55) + timedelta(hours = 3, minutes = 20)
        TimeEx(3, 4, 55)
        """
        assert isinstance(other, timedelta)
        microseconds = self.in_microseconds() + TimeDeltaEx.from_timedelta(other).in_microseconds()
        return TimeEx.from_microseconds(microseconds)


    def __sub__(self, other):
        """
        >>> TimeEx(3, 4, 55) - timedelta(hours = 3, minutes = 20)
        TimeEx(23, 44, 55)
        """
        assert isinstance(other, timedelta)
        microseconds = self.in_microseconds() - TimeDeltaEx.from_timedelta(other).in_microseconds()
        return TimeEx.from_microseconds(microseconds)



#
# Main
#

if __name__ == "__main__":
    import doctest
    doctest.testmod()
