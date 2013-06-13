#!/usr/bin/python
"""
The base classes to store the information about the FS events
in an uniform way.
"""

from datetime import datetime
from common.types import NoneType
from twisted.python import filepath


class Event(object):
    """Base class for FS events."""
    __slots__ = ('path', 'ts')

    def __init__(self, path, ts=None):
        """
        @param ts: timestamp of event creation time.
        @type ts: datetime, NoneType
        """
        assert isinstance(path, filepath.FilePath)
        self.path = path

        if ts is None:
            ts = datetime.utcnow()
        self.ts = ts

    def __repr__(self):
        return '{}(path={!r}'.format(self.__class__.__name__, self.path) \
               + (', ts={!s}'.format(self.ts) if self.ts is not None else '') \
               + ')'

    @property
    def _constructor_args(self):
        return {'path': self.path,
                'ts': self.ts}

    def copy(self):
        return self.__class__(**self._constructor_args)


class DeleteEvent(Event):
    pass


class CreateEvent(Event):
    pass


class ModifyEvent(Event):
    pass


class MoveEvent(Event):
    __slots__ = ('to_path',)

    def __init__(self, from_path, to_path, ts=None):
        super(MoveEvent, self).__init__(from_path, ts=ts)
        self.to_path = to_path

    @property
    def _constructor_args(self):
        """
        Overwrites the C{_constructor_args} from the base class,
        rather than updates it.
        """
        return {'from_path': self.from_path,
                'to_path': self.to_path,
                'ts': self.ts}

    @property
    def from_path(self):
        return self.path

    @from_path.setter
    def from_path(self, value):
        self.path = value

    def __repr__(self):
        return u'{}(from_path={!r}, to_path={!r}'.format(
                   self.__class__.__name__,
                   self.from_path,
                   self.to_path) \
               + (', ts={!s}'.format(self.ts) if self.ts is not None else '') \
               + ')'
