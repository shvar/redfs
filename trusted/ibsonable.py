#!/usr/bin/python
"""The interface to convert the data to BSON and back."""

#
# Imports
#

from __future__ import absolute_import
import warnings
from abc import ABCMeta, abstractmethod
import warnings

import bson


warnings.filterwarnings('ignore',
                        message=r"^couldn't encode",
                        category=RuntimeWarning,
                        module='^bson$')



#
# Classes
#

class IBSONable(object):
    """
    This interface requires you to implement the methods converting the object
    from and to BSON-compatible structure.
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def to_bson(self):
        """
        Convert this object to the simple BSON-compatible structure.

        Can be inherited (if it is ok that the inherited class is serialized
        to a dictionary).
        """
        return {}


    @classmethod
    @abstractmethod  # TODO: should become @abstractclassmethod
    def from_bson(cls, doc):  # pylint:disable=E0213
        """
        Create this object from a simple BSON-compatible structure.
        The derived method must be a classmethod.
        Such method should return a callable (probably functools.partial
        object) which creates an instance of C{cls} class from C{json_struct}
        upon call.

        Why callable rather than the class itself? This way, the from_json
        implementation may be inherited.

        @rtype: col.Callable
        """
        return cls


    def __str__(self):
        return u', '.join(u'{}={!r}'.format(k, v)
                              for k, v in self.to_bson().iteritems())


    def __repr__(self):
        return u'{}.from_bson({})'.format(self.__class__.__name__, self)


    def is_valid_bsonable(self):
        """Whether the contents of the object indeed can be serialized to BSON.

        @rtype: bool
        """
        with warnings.catch_warnings(RuntimeWarning):
            try:
                self.bson_encode()
            except bson.InvalidDocument:
                return False
            else:
                return True


    def bson_encode(self):
        r"""Serialize to the BSON stream.

        @rtype: str

        @raises bson.InvalidDocument: if cannot be encoded properly.
        """
        return bson.BSON.encode(self.to_bson())



if __debug__:
    __test__ = {
        'test-IBSONable':
            r"""
            >>> from common.abstractions import IPrintable
            >>> class SampleBSONableClass(IPrintable, IBSONable):
            ...     def __init__(self, a, b):
            ...         self.a = a
            ...         self.b = b
            ...
            ...     def __str__(self):
            ...         return 'a={self.a!r}, b={self.b!r}'.format(self=self)
            ...
            ...     def to_bson(self):
            ...         return {'a': self.a, 'b': self.b}
            ...
            ...     @classmethod
            ...     def from_bson(cls, doc):
            ...         return cls(a=doc['a'], b=doc['b'])

            >>> valid = SampleBSONableClass(5, None)
            >>> valid
            SampleBSONableClass(a=5, b=None)
            >>> valid.to_bson()
            {'a': 5, 'b': None}
            >>> valid.bson_encode()
            '\x0f\x00\x00\x00\x10a\x00\x05\x00\x00\x00\nb\x00\x00'
            >>> invalid = \
            ...     SampleBSONableClass.from_bson({'a': 9, 'b': xrange(11)})
            >>> invalid
            SampleBSONableClass(a=9, b=xrange(11))
            >>> invalid.to_bson()
            {'a': 9, 'b': xrange(11)}
            >>> valid.is_valid_bsonable()
            True
            >>> invalid.is_valid_bsonable()
            False
            """
    }
