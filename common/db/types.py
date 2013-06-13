#!/usr/bin/python
"""
Extra types for the models.
"""

#
# Imports
#

from __future__ import absolute_import
import numbers
import uuid

from sqlalchemy import types
try:
    from sqlalchemy.dialects.postgresql import UUID as pg_UUID
except:
    pg_UUID = None

from contrib.dbc import contract_epydoc

#
# Functions
#

@contract_epydoc
def crc32_to_signed(crc32):
    """Convert internal unsigned representation of the CRC32 to the signed int.

    >>> crc32_to_signed(0)
    0
    >>> crc32_to_signed(0xFFFFFFFF)
    -1
    >>> crc32_to_signed(0x7FFFFFFF)
    2147483647
    >>> crc32_to_signed(0x80000000)
    -2147483648
    >>> crc32_to_signed(0x80000001)
    -2147483647
    >>> crc32_to_signed(0x8879817E)
    -2005302914
    >>> crc32_to_signed(0x63211234)
    1663111732

    @type crc32: int
    @precondition: 0 <= crc32 <= 0xFFFFFFFF
    @postcondition: -2147483648 <= result <= 2147483647
    """
    result = int(crc32) & 0xFFFFFFFF
    return result - 0x100000000 if result > 0x7FFFFFFF else result


@contract_epydoc
def crc32_to_unsigned(crc32):
    """
    Convert database signed representation of the CRC32 to
    the internal unsigned.

    >>> hex(crc32_to_unsigned(0))
    '0x0'
    >>> hex(crc32_to_unsigned(-1))
    '0xffffffff'
    >>> hex(crc32_to_unsigned(2147483647))
    '0x7fffffff'
    >>> hex(crc32_to_unsigned(-2147483648))
    '0x80000000'
    >>> hex(crc32_to_unsigned(-2147483647))
    '0x80000001'
    >>> hex(crc32_to_unsigned(-2005302914))
    '0x8879817e'
    >>> hex(crc32_to_unsigned(1663111732))
    '0x63211234'

    @type crc32: int
    @precondition: -2147483648 <= crc32 <= 2147483647
    @postcondition: 0 <= result <= 0xFFFFFFFF
    """
    return int(crc32) & 0xFFFFFFFF



#
# Classes
#

class CRC32(types.TypeDecorator):
    """CRC32 mapper to CRC32 sqlite3 type."""

    impl = types.Integer


    def process_bind_param(self, value, dialect):
        r"""Implement TypeDecorator interface.

        >>> from sqlalchemy.dialects import sqlite as sqlite_dialect, \
        ...                                 postgresql as pg_dialect
        >>> test_sq = lambda value: CRC32().process_bind_param(
        ...                                     value,
        ...                                     sqlite_dialect.dialect())
        >>> test_pg = lambda value: CRC32().process_bind_param(
        ...                                     value,
        ...                                     pg_dialect.dialect())

        >>> test_sq(0), test_sq(-1), test_sq(0xFFFFFFFF)
        (0, 4294967295, 4294967295)
        >>> test_pg(0), test_pg(-1), test_pg(0xFFFFFFFF)
        (0, -1, -1)

        >>> test_sq(0x7FFFFFFF), test_sq(0x80000000)
        (2147483647, 2147483648)
        >>> test_pg(0x7FFFFFFF), test_pg(0x80000000)
        (2147483647, -2147483648)

        >>> test_sq(-2147483648), test_sq(-2147483647)
        (2147483648, 2147483649)
        >>> test_pg(-2147483648), test_pg(-2147483647)
        (-2147483648, -2147483647)

        >>> test_sq(0x8879817E), test_sq(-2005302914)
        (2289664382, 2289664382)
        >>> test_pg(0x8879817E), test_pg(-2005302914)
        (-2005302914, -2005302914)
        """
        assert isinstance(value, numbers.Integral), repr(value)
        if value is None:
            return value
        else:
            if dialect.name == 'sqlite':
                return crc32_to_unsigned(value)
            elif dialect.name == 'postgresql':
                return crc32_to_signed(value)
            else:
                raise NotImplementedError(u'No process_bind_param({!r}, {!r})'
                                              .format(value, dialect))


    def process_result_value(self, value, dialect):
        r"""Implement TypeDecorator interface.

        >>> from sqlalchemy.dialects import sqlite as sqlite_dialect, \
        ...                                 postgresql as pg_dialect
        >>> test_sq = lambda value: CRC32().process_result_value(
        ...                                     value,
        ...                                     sqlite_dialect.dialect())
        >>> test_pg = lambda value: CRC32().process_result_value(
        ...                                     value,
        ...                                     pg_dialect.dialect())

        >>> test_sq(0), test_sq(-1), test_sq(0xFFFFFFFF)
        (0, 4294967295, 4294967295)
        >>> test_pg(0), test_pg(-1), test_pg(0xFFFFFFFF)
        (0, 4294967295, 4294967295)

        >>> test_sq(0x7FFFFFFF), test_sq(0x80000000)
        (2147483647, 2147483648)
        >>> test_pg(0x7FFFFFFF), test_pg(0x80000000)
        (2147483647, 2147483648)

        >>> test_sq(-2147483648), test_sq(-2147483647)
        (2147483648, 2147483649)
        >>> test_pg(-2147483648), test_pg(-2147483647)
        (2147483648, 2147483649)

        >>> test_sq(0x8879817E), test_sq(-2005302914)
        (2289664382, 2289664382)
        >>> test_pg(0x8879817E), test_pg(-2005302914)
        (2289664382, 2289664382)
        """
        assert isinstance(value, numbers.Integral), repr(value)
        if value is None:
            return value
        else:
            if dialect.name in ('sqlite', 'postgresql'):
                return crc32_to_unsigned(value)
            else:
                raise NotImplementedError(
                          u'No process_result_value({!r}, {!r})'
                              .format(value, dialect))



class UUID(types.TypeDecorator):
    """UUID mapper to UUIDTEXT sqlite3 type or UUID postgresql type.

    See also:
    http://docs.sqlalchemy.org/en/latest/core/types.html
        #backend-agnostic-guid-type
    """

    impl = types.CHAR


    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            assert pg_UUID is not None
            return dialect.type_descriptor(pg_UUID())
        else:
            return dialect.type_descriptor(types.CHAR(32))


    def process_bind_param(self, value, dialect):
        r"""Implement TypeDecorator interface."""
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if isinstance(value, uuid.UUID):
                return value.hex
            else:
                return uuid.UUID(value).hex


    def process_result_value(self, value, dialect):
        r"""Implement TypeDecorator interface."""
        if value is None:
            return value
        elif isinstance(value, uuid.UUID):
            return value
        else:
            return uuid.UUID(value)
