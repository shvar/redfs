#!/usr/bin/python
"""
Structures and classes related to Chunks/Blocks/Files stored in the cloud.
"""

#
# Imports
#

from __future__ import absolute_import
import base64
import collections as col
import logging
import numbers
import os
import random
import sys
import uuid
from abc import ABCMeta, abstractmethod, abstractproperty
from binascii import hexlify, crc32
from collections import namedtuple
from datetime import datetime
from functools import partial
from hashlib import sha512  # pylint:disable=E0611
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from . import limits
from .abstractions import IJSONable, IPrintable
from .crypto import (
    Fingerprint, Cryptographer, AES_BLOCK_SIZE, co_crc32, crc32 as eager_crc32)
from .itertools_ex import ilen, only_one_of, repr_long_sized_iterable
from .path_ex import decode_posix_path, encode_posix_path
from .typed_uuids import ChunkUUID, FileUUID, PeerUUID
from .utils import (
    antisymmetric_comparison, shorten_uuid, size_code_to_size,
    strptime, strftime, coalesce, gen_uuid, fnmatchany, round_up_to_multiply)



#
# Constants
#

logger = logging.getLogger(__name__)
logger_fs_problem = logging.getLogger('status.fs_problem')

# A mapping from the internal builtin filter name
# to the set of appropriate wildcards.
FILTERS_TO_WILDCARDS = {
    'all':   frozenset(['*']),
    'video': frozenset('*.{}'.format(ext)
                           for ext in ('avi', 'mkv', 'mov', 'm4v', 'mpg',
                                       'mp4', 'flv', 'divx')),
    'audio': frozenset('*.{}'.format(ext)
                           for ext in ('aac', 'ac3', 'ape', 'mp3', 'ogg',
                                       'flac', 'wav')),
    'img':   frozenset('*.{}'.format(ext)
                           for ext in ('bmp', 'gif', 'jpg', 'jpeg', 'psd')),
    'doc':   frozenset('*.{}'.format(ext)
                           for ext in ('doc', 'docx', 'xls', 'xlsx', 'ppt',
                                       'pdf', 'odt')),
}



#
# Classes
#

@antisymmetric_comparison
class File(IPrintable):
    """
    A single file in the chunk stream. It is probably unbound
    to the actual file on the file system.

    @ivar uuid: UUID for this chunk; may be None if it is undefined.
                This can be defined only on the node, as it should
                become globally unique for all the hosts on this node!
    @type uuid: FileUUID, NoneType

    # Non-database fields:

    @ivar chunks: Either the list of chunks storing this file
                  (may be empty if the file is empty itself),
                  or None (if the file is not yet mapped to chunks).
    @type chunks: list, NoneType
    """
    __slots__ = ('crc32', 'uuid', 'fingerprint',
                 # Non-database
                 'chunks')


    @contract_epydoc
    def __init__(self, fingerprint, crc32, uuid=None, chunks=None):
        r"""Constructor.

        >>> # Smoke test
        >>> File(fingerprint=Fingerprint(123, 0xb6342L),
        ...      crc32=0x3a1d2b40)  # doctest:+ELLIPSIS
        File(000000000000007b00000...0000000000000000000000000b6342: 123 bytes)

        @param crc32: the CRC32 of the file.
            May be C{None} if the file is considered deleted.
        @type crc32: NoneType, int
        @precondition: crc32 is None or 0 <= crc32 <= 0xFFFFFFFF

        @param fingerprint: the fingerprint of the file.
            May be C{None} if the file is considered deleted.
        @type fingerprint: NoneType, Fingerprint

        @type uuid: NoneType, UUID
        """
        self.crc32 = crc32

        self.fingerprint = fingerprint
        if uuid is None:
            self.uuid = uuid
        else:
            self.uuid = FileUUID.safe_cast_uuid(uuid)
        self.chunks = chunks


    def __str__(self):
        # What if the len is not calculable (such as, "deleted file")?
        try:
            len_ = len(self)
        except TypeError:
            len_ = None

        if self.fingerprint is None:
            return u'{} bytes'.format(len_)
        else:
            return u'{}: {} bytes'.format(self.fingerprint, len_)


    @property
    def size(self):
        """
        @return: The file size, in fact calculated from the fingerprint.
                 Also can be used via len(file) call.
                 If the file is in "deleted" state, returns None.
        @rtype: numbers.Integral
        """
        return None if self.fingerprint is None \
                    else self.fingerprint.size


    def __len__(self):
        """
        @return: The file size, in fact calculated from the fingerprint.
                 Also can be used via file.size access.
        @rtype: numbers.Integral
        """
        return self.size


    def __eq__(self, other):
        r"""
        >>> # f1 basically matches f2, but not f3

        >>> f1 = File(fingerprint=Fingerprint(123, 0xb6342L),
        ...           crc32=0x3a1d2b40)
        >>> f1u1 = File(fingerprint=Fingerprint(123, 0xb6342L),
        ...             crc32=0x3a1d2b40,
        ...             uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> f1u2 = File(fingerprint=Fingerprint(123, 0xb6342L),
        ...             crc32=0x3a1d2b40,
        ...             uuid=UUID('af386d02-f944-400d-bd13-396d4379f84a'))

        >>> f2 = File(fingerprint=Fingerprint(123, 0xb6342L),
        ...           crc32=0x3a1d2b40)
        >>> f2u1 = File(fingerprint=Fingerprint(123, 0xb6342L),
        ...             crc32=0x3a1d2b40,
        ...             uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> f2u2 = File(fingerprint=Fingerprint(123, 0xb6342L),
        ...             crc32=0x3a1d2b40,
        ...             uuid=UUID('af386d02-f944-400d-bd13-396d4379f84a'))

        >>> f3 = File(fingerprint=Fingerprint(234, 0xe618eL),
        ...           crc32=0xa832764a)
        >>> f3u1 = File(fingerprint=Fingerprint(234, 0xe618eL),
        ...             crc32=0xa832764a,
        ...             uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> f3u2 = File(fingerprint=Fingerprint(234, 0xe618eL),
        ...             crc32=0xa832764a,
        ...             uuid=UUID('af386d02-f944-400d-bd13-396d4379f84a'))

        >>> # Basic comparison (also, for non-equality)
        >>> f1 == f2, f1 == f3, f1 != f2, f1 != f3
        (True, False, False, True)

        >>> # Compare with apples
        >>> f1 == 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42
        >>> f1 != 42
        Traceback (most recent call last):
          ...
        TypeError: other is 42

        >>> # What if an UUID on one of the sides is non-empty?
        >>> (f1 == f1u1, f1u1 == f1, f1 == f2u1,
        ...  f2u1 == f1, f1 == f3u1, f3u1 == f1)
        (True, True, True, True, False, False)
        >>> # ... and for non-equality
        >>> (f1 != f1u1, f1u1 != f1, f1 != f2u1,
        ...  f2u1 != f1, f1 != f3u1, f3u1 != f1)
        (False, False, False, False, True, True)

        >>> # What if both UUIDs are non-empty?
        >>> f1u1 == f2u1, f1u1 == f3u2, f1u1 != f2u1, f1u1 != f3u2
        (True, False, False, True)

        >>> # And now for something completely different...
        >>> # fp match, uuids don't match
        >>> f1u1 == f1u2  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError:
            (FileUUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'),
                 <Fingerprint: 00...000b6342, 123 bytes>,
             FileUUID('af386d02-f944-400d-bd13-396d4379f84a'),
                 <Fingerprint: 00...000b6342, 123 bytes>)
        >>> f1u1 != f1u2  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError:
            (FileUUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'),
                 <Fingerprint: 00...000b6342, 123 bytes>,
             FileUUID('af386d02-f944-400d-bd13-396d4379f84a'),
                 <Fingerprint: 00...000b6342, 123 bytes>)

        >>> # fp don't match, uuids match
        >>> f1u1 == f3u1  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError:
            (FileUUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'),
                 <Fingerprint: 00...000b6342, 123 bytes>,
             FileUUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'),
                 <Fingerprint: 00...000e618e, 234 bytes>)
        >>> f1u1 != f3u1  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError:
            (FileUUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'),
                 <Fingerprint: 00...000b6342, 123 bytes>,
             FileUUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'),
                 <Fingerprint: 00...000e618e, 234 bytes>)
        """
        if isinstance(other, File):
            # It UUIDs of the both files are non-empty,
            # the fingerprints must match when and only when UUIDs match.
            assert (self.uuid is None) or \
                   (other.uuid is None) or \
                   ((self.uuid == other.uuid) ==
                    (self.fingerprint == other.fingerprint)), \
                   (self.uuid, self.fingerprint, other.uuid, other.fingerprint)
            return self.fingerprint == other.fingerprint
        else:
            raise TypeError(u'other is {!r}'.format(other))


    def __hash__(self):
        return None if self.uuid is None \
                    else hash(self.uuid.hex)



class LocalFile(File, IJSONable):
    """The file, with known location on the file system.

    @ivar base_dir: The base directory, so that the .rel_path path
                    is relative to it.
    @type base_dir: basestring

    @ivar rel_path: Relative path (filename) of the file in the file system,
                    comparing to the base directory of the backup.
    @type rel_path: basestring
    """
    __slots__ = ('base_dir', 'rel_path')


    @contract_epydoc
    def __init__(self, rel_path, base_dir=None, *args, **kwargs):
        r"""Constructor.

        >>> # Smoke test 1
        >>> LocalFile(
        ...     # LocalFile-specific
        ...     rel_path='abc/def', base_dir='/home/john',
        ...     # File-specific
        ...     fingerprint=None, crc32=None, uuid=None
        ... )
        LocalFile(None bytes, rel_path='abc/def', base_dir='/home/john')
        >>> # Smoke test 2
        >>> LocalFile(
        ...     # LocalFile-specific
        ...     rel_path='abc/def', base_dir='/home/john',
        ...     # File-specific
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('1ab2d6bc-2d6a-4079-a585-76bfa450b893')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        LocalFile(000000000000007b00000...00000000000000000000b6342: 123 bytes,
                  rel_path='abc/def', base_dir='/home/john')

        @type rel_path: basestring
        @type base_dir: basestring
        """
        super(LocalFile, self).__init__(*args, **kwargs)

        self.rel_path = rel_path
        self.base_dir = base_dir


    @property
    def __unique_data(self):
        return (self.base_dir, self.rel_path)


    def __eq__(self, other):
        r"""
        >>> # f1 basically matches f2, but not f3

        >>> f1u1 = File(
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l1f1u1 = LocalFile(
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l2f1u1 = LocalFile(
        ...     rel_path='d1/f1', base_dir='/home/alex',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l3f1u1 = LocalFile(
        ...     rel_path='d1/f2', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))

        >>> f2u1 = File(
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l1f2u1 = LocalFile(
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l2f2u1 = LocalFile(
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))

        >>> f3u1 = File(
        ...     fingerprint=Fingerprint(234, 0xe618eL), crc32=0xa832764a,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l1f3u1 = LocalFile(
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(234, 0xe618eL), crc32=0xa832764a,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> f3u2 = File(
        ...     fingerprint=Fingerprint(234, 0xe618eL), crc32=0xa832764a,
        ...     uuid=UUID('af386d02-f944-400d-bd13-396d4379f84a'))

        >>> # Now, comparisons.
        >>> l1f1u1 == l2f1u1, l1f1u1 == l3f1u1, l1f1u1 == l1f2u1
        (False, False, True)
        >>> l1f1u1 != l2f1u1, l1f1u1 != l3f1u1, l1f1u1 != l1f2u1
        (True, True, False)
        >>> # Compare with the base classes
        >>> l1f1u1 == f1u1, l1f1u1 == f3u2, f1u1 == l1f1u1, f3u2 == l1f1u1
        (True, False, True, False)
        >>> l1f1u1 != f1u1, l1f1u1 != f3u2, f1u1 != l1f1u1, f3u2 != l1f1u1
        (False, True, False, True)
        """
        if isinstance(other, LocalFile):
            # If the LocalFile data matches, the File data must match as well.
            # Todo: is it right? Maybe not, maybe the local states
            # can be different. But for now, this doesn't occur.
            assert (self.__unique_data != other.__unique_data or
                    super(LocalFile, self).__eq__(other)), \
                    (self, other)
            return self.__unique_data == other.__unique_data
        else:
            return super(LocalFile, self).__eq__(other)


    def __hash__(self):
        return hash((super(LocalFile, self).__hash__(),
                     self.base_dir,
                     self.rel_path))


    @property
    def rel_dir(self):
        return os.path.dirname(self.rel_path)


    @property
    def rel_file(self):
        return os.path.basename(self.rel_path)


    def __str__(self):
        return u'{super}, rel_path={self.rel_path!r}{opt_base_dir}'.format(
                   super=super(LocalFile, self).__str__(),
                   self=self,
                   opt_base_dir=u', base_dir={self.base_dir!r}'
                                        .format(self=self)
                                    if self.base_dir is not None
                                    else '')


    def to_json(self):
        r"""Implement C{IJSONable} interface.

        >>> # Smoke test 1
        >>> LocalFile(
        ...     # LocalFile-specific
        ...     rel_path='abc/def', base_dir='/home/john',
        ...     # File-specific
        ...     fingerprint=None, crc32=None, uuid=None
        ... ).to_json()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'rel_path': 'abc/def', 'base_dir': '/home/john'}
        >>> # Smoke test 2
        >>> LocalFile(
        ...     # LocalFile-specific
        ...     rel_path='abc/def', base_dir='/home/john',
        ...     # File-specific
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('1ab2d6bc-2d6a-4079-a585-76bfa450b893')
        ... ).to_json()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'crc32': 974990144, 'uuid': '1ab2d6bc2d6a4079a58576bfa450b893',
         'fingerprint': '000000000000007b000...000000000000000000000000b6342',
         'rel_path': 'abc/def', 'base_dir': '/home/john'}
        """
        res = {}

        # All three of these must be "None" or "not None" simultaneously.
        assert ((self.fingerprint is None) ==
                (self.crc32 is None) ==
                (self.uuid is None)), \
               (self.fingerprint, self.crc32, self.uuid)

        # Optional
        if self.base_dir is not None:
            res['base_dir'] = encode_posix_path(self.base_dir)

        if self.rel_path is not None:
            res['rel_path'] = encode_posix_path(self.rel_path)

        if self.fingerprint is not None:
            res['fingerprint'] = str(self.fingerprint)

        if self.crc32 is not None:
            res['crc32'] = self.crc32

        if self.uuid is not None:
            res['uuid'] = self.uuid.hex

        return res


    @classmethod
    def from_json(cls, data):
        r"""Implement C{IJSONable} interface.

        >>> # Smoke test 1
        >>> LocalFile.from_json({
        ...     'rel_path': 'abc/def', 'attrs': '', 'base_dir': '/home/john'
        ... })()
        LocalFile(None bytes, rel_path='abc/def', base_dir='/home/john')

        >>> # Smoke test 2
        >>> LocalFile.from_json({
        ...     'uuid': '1ab2d6bc2d6a4079a58576bfa450b893',
        ...     'rel_path': 'abc/def',
        ...     'fingerprint': '000000000000007b0000000000000000'
        ...                    '00000000000000000000000000000000'
        ...                    '00000000000000000000000000000000'
        ...                    '00000000000000000000000000000000'
        ...                    '00000000000b6342',
        ...     'crc32': 974990144, 'base_dir': '/home/john', 'attrs': ''
        ... })()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        LocalFile(000000000000007b000...0000000000000000000000b6342: 123 bytes,
                  rel_path='abc/def', base_dir='/home/john')
        """
        _raw_crc32 = data.get('crc32', None)
        _raw_fp = data.get('fingerprint', None)
        _raw_uuid = data.get('uuid', None)
        return partial(super(LocalFile, cls).from_json(data),
                       rel_path=decode_posix_path(data['rel_path']),
                       crc32=_raw_crc32,
                       fingerprint=None if _raw_fp is None
                                        else Fingerprint.from_string(_raw_fp),
                       uuid=None if _raw_uuid is None or not _raw_uuid
                                 else FileUUID(_raw_uuid),
                       base_dir=decode_posix_path(data['base_dir']))


    @property
    def full_path(self):
        """The full path to the file.

        @return: The full path to the file. It is required that
                 the .base_dir field is not None.
        @rtype: basestring
        """
        assert self.base_dir is not None
        return os.path.join(self.base_dir, self.rel_path)



class LocalFileState(LocalFile):
    """The particular state of a file, with known last change time.

    May read the file.
    """
    __slots__ = ('isdir', 'time_changed', 'attrs')

    __metaclass__ = ABCMeta


    @contract_epydoc
    def __init__(self, isdir=False, time_changed=None, attrs='',
                 *args, **kwargs):
        r"""Constructor.

        >>> class C(LocalFileState):
        ...     def open_file(self): pass

        >>> # Smoke test 1
        >>> C(
        ...     # LocalFile-specific
        ...     rel_path='abc/def',
        ...     # File-specific
        ...     fingerprint=None, crc32=None, uuid=None
        ... )
        C(None bytes, rel_path='abc/def')
        >>> # Smoke test 2
        >>> C(
        ...     # LocalFile-specific
        ...     rel_path='abc/def',
        ...     # File-specific
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('1ab2d6bc-2d6a-4079-a585-76bfa450b893')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        C(000000000000007b00000...00000000000000000000b6342: 123 bytes,
          rel_path='abc/def')

        @param isdir: whether this is a directory (rather than a regular file).
            Note: can B{NOT} be C{None} (precisely this value goes into
            the RDB)!
            If it is C{True}, this B{definitely} means this is
            an added directory.
            If it is C{False}, this means it is either an added file,
            or maybe a deleted path (which may be a directory as well)
        @type isdir: bool

        @param time_changed: the timestamp of the file change, or C{None} if
            the particular state is not known/relevant.
        @type time_changed: datetime, NoneType

        @todo: Fix this tests because of new fields ("isdir" and "attrs").
        """
        super(LocalFileState, self).__init__(*args, **kwargs)
        self.isdir = isdir
        self.time_changed = time_changed
        self.attrs = attrs


    def __str__(self):
        return u'{super}{opt_isdir}{opt_time_changed}{opt_attrs}'.format(
                   super=super(LocalFileState, self).__str__(),
                   opt_isdir=', isdir={!r}'.format(
                                    self.isdir)
                                 # defaults
                                 if self.isdir != False
                                 else '',
                   opt_time_changed=', time_changed={!r}'.format(
                                            self.time_changed)
                                        # defaults
                                        if self.time_changed is not None
                                        else '',
                   opt_attrs=', attrs={!r}'.format(
                                    self.attrs)
                                 # defaults
                                 if self.attrs != ''
                                 else '')


    def __eq__(self, other):
        r"""
        >>> class C(LocalFileState):
        ...     def open_file(self): pass

        >>> l2f1u1 = LocalFile(
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l2f1u1s1 = C(
        ...     time_changed=datetime(2012, 12, 27),
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l2f1u1s1b = C(
        ...     time_changed=datetime(2012, 12, 27),
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l2f1u1s2 = C(
        ...     time_changed=datetime(2012, 12, 28),
        ...     rel_path='d1/f1', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))

        >>> l3f1u1s1 = C(
        ...     time_changed=datetime(2012, 12, 27),
        ...     rel_path='d1/f2', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))
        >>> l3f1u1s2 = C(
        ...     time_changed=datetime(2012, 12, 28),
        ...     rel_path='d1/f2', base_dir='/home/john',
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('6d56867f-57d3-4dc1-ae1b-86e616baabf4'))

        # Comparisons
        >>> l2f1u1s1 == l2f1u1s1 == l2f1u1s1b, l2f1u1s1 == l2f1u1s2
        (True, False)
        >>> l2f1u1s1 == l3f1u1s1, l2f1u1s1 == l3f1u1s2
        (False, False)

        # Negative
        >>> l2f1u1s1 != l2f1u1s1b, l2f1u1s1 != l2f1u1s2
        (False, True)
        >>> l2f1u1s1 != l3f1u1s1, l2f1u1s1 != l3f1u1s2
        (True, True)

        # With super classes
        >>> l2f1u1s1 == l2f1u1, l2f1u1s2 == l2f1u1, l3f1u1s1 == l2f1u1
        (True, True, False)
        >>> l2f1u1s1 != l2f1u1, l2f1u1s2 != l2f1u1, l3f1u1s1 != l2f1u1
        (False, False, True)

        # ... and opposite direction
        >>> l2f1u1 == l2f1u1s1, l2f1u1 == l2f1u1s2, l2f1u1 == l3f1u1s1
        (True, True, False)
        >>> l2f1u1 != l2f1u1s1, l2f1u1 != l2f1u1s2, l2f1u1 != l3f1u1s1
        (False, False, True)

        @todo: Fix this tests because of new fields ("isdir" and "attrs").
        """
        if isinstance(other, LocalFileState):
            return (super(LocalFileState, self).__eq__(other) and
                    self.isdir == other.isdir and
                    self.time_changed == other.time_changed and
                    self.attrs == other.attrs)
        else:
            return super(LocalFileState, self).__eq__(other)


    def __hash__(self):
        return hash((super(LocalFileState, self).__hash__(),
                     self.isdir, self.time_changed, self.attrs))


    def to_json(self):
        r"""Implement C{IJSONable} interface.

        >>> class C(LocalFileState):
        ...     def open_file(self): pass

        >>> # Smoke test 1
        >>> C(
        ...     # LocalFile-specific
        ...     rel_path='abc/def',
        ...     # File-specific
        ...     fingerprint=None, crc32=None, uuid=None
        ... ).to_json()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'rel_path': 'abc/def', 'attrs': ''}
        >>> # Smoke test 2
        >>> C(
        ...     # LocalFile-specific
        ...     rel_path='abc/def',
        ...     # File-specific
        ...     fingerprint=Fingerprint(123, 0xb6342L), crc32=0x3a1d2b40,
        ...     uuid=UUID('1ab2d6bc-2d6a-4079-a585-76bfa450b893')
        ... ).to_json()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'crc32': 974990144, 'uuid': '1ab2d6bc2d6a4079a58576bfa450b893',
         'attrs': '', 'rel_path': 'abc/def',
         'fingerprint': '000000000000007b0000...00000000000000000000b6342'}

        @todo: Fix this tests because of new fields ("isdir" and "attrs").
        """
        res = super(LocalFileState, self).to_json()
        # Optional
        if self.isdir is not False:  # non-default
            res['isdir'] = self.isdir
        if self.time_changed is not None:
            res['time_changed'] = strftime(self.time_changed)
        if self.attrs is not None:
            res['attrs'] = self.attrs
        return res


    @classmethod
    def from_json(cls, data):
        """Implement C{IJSONable} interface.

        >>> class C(LocalFileState):
        ...     def open_file(self): pass

        >>> # Smoke test 1
        >>> C.from_json({
        ...     'rel_path': 'abc/def', 'base_dir': '/home/john'
        ... })()
        C(None bytes, rel_path='abc/def', base_dir='/home/john')

        >>> # Smoke test 2
        >>> C.from_json({
        ...     'uuid': '1ab2d6bc2d6a4079a58576bfa450b893',
        ...     'rel_path': 'abc/def',
        ...     'fingerprint': '000000000000007b0000000000000000'
        ...                    '00000000000000000000000000000000'
        ...                    '00000000000000000000000000000000'
        ...                    '00000000000000000000000000000000'
        ...                    '00000000000b6342',
        ...     'crc32': 974990144, 'base_dir': '/home/john', 'attrs': ''
        ... })()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        C(000000000000007b000...0000000000000000000000b6342: 123 bytes,
                  rel_path='abc/def', base_dir='/home/john')

        @rtype: LocalFileState

        @todo: Fix this tests because of new fields ("isdir" and "attrs").
        """
        return partial(super(LocalFileState, cls).from_json(data),
                       # LocalFileState-specific
                       isdir=data.get('isdir', False),
                       time_changed=strptime(data['time_changed'])
                                        if 'time_changed' in data
                                        else None,
                       attrs=data.get('attrs', ''))


    @abstractmethod
    def open_file(self):
        """Implement in any subclass!

        @return: the file handler opened for reading.
        """
        pass


    @property
    def full_path(self):
        """The full path to the file."""
        return os.path.join(self.base_dir, self.rel_path)



class LocalPhysicalFileState(LocalFileState):
    """
    The state of a file, with the file being able to be physically open.
    """

    __slots__ = ()


    def open_file(self):
        """Implement interface from C{LocalFileState}."""
        return open(self.full_path, 'rb')



class LocalVirtualFileState(LocalFileState):
    """
    The state of a file, with the file contents readable only implicitly.
    """

    __slots__ = ('file_getter',)


    def __init__(self, file_getter, *args, **kwargs):
        r"""Constructor.

        @param file_getter: a function that returns a file-like object
            opened only for reading (not necessarily writable!).
            Most likely, it is a variation of C{open()} function,
            with the bound C{path} argument, and C{mode='rb'}.
        @type file_getter: col.Callable

        @rtype: LocalVirtualFileState
        """
        super(LocalVirtualFileState, self).__init__(*args, **kwargs)
        self.file_getter = file_getter


    def open_file(self):
        """Implement interface from C{LocalFileState}."""
        return self.file_getter()



class LocalPhysicalFileStateRel(IPrintable):
    """
    A workaround class created to be similar to C{LocalPhysicalFileState},
    but when C{LocalPhysicalFileState} didn't fit perfectly
    to the aim.

    @todo: C{LocalFile} should be adapted to contain C{rel_dir} and C{rel_file}
        rather than the merged C{rel_path}.
        Also, C{LocalFile} should not be derived from C{File},
        but become an alternative class for multiple inheritance;
        that is, C{File} should contain UUID/Fingerprint while C{LocalFile}
        should contain the local FS attributes.
    @todo: ticket:141 - either this class or some C{LocalDirStateRel} must be
        able to support directories.
    """
    __slots__ = ('rel_dir', 'rel_file', 'size', 'time_changed', 'isdir')


    @contract_epydoc
    def __init__(self, rel_dir, rel_file, size, time_changed, isdir):
        r"""Constructor.

        >>> # Regular file
        >>> f = LocalPhysicalFileStateRel(
        ...         rel_dir=u'a/b', rel_file=u'bbb',
        ...         size=4,
        ...         time_changed=datetime(2012, 10, 11, 15, 33, 42, 19808),
        ...         isdir=False
        ...     )
        >>> f  # doctest:+NORMALIZE_WHITESPACE
        LocalPhysicalFileStateRel(rel_dir=u'a/b',
            rel_file=u'bbb',
            size=4,
            time_changed=datetime.datetime(2012, 10, 11, 15, 33, 42, 19808),
            isdir=False)
        >>> f.isdir, f.is_deleted
        (False, False)

        >>> # Directory
        >>> f = LocalPhysicalFileStateRel(
        ...         rel_dir=u'a/b', rel_file=u'bbb',
        ...         size=None,
        ...         time_changed=datetime(2012, 10, 11, 15, 33, 42, 19808),
        ...         isdir=True
        ...     )
        >>> f  # doctest:+NORMALIZE_WHITESPACE
        LocalPhysicalFileStateRel(rel_dir=u'a/b',
            rel_file=u'bbb',
            size=None,
            time_changed=datetime.datetime(2012, 10, 11, 15, 33, 42, 19808),
            isdir=True)
        >>> f.isdir, f.is_deleted
        (True, False)

        >>> # Deleted path
        >>> f = LocalPhysicalFileStateRel(
        ...         rel_dir=u'a/b', rel_file=u'bbb',
        ...         size=None,
        ...         time_changed=datetime(2012, 10, 11, 15, 33, 42, 19808),
        ...         isdir=False
        ...     )
        >>> f  # doctest:+NORMALIZE_WHITESPACE
        LocalPhysicalFileStateRel(rel_dir=u'a/b',
            rel_file=u'bbb',
            size=None,
            time_changed=datetime.datetime(2012, 10, 11, 15, 33, 42, 19808),
            isdir=False)
        >>> f.isdir, f.is_deleted
        (False, True)

        @type rel_dir: basestring
        @type rel_file: basestring

        @param size: the size of the file, or None if the file was deleted
            (C{isdir} must be C{False}).
        @type size: numbers.Integral, NoneType

        @type time_changed: datetime

        @todo: similarly to LocalPhysicalFileState, add C{isdir} field!
        """
        self.rel_dir = rel_dir
        self.rel_file = rel_file
        self.size = size
        self.time_changed = time_changed
        self.isdir = isdir


    def __str__(self):
        return u'rel_dir={self.rel_dir!r}, rel_file={self.rel_file!r}, ' \
               u'size={self.size!r}, time_changed={self.time_changed!r}, ' \
               u'isdir={self.isdir!r}'.format(self=self)


    @property
    def rel_path(self):
        r"""Return full relative path.

        >>> LocalPhysicalFileStateRel(
        ...     rel_dir=u'a/b', rel_file=u'bbb',
        ...     size=4,
        ...     time_changed=datetime(2012, 10, 11, 15, 33, 42, 19808),
        ...     isdir=False
        ... ).rel_path
        u'a/b/bbb'

        @rtype: basestring
        """
        return os.path.join(self.rel_dir, self.rel_file)


    @property
    def is_deleted(self):
        return not self.isdir and self.size is None



@antisymmetric_comparison
class Chunk(IPrintable, IJSONable):
    """A single data chunk.

    It may have any size (measured in MiB) being a power of 2, no more than 16.
    i.e. 1, 2, 4, 8, 16 mebibytes. (2^0 .. 2^4)

    @ivar uuid: UUID for this chunk.
    @type uuid: ChunkUUID

    @param crc32: the property containing the value of CRC32 from the contents.
    @type crc32: int

    @ivar _hash: The (SHA512) hash sum of the chunk.
                 May contain None if not yet calculated.
                 Use .hash property to access
                 (rather than the hash() function on the object!)
    @type _hash: (str, NoneType)

    @ivar _size: The size of the chunk. May be None if not yet calculated.
                 Use size() property to access.
                 There are two ways to calculate the size,
                 one is to calculate over the available blocks
                 and another is to use the value directly if available.
                 If both sizes are available, they MUST be equal.
    @type _size: (int, NoneType)

    @ivar blocks: The list of Block objects
    @type blocks: list<Block>
    """
    __slots__ = ('uuid', '_hash', '_size', 'maxsize_code',
                 # Non-database
                 'blocks')


    #
    # Subclasses
    #

    @antisymmetric_comparison
    class Block(IJSONable):
        """
        A block in the file, the minimum section comprising
        the chunks and the files.

        @ivar offset_in_file: May be None, if it is not known

        @todo: Make "chunk" mandatory
        """
        __slots__ = ('file', 'offset_in_file', 'offset_in_chunk',
                     'size', 'chunk')


        @contract_epydoc
        def __init__(self, file, offset_in_file, offset_in_chunk, size, chunk):
            """Constructor.

            @type file: File
            @type offset_in_file: numbers.Integral
            @type offset_in_chunk: numbers.Integral
            @type size: numbers.Integral
            @type chunk: Chunk
            """
            self.file = file
            self.offset_in_file = offset_in_file
            self.offset_in_chunk = offset_in_chunk
            self.size = size
            self.chunk = chunk


        def to_json(self, chunk, with_file=True):
            """
            @precondition: isinstance(self.file.fingerprint, Fingerprint)
            """
            result = {'chunk': chunk.to_json(),
                      'offset_in_file': self.offset_in_file,
                      'offset_in_chunk': self.offset_in_chunk,
                      'size': self.size}
            if with_file:
                result.update({'file': self.file.to_json()})

            return result


        @classmethod
        def from_json(cls, data, bind_to_file=None):
            """
            @postcondition: isinstance(result(), tuple)
            @postcondition: isinstance(result()[0], Chunk.Block) and \
                            isinstance(result()[1], ChunkInfo)

            @note: The files referred by the created Chunk.Block-s are dummy,
                   and the only valid data they contain
                   are the fingerprint and the local file name.
            @todo: Return only the Block object, but with bound Chunk.
                   Change all callers.
            """
            if bind_to_file is None:
                use_file = LocalFile.from_json(data['file'])()
            else:
                assert isinstance(bind_to_file, File), repr(bind_to_file)
                use_file = bind_to_file

            chunk = ChunkInfo.from_json(data['chunk'])()
            block = Chunk.Block(file=use_file,
                                offset_in_file=int(data['offset_in_file']),
                                offset_in_chunk=int(data['offset_in_chunk']),
                                size=int(data['size']),
                                chunk=chunk)
            return lambda: (block, chunk)


        def __repr__(self):
            return u'<Block: {!r}, #{:d}, {:d} bytes{:s}>' \
                       .format(self.file,
                               self.offset_in_chunk,
                               self.size,
                               u', chunk {}'.format(self.chunk.uuid)
                                   if self.chunk is not None
                                   else '')


        def __len__(self):
            """
            Alias to .size attribute.
            """
            return self.size


        @property
        def __unique_data(self):
            return (self.file,
                    self.offset_in_file,
                    self.offset_in_chunk,
                    self.size,
                    self.chunk)


        def __eq__(self, other):
            if isinstance(other, Chunk.Block):
                return self.__unique_data == other.__unique_data
            else:
                raise TypeError(u'other is {!r}'.format(other))


        def __hash__(self):
            return hash(self.__unique_data)


        @property
        def offset_after(self):
            """
            @return: The offset where the next block is assumed to be started.
            @rtype: numbers.Integral
            """
            return self.offset_in_chunk + self.size


        @contract_epydoc
        def get_bytes(self):
            """
            Read the bytes for this block and return it.
            If cannot be read, then the string of zeroes will be returned.

            @rtype: str
            @postcondition: len(result) == self.size
            """
            try:
                with self.file.open_file() as fh:
                    fh.seek(self.offset_in_file)
                    res = fh.read(self.size)
                    # What if the file size is less than we expected,
                    # and we cannot read it fully?
                    # Likely, the file was changed, and we need to pad it
                    # to the end to still try to create the Block/Chunk.
                    _len_res = len(res)
                    pad_size = self.size - _len_res
                    assert pad_size >= 0, \
                           (self.size, len(res), res)
                    if pad_size:
                        logger_fs_problem.warning(
                            'While trying to read the file %r from #%i, '
                                '%i bytes were expected but only %i bytes '
                                'were read; padding with %i zero bytes.',
                            self.file,
                            self.offset_in_file,
                            self.size,
                            _len_res,
                            pad_size,
                            extra={'path': self.file.full_path,
                                   '_type': 'file resized',
                                   'exc': None})

                        res += '\x00' * pad_size
            except Exception as e:
                logger_fs_problem.warning(
                    'Cannot read the file %r (%i:%i): %r',
                    self.file, self.offset_in_file, self.size, e,
                    extra={'path': self.file.full_path,
                           '_type': 'read file',
                           'exc': e})
                res = '\x00' * self.size

            return res


    __metaclass__ = ABCMeta


    #
    # Methods
    #

    @contract_epydoc
    def __init__(self,
                 maxsize_code=None,
                 uuid=None, hash=None, size=None, blocks=None,
                 *args, **kwargs):
        """Constructor.

        @param maxsize_code: Maximum chunk size (a power of 2)
        @type maxsize_code: NoneType, int

        @type uuid: UUID, NoneType

        @type hash: (str, buffer, NoneType)
        @precondition: hash is None or len(hash) == 64 # hash

        @type size: numbers.Integral, NoneType

        @type blocks: NoneType, list
        @precondition: blocks is None or consists_of(blocks, Chunk.Block)

        @invariant: 0 <= self.size() <= self.max_size
        """
        super(Chunk, self).__init__(*args, **kwargs)

        if uuid is not None:
            _u = uuid
        else:
            _u = gen_uuid()

        self.uuid = ChunkUUID.safe_cast_uuid(_u)

        self._hash = hash
        self._size = size

        assert maxsize_code is None or\
               0 <= maxsize_code <= limits.MAX_CHUNK_SIZE_CODE, \
               maxsize_code
        self.maxsize_code = maxsize_code
        self.blocks = coalesce(blocks, [])


    def __str__(self):
        return u'uuid={self.uuid!r}' \
               u'{opt_maxsize_code}{opt_hash}{opt_size}{opt_blocks}' \
                   .format(self=self,
                           opt_maxsize_code=
                               '' if self.maxsize_code is None
                                  else u', maxsize_code={!r}'.format(
                                           self.maxsize_code),
                           opt_hash='' if self._hash is None
                                       else u', hash=unhexlify({!r})'.format(
                                                hexlify(self._hash)),
                           opt_size='' if self._size is None
                                       else u', size={!r}'.format(self._size),
                           opt_blocks='' if not self.blocks
                                         else u', blocks={!s}'.format(
                                             repr_long_sized_iterable(
                                                 self.blocks)))

    @abstractproperty
    def crc32(self):
        """
        @rtype: numbers.Integral
        @postcondition: 0 <= result <= 0xFFFFFFFF
        """
        pass


    def to_json(self):
        result = super(Chunk, self).to_json()
        result.update({
            'uuid': self.uuid.hex,
            'hash': base64.b64encode(self.hash),
            'size': self.size(),
            'crc32': self.crc32
        })
        if self.maxsize_code is not None:
            result['maxsize_code'] = self.maxsize_code

        return result


    @classmethod
    def from_json(cls, data):
        return partial(super(Chunk, cls).from_json(data),
                       maxsize_code=int(data['maxsize_code'])
                                        if 'maxsize_code' in data
                                        else None,
                       uuid=ChunkUUID(data['uuid']),
                       hash=base64.b64decode(data['hash']),
                       size=int(data['size']))


    def __eq__(self, other):
        if isinstance(other, Chunk):
            return self.uuid == other.uuid
        else:
            raise TypeError(u'other is {!r}'.format(other))


    def __hash__(self):
        return hash(self.uuid.int)


    def _hash_read(self):
        """
        Read the pre-calculated/pre-written hash sum of the chunk
        and return it.
        Do not access this method directly, but use the public .hash property.

        May be overridden if a custom hash calculation method is needed.

        @rtype: str
        @postcondition: len(result) == 64
        """
        assert self._hash is not None and len(self._hash) == 64, \
               repr(self._hash)
        return self._hash

    def _hash_write(self, value):
        """
        Property setter.
        Do not access this method directly, but use the public .hash property.

        May be overridden if a custom hash calculation method is needed.
        """
        self._hash = value

    hash = property(_hash_read, _hash_write)


    def __getitem__(self, key):
        """
        Index over chunk blocks in this chunk.
        @rtype: Chunk.Block
        """
        return self.blocks[key]


    @classmethod
    def max_chunk_size_code_for_total_size(cls, total_size):
        """
        @param total_size: The total dataset size,
                           for which to calculate the chunk size.
        @type total_size: numbers.Integral

        @return: The maxsize_code (see definition in specification)
                 for the given total dataset size.
        @rtype: numbers.Integral

        >>> f = Chunk.max_chunk_size_code_for_total_size
        >>> M = 1024*1024
        >>> 0 == f(0) == f(15) == f(1*M) == f(8*M - 1)
        True
        >>> 1 == f(8*M) == f(15*M) == f(32*M - 1)
        True
        >>> 2 == f(32*M) == f(100*M) == f(128*M - 1)
        True
        >>> 3 == f(128*M) == f(400*M) == f(512*M - 1)
        True
        >>> 4 == f(512*M) == f(1600*M) == f(2048*M - 1)
        True
        >>> 5 == f(2048*M) == f(6400*M) == f(8192*M - 1)
        True
        >>> 6 == f(8192*M) == f(25*M*M) == f(32*M*M) == f(32*M*M*M*M*M)
        True
        """
        # Please no bit mashing here
        if total_size < 0x800000:       # 8 MiB
            return 0  # 1 MiB
        elif total_size < 0x2000000:    # 32 MiB
            return 1  # 2 MiB
        elif total_size < 0x8000000:    # 128 MiB
            return 2  # 4 MiB
        elif total_size < 0x20000000:   # 512 MiB
            return 3  # 8 MiB
        elif total_size < 0x80000000:   # 2 GiB
            return 4  # 16 MiB
        elif total_size < 0x200000000:  # 8 GiB
            return 5  # 32 MiB
        else:
            return 6  # 64 MiB


    @property
    def max_size(self):
        max_size = size_code_to_size(self.maxsize_code)
        assert max_size in limits.SUPPORTED_CHUNK_SIZES
        return max_size


    def __size_from_blocks(self):
        assert self.blocks is not None, repr(self.blocks)
        return max(block.offset_after
                       for block in self.blocks) if self.blocks \
                                                 else 0


    def size(self):
        """
        This method forcibly overrides the according method
        in the Chunk object, as, with the encryption enabled,
        the size is given manually.
        """
        # If blocks are present, the size should be calculated over them.
        if self._size is None:
            return self.__size_from_blocks()
        else:
            # if __debug__:
            #     size_from_blocks = self.__size_from_blocks()
            #     assert size_from_blocks == self._size, \
            #            (size_from_blocks, self._size)

            return self._size



class ChunkInfo(Chunk):
    """
    An object containing all the information about the chunk,
    but not the chunk contents. I.e. it is the object used
    in the DB operations, message passing, etc.
    """

    __slots__ = ('__crc32',)


    @contract_epydoc
    def __init__(self, crc32, *args, **kwargs):
        """Constructor.

        >>> ChunkInfo(crc32=0x734B8921D,
        ...           uuid=ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
        ...           maxsize_code=1,
        ...           hash='abcdefgh' * 8,
        ...           size=2097152)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ChunkInfo(uuid=ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
            maxsize_code=1,
            hash=unhexlify('61626364656667686162...4656667686162636465666768'),
            size=2097152, crc32=0x734B8921D)

        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF
        """
        assert 'uuid' in kwargs, repr(kwargs)
        assert 'hash' in kwargs, repr(kwargs)
        assert 'size' in kwargs, repr(kwargs)
        assert 'blocks' not in kwargs, repr(kwargs)

        super(ChunkInfo, self).__init__(*args, **kwargs)
        self.__crc32 = crc32


    def __str__(self):
        return u'{super}, crc32=0x{self.crc32:08X}'.format(
                   super=super(ChunkInfo, self).__str__(),
                   self=self)


    @property
    @contract_epydoc
    def crc32(self):
        """
        @rtype: numbers.Integral
        @postcondition: 0 <= result <= 0xFFFFFFFF
        """
        return self.__crc32


    @classmethod
    def from_json(cls, data):
        return partial(super(ChunkInfo, cls).from_json(data),
                       crc32=int(data['crc32']))



class AbstractChunkWithContents(Chunk):
    """
    A Chunk object which has a contents available via C{.get_bytes()} method.

    @note: The method .get_bytes() should be overridden
           in any practical implementation.
    """

    @abstractmethod
    def get_bytes(self):
        """Override this method!

        @rtype: str
        """
        pass


    @abstractmethod
    def phys_size(self):
        """
        The physical size of the message may be different from the real one,
        so override it if needed.
        """
        raise NotImplementedError()


    @property
    def hash_from_body(self):
        """Calculate the hash sum of the chunk upon the contents.

        @note: contrary to C{.hash}, this property is always recalculated
            from the body value and always matches it.
        """
        return sha512(self.get_bytes()).digest()



class ChunkFromContents(AbstractChunkWithContents):
    """
    The Chunk which is created with all of its contents known,
    most likely on an incoming CHUNKS message.
    """
    __slots__ = ('__bytes', '__crc32')


    @contract_epydoc
    def __init__(self, bytes, crc32, *args, **kwargs):
        """Constructor.

        >>> ch = ChunkFromContents('abcdefghi', crc32=12345, hash=' '*64)

        >>> len(ch.hash) == len(ch.hash_from_body) == 64
        True

        >>> hexlify(ch.hash) # doctest:+ELLIPSIS
        '20202020...20202020'
        >>> hexlify(ch.hash_from_body) # doctest:+ELLIPSIS
        'f22d51d25292ca1d0f68f69aedc789...93ce98dc9b833db7839247b1d9c24fe'

        >>> ch.hash == sha512('abcdefghi').digest()
        False
        >>> ch.hash_from_body == sha512('abcdefghi').digest()
        True

        >>> ch.crc32
        12345
        >>> eager_crc32('abcdefghi')  # different from .crc32
        2376698031

        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF
        """
        super(ChunkFromContents, self).__init__(*args, **kwargs)
        self.__bytes = bytes
        self.__crc32 = crc32


    def get_bytes(self):
        """
        >>> ch = ChunkFromContents('abcdefghi', crc32=123456)
        >>> ch.get_bytes()
        'abcdefghi'
        """
        return self.__bytes


    def phys_size(self):
        """
        >>> ch = ChunkFromContents('abcdefghi', crc32=123456)
        >>> ch.phys_size()
        9
        """
        return len(self.__bytes)


    @property
    @contract_epydoc
    def crc32(self):
        """
        >>> ch = ChunkFromContents('abcdefghi', crc32=123456)
        >>> ch.crc32
        123456

        @rtype: numbers.Integral
        @postcondition: 0 <= result <= 0xFFFFFFFF
        """
        return self.__crc32



class ChunkFromFiles(AbstractChunkWithContents):
    """
    A Chunk which is being constructed from the original files,
    and whose internal structure is still not finalized and being created.

    @ivar __crc32: the cache of the CRC32 from the chunk contents.
    """
    __slots__ = ('__crc32',)


    def __init__(self, *args, **kwargs):
        assert 'maxsize_code' in kwargs, \
               (args, kwargs)
        super(ChunkFromFiles, self).__init__(*args, **kwargs)
        self.__crc32 = None


    @contract_epydoc
    def append_file(self, offset_in_file, how_many, file):
        """
        Append C{how_many} bytes from the C{file} file
        (from C{offset_in_file} offset) to the chunk.

        @param offset_in_file: How many bytes from the file to append
            to the chunk.
            Note: empty blocks should never be added,
            even if the file is empty in fact!
        @type offset_in_file: numbers.Integral
        @precondition: 0 <= offset_in_file < file.size

        @param how_many: How many bytes from the file to append to the chunk.
                         Note: empty blocks should never be added,
                         even if the file is empty in fact!
        @type how_many: numbers.Integral
        @precondition: how_many > 0

        @precondition: 0 <= offset_in_file+how_many <= file.size
                       # offset_in_file, how_many, file
        @precondition: how_many <= self.free_space # how_many,

        @param file: The file object to append
        @type file: File
        @precondition: file not in (bl.file for bl in self.blocks)
                       # repr(file), repr(self)
        """
        # Done with the verifications
        self.blocks.append(Chunk.Block(file=file,
                                       offset_in_file=offset_in_file,
                                       offset_in_chunk=self.free_offset,
                                       size=how_many,
                                       chunk=self))


    @property
    def free_offset(self):
        """Get the offset where the next block can be added.

        @return: The offset where the new data can be put.
                 May be equal to the max size of the chunk,
                 what means the chunk is full.
        @rtype: numbers.Integral
        """
        return self.blocks[-1].offset_after if self.blocks \
                                            else 0


    @property
    def free_space(self):
        """Get the amount of remaining space in the chunk.

        @return: The free space (in bytes) at the end of the chunk.
        @rtype: numbers.Integral
        """
        return self.max_size - self.free_offset


    def __get_bytes(self):
        """
        Reread the byte contents of the chunk. Also, fill the C{.crc32}
        field to cache the value.
        """
        cur_offset_in_chunk = 0

        # Initialize the coroutine-style CRC32 calculator
        crc32_gen = co_crc32()
        crc32_gen.next()

        # It is probably wrong to re-read the body
        # assert self.__crc32 is None, repr(self)
        old_crc32 = self.__crc32

        for block in self.blocks:
            if block.offset_in_chunk == cur_offset_in_chunk:
                # Next block is at the proper place
                cur_offset_in_chunk += block.size
                bytes_to_yield = block.get_bytes()
            elif block.offset_in_chunk < cur_offset_in_chunk:
                # Next block starts earlier than we've sent already.
                # It is possible, though; if the chunk was somehow deduplicated
                # even within the single file, so there are multiple blocks
                # inside same file referring to the same chunk.
                # For example, imagine a large file contains only 0x00 bytes.
                logger.verbose(
                    'Getting bytes for %r, ignoring duplicated block %r',
                    self, block)
                bytes_to_yield = None
            else:  # block.offset_in_chunk > cur_offset_in_chunk:
                # Next block starts later than needed
                missing_bytes = block.offset_in_chunk - cur_offset_in_chunk
                # During backing up, some file was probably already removed.
                # That's a problem, but generally recoverable.
                logger.warning('For some reason, %i bytes were lost '
                                   'from %r #%i; next block is %r',
                               missing_bytes,
                               self, cur_offset_in_chunk, block)
                # Now do the same as if everything was at the proper place
                cur_offset_in_chunk += missing_bytes + block.size
                bytes_to_yield = '\x00' * missing_bytes + block.get_bytes()

            # Update CRC32 property
            if bytes_to_yield is not None:
                self.__crc32 = crc32_gen.send(bytes_to_yield)
                yield bytes_to_yield

        # We've iterated over the blocks.
        # But if we are doing it for the second time, it may happen
        # that during the second reread, the contents have already changed...
        if old_crc32 is not None:
            # Uhm, we indeed have read it for the second time.
            # So let's recheck the CRC32.
            if self.__crc32 != old_crc32:
                logger.error('For %r CRC32 on reread != original CRC32: '
                                 '%04x != %04x',
                             self, self.__crc32, old_crc32)


    def get_bytes(self):
        """The bytes for the chunk, always being re-read (i.e. never cached).

        @note: after the function is called, the C{.crc32} field of the object
            contains the valid value.

        @returns: The byte stream for this chunk.
        @rtype: str
        """
        return ''.join(self.__get_bytes())


    def phys_size(self):
        return self.size()


    @property
    @contract_epydoc
    def crc32(self):
        """
        @rtype: numbers.Integral
        @postcondition: 0 <= result <= 0xFFFFFFFF
        """
        if self.__crc32 is None:
            self.__crc32 = eager_crc32(self.get_bytes())

        return self.__crc32



class ChunkFromFilesFinal(ChunkFromFiles):
    """
    A Chunk which has a contents available from the original files,
    most likely on a backup.

    @note: it always rereads the contents on every C{.get_bytes()} call,
        and updates C{.crc32} property. Thus, the value of the C{.crc32}
        property is unavailable until C{.get_body()} is called at least once,
        and contains the value of the CRC32 of the contents generated
        during the latest C{.get_body()} call.
    """
    __slots__ = ('__expected_crc32')


    def __init__(self, expected_crc32, *args, **kwargs):
        """Constructor.

        @todo: C{self.__expected_crc32} could be used somehow.
        """
        assert 'maxsize_code' in kwargs, (args, kwargs)
        assert 'uuid' in kwargs, (args, kwargs)
        assert 'hash' in kwargs, (args, kwargs)
        super(ChunkFromFilesFinal, self).__init__(*args, **kwargs)
        self.__expected_crc32 = expected_crc32



class ChunkWithEncryptedBodyMixin(object):
    def _hash_read(self):
        """
        Calculate the hash sum of the chunk upon the contents.
        Overrides the according method in Chunk.

        @note: if the hash value was initialized in the class explicitly,
            the explicit value is used. Note that in this case, is will not
            match the real value of the chunk hash. Use C{.hash_from_body}.
        """
        if self._hash is None:
            # Recalculate hash
            self._hash = sha512(self.get_bytes()).digest()

        return super(AbstractChunkWithContents, self)._hash_read()

    hash = property(_hash_read, Chunk._hash_write)



class EncryptedChunkFromFiles(ChunkWithEncryptedBodyMixin,
                              ChunkFromFilesFinal):
    """
    A Chunk which has a contents available from the original files,
    but the contents is encrypted.
    """
    __slots__ = ()


    @contract_epydoc
    def __init__(self, cryptographer, *args, **kwargs):
        """Constructor.

        @type cryptographer: Cryptographer
        """
        super(EncryptedChunkFromFiles, self).__init__(*args, **kwargs)
        self.__cryptographer = cryptographer


    @contract_epydoc
    def get_bytes(self):
        """
        @returns: The byte stream for this chunk,
                  encrypted with the cryptographer.
        @rtype: basestring

        @postcondition: len(result) == self.phys_size()
        """
        bytes_pre = super(EncryptedChunkFromFiles, self).get_bytes()

        phys_size = self.phys_size()
        to_add_final = phys_size - self.size()

        rng = random.Random(bytes_pre)  # pseudorandom

        rand_bytes = ''.join(chr(rng.randrange(0, 255))
                                     for i in xrange(to_add_final))

        final_bytes = (bytes_pre + rand_bytes) if to_add_final \
                                               else bytes_pre
        assert len(final_bytes) == phys_size, (phys_size, final_bytes)

        encrypted = self.__cryptographer.encrypt(final_bytes, self.crc32)
        # The EncryptedChunkFromFiles is normally initialized
        # with the _hash field known. So each time the body is read,
        # let's recheck that the _hash matches.
        if self._hash is not None:
            _recalc_hash = sha512(encrypted).digest()
            _exp_hash = str(self._hash)
            assert _recalc_hash == _exp_hash, \
                   (self, _recalc_hash, _exp_hash)
        return encrypted


    @contract_epydoc
    def phys_size(self):
        """
        For the encrypted chunk, the physical size of the message is higher
        than the real size.

        @postcondition: result % AES_BLOCK_SIZE == 0
        @postcondition: result >= 4 * 1024
        """
        # Original size should be:
        # 1. Aligned to AES_BLOCK_SIZE
        # 2. Increased to be not shorter than 4k bytes.
        size_pre = self.size()
        assert size_pre == self._size, (size_pre, self._size)

        # How many bytes should be added to align the contents?
        to_align = (-size_pre) % AES_BLOCK_SIZE
        aligned_size = size_pre + to_align

        rng = random.Random(size_pre)  # pseudorandom

        # If the chunk body is shorter than 128k, let's add a random number
        # or random bytes so it increases its size, so it's at least
        # not shorter than 4k.
        # Technically, we'd want just to increase any "shorter than 4k" chunk
        # to be at 4k long; but this would cause a lot of chunks to become
        # exactly 4k long. So let's not increase any histogram of
        # chunk lengths distribution significantly, and distribute
        # the increased size more or less evenly.
        _4k = 4 * 1024
        _128k = 128 * 1024
        if aligned_size < _128k:
            _to_pepper_min = max(_4k - aligned_size, 0)
            _to_pepper_max = _128k - aligned_size
            final_size = aligned_size + rng.randrange(_to_pepper_min,
                                                      _to_pepper_max,
                                                      AES_BLOCK_SIZE)
        else:
            final_size = aligned_size

        assert final_size % AES_BLOCK_SIZE == 0, \
               (aligned_size, final_size, _to_pepper_min, _to_pepper_max)
        return final_size


    @classmethod
    @contract_epydoc
    def from_non_encrypted(cls, cryptographer, orig_chunk):
        """
        Create an encrypted chunk from non-encrypted chunk.

        @type cryptographer: Cryptographer

        @type orig_chunk: ChunkFromFiles
        @precondition: not isinstance(orig_chunk, EncryptedChunkFromFiles)
                       # orig_chunk

        @rtype: EncryptedChunkFromFiles
        """
        return cls(  # Chunk-specific
                   maxsize_code=orig_chunk.maxsize_code,
                   uuid=orig_chunk.uuid,
                   hash=orig_chunk._hash,
                   size=orig_chunk.size(),
                   blocks=orig_chunk.blocks,
                     # AbstractChunkWithContents-specific
                     # ---
                     # ChunkFromFiles-specific
                     # ---
                     # ChunkFromFilesFinal-specific
                   expected_crc32=orig_chunk.crc32,
                     # EncryptedChunkFromFiles-specific
                   cryptographer=cryptographer)
