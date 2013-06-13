#!/usr/bin/python
"""
Extra functions than enhance the functionality of default C{os} module.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import os
import re
import stat
from collections import namedtuple
from datetime import datetime
from types import NoneType

from .abstractions import IPrintable
from .utils import coalesce, on_exception, dt_to_ts



#
# Constants
#

logger = logging.getLogger(__name__)
logger_fs_problem = logging.getLogger('status.fs_problem')



#
# Classes
#

FakeStatResult = namedtuple('FakeStatResult',
                            ['st_mode', 'st_ino', 'st_dev', 'st_nlink',
                             'st_uid', 'st_gid', 'st_size',
                             'st_atime', 'st_mtime', 'st_ctime'])

class FakeStatResult(IPrintable):
    """
    A class that simulates the C{stat} object usually returned by calls
    like {os.stat()}.
    """

    __slots__ = ('st_mode', 'st_ino', 'st_dev', 'st_nlink',
                 'st_uid', 'st_gid', 'st_size',
                 'st_atime', 'st_mtime', 'st_ctime')


    def __init__(self, st_mode=None, st_ino=None, st_dev=None, st_nlink=None,
                       st_uid=None, st_gid=None, st_size=None, st_atime=None,
                       st_mtime=None, st_ctime=None):
        r"""Constructor.

        >>> # Smoke test
        >>> FakeStatResult()
        FakeStatResult()

        >>> # Real values
        >>> FakeStatResult(
        ...     st_mode=33261, st_ino=1234, st_dev=8765, st_nlink=4,
        ...     st_uid=2005, st_gid=2007, st_size=34256,
        ...     st_atime=1349222400, st_mtime=1351814400, st_ctime=1354320000
        ... )  # doctest:+NORMALIZE_WHITESPACE
        FakeStatResult(st_mode=33261, st_ino=1234, st_dev=8765, st_nlink=4,
            st_uid=2005, st_gid=2007, st_size=34256, st_atime=1349222400,
            st_mtime=1351814400, st_ctime=1354320000)

        @note: C{st_mtime} and C{st_ctime} must either both be C{None},
            or both be C{not None}.
        """
        assert ((st_mtime is None and st_ctime is None) or
                (st_mtime is not None and st_ctime is not None)), \
               (st_mtime, st_ctime)
        self.st_mode = st_mode
        self.st_ino = st_ino
        self.st_dev = st_dev
        self.st_nlink = st_nlink
        self.st_uid = st_uid
        self.st_gid = st_gid
        self.st_size = st_size
        self.st_atime = st_atime
        self.st_mtime = st_mtime
        self.st_ctime = st_ctime


    def __str__(self):
        parts = [
            ('st_mode', self.st_mode),
            ('st_ino', self.st_ino),
            ('st_dev', self.st_dev),
            ('st_nlink', self.st_nlink),
            ('st_uid', self.st_uid),
            ('st_gid', self.st_gid),
            ('st_size', self.st_size),
            ('st_atime', self.st_atime),
            ('st_mtime', self.st_mtime),
            ('st_ctime', self.st_ctime),
        ]
        return ', '.join('{}={!r}'.format(name, value)
                             for name, value in parts
                             if value is not None)


    @property
    def means_deleted(self):
        """
        @todo: even the deleted file in some cases needs a fake "stat" value
            to contain the update/delete time.
        """
        return self.st_mode is None



#
# Functions
#

@on_exception(callback=lambda exc: [])
def safe_listdir(_dir):
    """
    Similar to C{os.listdir}, but, if a problem occurs
    during reading the directory, warn via a logger and return empty result
    rather than raise an exception.
    """
    try:
        return os.listdir(_dir)
    except Exception as e:
        logger_fs_problem.warning('Cannot read the directory %r: %r', _dir, e,
                                  extra={'path': _dir,
                                         '_type': 'list dir',
                                         'exc': e})
        raise


@on_exception(callback=lambda exc: None)
def safe_stat(path):
    """
    Similar to C{os.stat}, but, if a problem occurs
    during reading the attributes, warn via a logger and return empty result
    rather than raise an exception.

    @returns: a stat object, or C{None}.
    """
    try:
        return os.stat(path)
    except Exception as e:
        logger_fs_problem.warning('Cannot read attributes for %r: %r', path, e,
                                  extra={'path': path,
                                         '_type': 'stat',
                                         'exc': e})
        raise


def fake_stat(st_mode=0777 | stat.S_IFREG,
              st_ino=None, st_dev=None,
              isdir=None,
              nlink=1,
              uid=None, gid=None,
              size=0,
              atime=None, mtime=None, ctime=None):
    r"""Create a fake C{stat}-alike structure from the data.

    >>> # Smoke test
    >>> fake_stat()  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=33279, st_nlink=1, st_size=0)

    >>> # Semi-real data

    >>> # 1. Neither a file, nor a directory
    >>> fake_stat(st_mode=0755, st_ino=1234, st_dev=8765,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=493, st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    >>> # 2.1 A file (defined via isdir).
    >>> fake_stat(st_mode=0755, st_ino=1234, st_dev=8765, isdir=False,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=33261, st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    >>> # 2.2 A file (defined via st_mode).
    >>> fake_stat(st_mode=33261, st_ino=1234, st_dev=8765,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=33261, st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    >>> # 3.1 A directory (defined via isdir).
    >>> fake_stat(st_mode=0755, st_ino=1234, st_dev=8765, isdir=True,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=16877, st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    >>> # 3.2 A directory (defined via st_mode).
    >>> fake_stat(st_mode=16877, st_ino=1234, st_dev=8765,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=16877, st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    >>> # 4. A mix of st_mode and isdir definitions.

    >>> # 4.1 Previously a directory, forcefully converted to a file
    >>> #     (but why!?)
    >>> fake_stat(st_mode=16877, st_ino=1234, st_dev=8765, isdir=False,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=33261, st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    >>> # 4.2 Previously a file, forcefully converted to a directory
    >>> #     (but why!?)
    >>> fake_stat(st_mode=33261, st_ino=1234, st_dev=8765, isdir=True,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_mode=16877, st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    >>> # 5 Deleted directory (or file)....
    >>> fake_stat(st_mode=None, st_ino=1234, st_dev=8765,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)
    >>> # 5.1 ... and you cannot override whether it is a directory or file.
    >>> fake_stat(st_mode=None, st_ino=1234, st_dev=8765, isdir=True,
    ...           nlink=4, uid=2005, gid=2007, size=34256,
    ...           atime=datetime(2012, 10, 3),
    ...           mtime=datetime(2012, 11, 2),
    ...           ctime=datetime(2012, 12, 1))  # doctest:+NORMALIZE_WHITESPACE
    FakeStatResult(st_ino=1234, st_dev=8765, st_nlink=4,
        st_uid=2005, st_gid=2007, st_size=34256,
        st_atime=1349222400.0, st_mtime=1351814400.0, st_ctime=1354320000.0)

    @note: C{mtime} and C{ctime} must either both be C{None},
        or both be C{not None}.

    @param st_mode: the C{st_mode} flag; if not C{None} must be an int,
        never a long!. C{None} means the file was deleted
        (also C{is_dir} cannot be applied to it).
    @type st_mode: int, NoneType

    @param isdir: force the stat to be considered either a directory
        (C{isdir=True}) or a regular file (C{isdir=False}).
        The enforcing modifies the flags in C{st_mode} if they were present;
        after the modification the C{st_mode} is definitely valid and cannot
        contain, say, both C{stat.S_IFREG} and C{stat.S_IFDIR} flags set
        simultaneously.
        If C{isdir=None}, no enforcing is performed.
        If neither C{st_mode} or C{isdir} is provided, the stat contents
        is considered meaning a regular file.
    @type isdir: bool, NoneType

    @param size: the size of the file. May be C{None} if the file/dir
        was deleted; but one should define the "deleted" stats
        via C{st_mode=None}!
    @type size: numbers.Integral, NoneType

    @type atime: datetime, NoneType
    @type mtime: datetime, NoneType
    @type ctime: datetime, NoneType

    @rtype: FakeStatResult
    """
    assert ((mtime is None and ctime is None) or
            (mtime is not None and ctime is not None)), \
           (mtime, ctime)

    if st_mode is None:
        # The file/dir was deleted; isdir is invalid if present
        if isdir is not None:
            logger.error('Trying to set isdir to %r while st_mode was None',
                         isdir)
        new_st_mode = st_mode
    else:
        # If isdir is not None, we:
        # 1. nullify the bits regarding the file type (via AND);
        # 2. force-set the proper bits (via OR).
        st_mode_and = 0x0FFF if isdir is not None else 0xFFFF
        st_mode_or = (stat.S_IFDIR if isdir else stat.S_IFREG) \
                         if isdir is not None \
                         else 0
        new_st_mode = (st_mode & st_mode_and) | st_mode_or

    return FakeStatResult(
               st_mode=new_st_mode,
               st_ino=st_ino, st_dev=st_dev,
               st_nlink=nlink,
               st_uid=uid, st_gid=gid,
               st_size=size,
               st_atime=dt_to_ts(atime) if atime is not None else None,
               st_mtime=dt_to_ts(mtime) if mtime is not None else None,
               st_ctime=dt_to_ts(ctime) if ctime is not None else None)


def stat_isdir(stat_):
    r"""Whether a stat object is related to a directory.

    >>> # Neither a dir nor a file.
    >>> stat_isdir(fake_stat(st_mode=0777))
    False

    >>> # Definition via st_mode.
    >>> stat_isdir(fake_stat(st_mode=stat.S_IFDIR))
    True
    >>> stat_isdir(fake_stat(st_mode=stat.S_IFREG))
    False

    >>> # Definition via isdir.
    >>> stat_isdir(fake_stat(isdir=True))
    True
    >>> stat_isdir(fake_stat(isdir=False))
    False

    >>> # Mixed (forced) definition.
    >>> stat_isdir(fake_stat(st_mode=stat.S_IFREG, isdir=True))
    True
    >>> stat_isdir(fake_stat(st_mode=stat.S_IFDIR, isdir=False))
    False

    @rtype: bool
    """
    return stat.S_ISDIR(stat_.st_mode)


def stat_isfile(stat_):
    r"""Whether a stat object is related to a file.

    >>> # Neither a dir nor a file.
    >>> stat_isfile(fake_stat(st_mode=0777))
    False

    >>> # Definition via st_mode.
    >>> stat_isfile(fake_stat(st_mode=stat.S_IFDIR))
    False
    >>> stat_isfile(fake_stat(st_mode=stat.S_IFREG))
    True

    >>> # Definition via isdir.
    >>> stat_isfile(fake_stat(isdir=True))
    False
    >>> stat_isfile(fake_stat(isdir=False))
    True

    >>> # Mixed (forced) definition.
    >>> stat_isfile(fake_stat(st_mode=stat.S_IFREG, isdir=True))
    False
    >>> stat_isfile(fake_stat(st_mode=stat.S_IFDIR, isdir=False))
    True

    @rtype: bool
    """
    return stat.S_ISREG(stat_.st_mode)


def stat_get_mtime(stat_):
    r"""
    Given an stat object, return the datetime when the related object
    (is considered) last changed.

    >>> stat_get_mtime(fake_stat(atime=datetime(2012, 10, 3),
    ...                          mtime=datetime(2012, 11, 2),
    ...                          ctime=datetime(2012, 12, 1)))
    datetime.datetime(2012, 12, 1, 0, 0)

    >>> stat_get_mtime(fake_stat(atime=datetime(2012, 12, 1),
    ...                          mtime=datetime(2012, 10, 3),
    ...                          ctime=datetime(2012, 11, 2)))
    datetime.datetime(2012, 11, 2, 0, 0)

    >>> stat_get_mtime(fake_stat(atime=datetime(2012, 10, 3),
    ...                          mtime=datetime(2012, 11, 2),
    ...                          ctime=datetime(2012, 12, 1)))
    datetime.datetime(2012, 12, 1, 0, 0)

    @note: it's not just the actual value if C{st_mtime} field, but the maximum
        of C{st_ctime} and C{st_mtime}. The reason is that the mtime is
        basically user-controllable (and may be forcefully changed earlier
        and later), while ctime is system-controlled.

    @todo: shouldn't we just use st_ctime everywhere? But what about Windows?
        Under Windows, st_ctime is the "file creation" time.

    @rtype: datetime
    """
    return datetime.utcfromtimestamp(max(stat_.st_ctime, stat_.st_mtime))


# Global restriction on most systems (OS and FS) ".", ".." and "/".
__GLOBAL_OS_VALIDATOR = re.compile(r'^(?:\.{1,2}|.*/.*)$')

# Mostly Windows limitations (FAT and NTFS).
# Some info can be find here: http://support.microsoft.com/kb/100108
__WINDOWS_VALIDATOR = re.compile(r"""
                          ^(?:
                              # Restrict chars from FAT and NTFS.
                              .*["*:\\<>?\|+,;=\[\]].*
                              # Restrict file names in Windows.
                              |con|nul|prn|aux
                              # COM1..COM9 and LPT1..LPT9 are restricted
                              # too. But "COM" and "COM10" are not.
                              |com\d|lpt\d
                          )$""", re.VERBOSE)


def find_unsuitable_oses_in_path(path):
    """Find OSes for which this path is invalid.

    @param path: any file or directory path
    @type path: basestring

    @return: return iterable containing the names of OSes where C{path} might
        be an invalid filename; the names contain either C{"any"} or codename
        of the certain OS (as returned by sys.platform);
        the iterable is blank if the name is valid for all OSes;
    @rtype: col.Iterable
    @todo: tests
    """
    error = []
    if not all_oses_validator(path):
        error.append('any')
    if not unix_specific_validator(path):
        error.append('darwin')
    if not windows_specific_validator(path):
        error.append('win32')
    return error


def all_oses_validator(path):
    """
    Returns C{False} if the path is definitely invalid on all supported
    operating systems; C{True} if it is likely valid.

    @param path: any file or derictory path
    @type path: basestring
    @todo: tests
    """
    return True if path and __GLOBAL_OS_VALIDATOR.match(path) is None else False


def windows_specific_validator(path):
    r"""
    Return C{True} if there is no restricted chars for Windows (or it's
    most common file systems: FAT or NTFS) in path or C{False} if there are.

    @param path: any file or directory path
    @type path: basestring

    # >>> windows_specific_validator('/')
    @todo: tests
    """
    return True if path and __WINDOWS_VALIDATOR.match(path.lower()) is None \
                else False


def unix_specific_validator(path):
    """
    Return True if there is no restricted chars for Unix-like systems
    in path or False if there are.
    @note: I don't know any restricted chars in file names on Unix except
        those, which are described in global validator.
    """
    return True

# TODO: tests
# if __debug__:
#     __test__ = {
#         '__WINDOWS_VALIDATOR':
#             r"""
#             >>> f = windows_validator
#             >>> f(''), f('new'), f('new/'), f('..'), f('win?')
#             (False, True, False, True, False)
#             """
#     }
