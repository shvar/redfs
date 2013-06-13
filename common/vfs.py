#!/usr/bin/python
"""
Virtual File System abstractions.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import numbers
import os
import stat
from datetime import datetime
from types import NoneType

from . import os_ex
from .abstractions import IPrintable
from .utils import coalesce


#
# Constants
#

DEFAULT_PIECE_SIZE = 64 * 1024



#
# Class
#

class NoContentsException(Exception):
    """Some file cannot be read, cause it doesn't have any contents
    (like, maybe, it is a directory).
    """
    pass


class PathDeletedException(Exception):
    """Some operation has been called for a deleted path and is inapplicable.
    """
    pass



class VirtualFile(IPrintable):
    """
    A file (maybe a directory), as can be stored somewhere: with the filename
    (but not the whole path/url), with the metainformation (C{stat}-type),
    and with the functions to open the file or re-get its C{stat}.

    If its size is C{None} (and it is not a directory),
    the file is considered deleted.

    Sometimes the C{VirtualFile} is used to descript the file state
    being backed up. In this case, if its

    """

    __slots__ = ('filename', 'stat', '_stat_getter', 'file_getter')


    def __init__(self, filename, stat, stat_getter, file_getter=None):
        r"""Constructor.

        >>> # Smoke test
        >>> VirtualFile(filename='', stat=os_ex.fake_stat(st_mode=0),
        ...             stat_getter=lambda: None)  \
        ... # doctest:+NORMALIZE_WHITESPACE
        VirtualFile(filename='',
                    stat=FakeStatResult(st_mode=0, st_nlink=1, st_size=0))

        >>> from contextlib import closing
        >>> from cStringIO import StringIO
        >>> _file_getter = lambda: closing(StringIO('X'*50))

        >>> VirtualFile(
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=stat.S_IFREG),
        ...     stat_getter=lambda: os_ex.fake_stat(st_mode=stat.S_IFREG),
        ...     file_getter=_file_getter
        ... )  # doctest:+NORMALIZE_WHITESPACE
        VirtualFile(filename='data.txt',
                    stat=FakeStatResult(st_mode=32768, st_nlink=1, st_size=0))

        @param filename: the name of the file itself (not the full path!)
            Eg: 'explorer.exe'
        @type filename: basestring

        @param stat: the result of calling (or virtualizing) C{os.stat()}
            on such file. That is, either a physical stat-structure, or a
            C{FakeStatResult} object.
            Considered deleted if C{stat.st_mode is None}.

        @param stat_getter: a function that, when called,
            returns the latest "stat" of this file.
            Causes the C{VirtualFile} to have the C{.restat()} method,
            generating the latest stat value whenever called.
            Note that if the function returns C{None}, it means that the file
            is not anymore available. This function should not
            raise any exceptions!
        @type stat_getter: col.Callable

        @param file_getter: a function that, when called,
            returns a "file"-like object for this file.
            Contains C{None} if such object cannot be returned
            (directory? or deleted file?).
            Causes the C{VirtualFile} to have the C{.contents()} method,
            generating the file contents whenever called.
        @type file_getter: col.Callable, NoneType

        @rtype: VirtualFile
        """
        assert stat is not None

        self.filename = filename
        self.stat = stat
        self._stat_getter = stat_getter
        self.file_getter = file_getter


    def __str__(self):
        return u'filename={self.filename!r}, stat={self.stat!r}' \
                   .format(self=self)


    def contents(self, __piece_size_override=None):
        r"""Generator that yields the contents of the file, portion by portion.

        >>> from contextlib import closing
        >>> from cStringIO import StringIO
        >>> _file_getter = lambda: closing(StringIO('X'*50))

        >>> bad = VirtualFile(filename='',
        ...                   stat=os_ex.fake_stat(st_mode=0),
        ...                   stat_getter=lambda: None)

        >>> bad.contents()  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        NoContentsException:
            VirtualFile(filename='',
            stat=FakeStatResult(st_mode=0, st_nlink=1, st_size=0))

        >>> good = VirtualFile(filename='',
        ...                    stat=os_ex.fake_stat(st_mode=0),
        ...                    stat_getter=lambda: None,
        ...                    file_getter=_file_getter)

        >>> list(good.contents(7))  # doctest:+NORMALIZE_WHITESPACE
        ['XXXXXXX', 'XXXXXXX', 'XXXXXXX', 'XXXXXXX', 'XXXXXXX',
         'XXXXXXX', 'XXXXXXX', 'X']
        >>> list(good.contents(17))  # testing reiteration
        ['XXXXXXXXXXXXXXXXX', 'XXXXXXXXXXXXXXXXX', 'XXXXXXXXXXXXXXXX']

        @raises NoContentsException: if the file has not contents
            that can be read (eg, it is a directory).
        """
        if self.file_getter is None:
            raise NoContentsException(repr(self))
        else:
            return self.__contents(piece_size=coalesce(__piece_size_override,
                                                       DEFAULT_PIECE_SIZE))


    def __contents(self, piece_size=DEFAULT_PIECE_SIZE):
        r"""Private function, just to generate the contents of the file.

        >>> from contextlib import closing
        >>> from cStringIO import StringIO
        >>> _file_getter = lambda: closing(StringIO('X'*50))

        >>> bad = VirtualFile(filename='',
        ...                   stat=os_ex.fake_stat(st_mode=0),
        ...                   stat_getter=lambda: None)

        >>> bad._VirtualFile__contents()  # doctest:+ELLIPSIS
        <generator object __contents at 0x...>

        >>> good = VirtualFile(filename='',
        ...                    stat=os_ex.fake_stat(st_mode=0),
        ...                    stat_getter=lambda: None,
        ...                    file_getter=_file_getter)

        >>> list(good._VirtualFile__contents(7))  \
        ... # doctest:+NORMALIZE_WHITESPACE
        ['XXXXXXX', 'XXXXXXX', 'XXXXXXX', 'XXXXXXX', 'XXXXXXX',
         'XXXXXXX', 'XXXXXXX', 'X']
        >>> list(good._VirtualFile__contents(17))  # testing reiteration
        ['XXXXXXXXXXXXXXXXX', 'XXXXXXXXXXXXXXXXX', 'XXXXXXXXXXXXXXXX']
        """
        cls = self.__class__
        with self.file_getter() as fh:
            for piece in cls.contents_for_file_by_file(
                             fh, piece_size=piece_size):
                yield piece


    @property
    def is_deleted(self):
        """Whether the file is considered deleted.

        >>> VirtualFile(
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=stat.S_IFREG),
        ...     stat_getter=lambda: None
        ... ).is_deleted
        False

        >>> VirtualFile(
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=None, size=None),
        ...     stat_getter=lambda: None
        ... ).is_deleted
        True

        >>> VirtualFile(
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=stat.S_IFDIR, size=25),
        ...     stat_getter=lambda: None
        ... ).is_deleted
        False

        >>> VirtualFile(
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=None, size=None),
        ...     stat_getter=lambda: None
        ... ).is_deleted
        True
        """
        # If the file is "deleted", it definitely contains a fake stat result
        # instead of physically implemented one. For fake stat, we consider
        # the file deleted if st_mode is None.
        return (isinstance(self.stat, os_ex.FakeStatResult) and
                self.stat.means_deleted)


    @property
    def isdir(self):
        r"""Whether the file is actually a directory.

        Not applicable for deleted paths.

        >>> VirtualFile('',
        ...             os_ex.fake_stat(st_mode=stat.S_IFDIR|0600),
        ...             lambda: None).isdir
        True
        >>> VirtualFile('',
        ...             os_ex.fake_stat(st_mode=stat.S_IFREG|0700),
        ...             lambda: None).isdir
        False
        >>> VirtualFile('',
        ...             os_ex.fake_stat(st_mode=None),
        ...             lambda: None).isdir  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        PathDeletedException:
            VirtualFile(filename='',
                        stat=FakeStatResult(st_nlink=1, st_size=0))

        @rtype: bool
        @raises PathDeletedException: if called for deleted path.
        """
        if self.is_deleted:
            raise PathDeletedException(repr(self))
        else:
            return os_ex.stat_isdir(self.stat)


    @property
    def isfile(self):
        r"""Whether the file is actually a regular file with contents.

        Not applicable for deleted paths.

        >>> VirtualFile('',
        ...             os_ex.fake_stat(st_mode=stat.S_IFDIR|0755),
        ...             lambda: None).isfile
        False
        >>> VirtualFile('',
        ...             os_ex.fake_stat(st_mode=stat.S_IFREG|0644),
        ...             lambda: None).isfile
        True
        >>> VirtualFile('',
        ...             os_ex.fake_stat(st_mode=None),
        ...             lambda: None).isfile  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        PathDeletedException:
            VirtualFile(filename='',
                        stat=FakeStatResult(st_nlink=1, st_size=0))

        @rtype: bool
        @raises PathDeletedException: if called for deleted path.
        """
        if self.is_deleted:
            raise PathDeletedException(repr(self))
        else:
            return os_ex.stat_isfile(self.stat)


    @property
    def size(self):
        r"""Get file size (if the file has contents).

        If it is C{None}, the file is considered deleted.

        >>> # Existing file
        >>> VirtualFile(
        ...     '',
        ...     os_ex.fake_stat(st_mode=stat.S_IFREG, size=15),
        ...     lambda: None
        ... ).size
        15

        >>> # Deleted file
        >>> VirtualFile(
        ...     '',
        ...     os_ex.fake_stat(st_mode=None, size=None),
        ...     lambda: None
        ... ).size  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        PathDeletedException:
            VirtualFile(filename='', stat=FakeStatResult(st_nlink=1))

        >>> VirtualFile(
        ...     '',
        ...     os_ex.fake_stat(st_mode=stat.S_IFDIR),
        ...     lambda: None
        ... ).size  # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        NoContentsException: VirtualFile(filename='',
                                         stat=FakeStatResult(st_mode=16384,
                                                             st_nlink=1,
                                                             st_size=0))

        @rtype: numbers.Integral
        @raises NoContentsException: if the file size is not calculable.
            (eg, if it is a directory).
        """
        if self.isdir or self.is_deleted:
            raise NoContentsException(repr(self))
        else:
            return self.stat.st_size


    @property
    def time_changed(self):
        r"""The time of last change.

        If C{None} (usually used to refer to some file state), it means
        that the file time is not important, and the latest one
        should be taken.

        >>> VirtualFile('',
        ...             os_ex.fake_stat(atime=datetime(2012, 10, 3),
        ...                             mtime=datetime(2012, 11, 2),
        ...                             ctime=datetime(2012, 12, 1)),
        ...             lambda: None).time_changed
        datetime.datetime(2012, 12, 1, 0, 0)

        >>> VirtualFile('',
        ...             os_ex.fake_stat(atime=datetime(2012, 12, 1),
        ...                             mtime=datetime(2012, 10, 3),
        ...                             ctime=datetime(2012, 11, 2)),
        ...             lambda: None).time_changed
        datetime.datetime(2012, 11, 2, 0, 0)

        >>> VirtualFile('',
        ...             os_ex.fake_stat(atime=datetime(2012, 10, 3),
        ...                             mtime=datetime(2012, 11, 2),
        ...                             ctime=datetime(2012, 12, 1)),
        ...             lambda: None).time_changed
        datetime.datetime(2012, 12, 1, 0, 0)

        @rtype: datetime, NoneType
        """
        assert self.stat
        return os_ex.stat_get_mtime(self.stat)


    def restat(self):
        r"""Get the most actual and fresh value of file stat.

        May be overridden in implementations.

        The implementation in C{VirtualFile} returns the original stat value
        always (so don't ever change its fields! Though you'll most like won't
        want to do it anyway).

        >>> st1 = os_ex.fake_stat(st_mode=16585, st_ino=1234, st_dev=5678,
        ...                       nlink=3, uid=205, gid=308, size=42,
        ...                       atime=datetime(2012, 10, 3),
        ...                       mtime=datetime(2012, 11, 2),
        ...                       ctime=datetime(2012, 12, 1))
        >>> st2 = os_ex.fake_stat(st_mode=16787, st_ino=4321, st_dev=8765,
        ...                       nlink=2, uid=502, gid=803, size=24,
        ...                       atime=datetime(2012, 3, 1),
        ...                       mtime=datetime(2012, 2, 2),
        ...                       ctime=datetime(2012, 1, 3))
        >>> vf = VirtualFile(filename='', stat=st1, stat_getter=lambda: st2)
        >>> # Read the original stat
        >>> vf.stat  # doctest:+NORMALIZE_WHITESPACE,
        FakeStatResult(st_mode=16585, st_ino=1234, st_dev=5678, st_nlink=3,
                       st_uid=205, st_gid=308, st_size=42,
                       st_atime=1349222400.0,
                       st_mtime=1351814400.0,
                       st_ctime=1354320000.0)
        >>> # "Reread" the stat "physically"
        >>> vf.restat()  # doctest:+NORMALIZE_WHITESPACE
        FakeStatResult(st_mode=16787, st_ino=4321, st_dev=8765, st_nlink=2,
                       st_uid=502, st_gid=803, st_size=24,
                       st_atime=1330560000.0,
                       st_mtime=1328140800.0,
                       st_ctime=1325548800.0)
        >>> # Read the original stat again
        >>> # - it shouldn't have been changed by restat()
        >>> vf.stat  # doctest:+NORMALIZE_WHITESPACE
        FakeStatResult(st_mode=16585, st_ino=1234, st_dev=5678, st_nlink=3,
                       st_uid=205, st_gid=308, st_size=42,
                       st_atime=1349222400.0,
                       st_mtime=1351814400.0,
                       st_ctime=1354320000.0)
        """
        return self._stat_getter()


    @classmethod
    def contents_for_file_by_file(cls, file_handler,
                                  piece_size=DEFAULT_PIECE_SIZE):
        r"""Contents getter function for a file defined by its file handler.

        Goes well together with C{functools.partial()} as an argument
        to C{VirtualFile} constructor.

        >>> from cStringIO import StringIO

        >>> c1 = VirtualFile.contents_for_file_by_file(StringIO('X'*50), 7)
        >>> c1  # doctest:+ELLIPSIS
        <generator object contents_for_file_by_file at 0x...>

        >>> list(c1)  # doctest:+NORMALIZE_WHITESPACE
        ['XXXXXXX', 'XXXXXXX', 'XXXXXXX', 'XXXXXXX', 'XXXXXXX',
         'XXXXXXX', 'XXXXXXX', 'X']

        >>> c2 = VirtualFile.contents_for_file_by_file(StringIO('X'*50), 17)
        >>> c2  # doctest:+ELLIPSIS
        <generator object contents_for_file_by_file at 0x...>
        >>> list(c2)
        ['XXXXXXXXXXXXXXXXX', 'XXXXXXXXXXXXXXXXX', 'XXXXXXXXXXXXXXXX']

        @param file_handler: the C{file}-compatible object.

        @return: the file data portions.
        @rtype: col.Iterable
        """
        data = True
        while data:
            data = file_handler.read(piece_size)
            if data:
                yield data



class RelVirtualFile(VirtualFile):
    """A Virtual File, within a particular (relative) directory."""

    __slots__ = ('rel_dir',)


    def __init__(self, rel_dir, *args, **kwargs):
        r"""Constructor.

        >>> # Smoke test
        >>> RelVirtualFile(rel_dir='', filename='',
        ...                stat=os_ex.fake_stat(st_mode=0),
        ...                stat_getter=lambda: None) \
        ... # doctest:+NORMALIZE_WHITESPACE
        RelVirtualFile(rel_dir='', filename='',
                       stat=FakeStatResult(st_mode=0, st_nlink=1, st_size=0))

        >>> from contextlib import closing
        >>> from cStringIO import StringIO
        >>> _file_getter = lambda: closing(StringIO('X'*50))

        >>> # Emulate some real file
        >>> rvf = RelVirtualFile(
        ...     # RelVirtualFile-specific
        ...     rel_dir='a/b/c',
        ...     # VirtualFile-specific
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=stat.S_IFREG),
        ...     stat_getter=lambda: os_ex.fake_stat(st_mode=stat.S_IFREG),
        ...     file_getter=_file_getter
        ... )
        >>> rvf  # doctest:+NORMALIZE_WHITESPACE
        RelVirtualFile(rel_dir='a/b/c',
                       filename='data.txt',
                       stat=FakeStatResult(st_mode=32768, st_nlink=1,
                                           st_size=0))
        >>> rvf.is_deleted
        False

        >>> # Emulate a deleted file
        >>> rvf_d = RelVirtualFile(
        ...     # RelVirtualFile-specific
        ...     rel_dir='a/b/c',
        ...     # VirtualFile-specific
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=None),
        ...     stat_getter=lambda: None,
        ...     file_getter=None
        ... )
        >>> rvf_d  # doctest:+NORMALIZE_WHITESPACE
        RelVirtualFile(rel_dir='a/b/c',
                       filename='data.txt',
                       stat=FakeStatResult(st_nlink=1, st_size=0))
        >>> rvf_d.is_deleted
        True

        @param rel_dir: the path to the directory where the file is located,
            relative to the base directory. Eg: 'System32/Drivers'
            (if base directory is pointed to 'C:/Windows').
        @type rel_dir: basestring
        """
        super(RelVirtualFile, self).__init__(*args, **kwargs)
        self.rel_dir = rel_dir


    def __str__(self):
        return u'rel_dir={self.rel_dir!r}, {super}' \
                   .format(self=self,
                           super=super(RelVirtualFile, self).__str__())


    @property
    def rel_path(self):
        r"""Get a full relative path of a file.

        >>> RelVirtualFile(
        ...     # RelVirtualFile-specific
        ...     rel_dir='a/b/c',
        ...     # VirtualFile-specific
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=stat.S_IFREG),
        ...     stat_getter=lambda: os_ex.fake_stat(st_mode=stat.S_IFREG)
        ... ).rel_path
        'a/b/c/data.txt'

        @rtype: basestring
        """
        return os.path.join(self.rel_dir, self.filename)


    @classmethod
    def from_virtual_file(cls, rel_dir, vfile):
        r"""
        Create a C{RelVirtualFile} from existing C{VirtualFile}, and a given
        relative directory.

        >>> vfile = VirtualFile(
        ...     filename='data.txt',
        ...     stat=os_ex.fake_stat(st_mode=stat.S_IFREG),
        ...     stat_getter=lambda: os_ex.fake_stat(st_mode=stat.S_IFREG)
        ... )

        >>> RelVirtualFile.from_virtual_file(
        ...     'a/b/c', vfile)  # doctest:+NORMALIZE_WHITESPACE
        RelVirtualFile(rel_dir='a/b/c',
                       filename='data.txt',
                       stat=FakeStatResult(st_mode=32768, st_nlink=1,
                                           st_size=0))

        @type rel_dir: basestring
        @type vfile: VirtualFile

        @rtype: RelVirtualFile
        """
        return cls(rel_dir=rel_dir,
                   filename=vfile.filename,
                   stat=vfile.stat,
                   stat_getter=vfile._stat_getter,
                   file_getter=vfile.file_getter)
