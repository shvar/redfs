#!/usr/bin/python
"""
The mechanism of tracking the directory changes by caching
the intermediate states in the memory.
"""

#
# Imports
#

from __future__ import absolute_import
import sys

from twisted.python import filepath



#
# Constants
#

_nonrecursive_descend = lambda path: False

ACT_CREATE = 0b10
ACT_MOVE = 0b100
ACT_CHANGE = 0b1000
ACT_DELETE = 0b10000

_ACT_NAMES_MAP = {
    ACT_CREATE: 'ACT_CREATE',
    ACT_MOVE:   'ACT_MOVE',
    ACT_CHANGE: 'ACT_CHANGE',
    ACT_DELETE: 'ACT_DELETE'
}



#
# Functions
#

def action_name(action):
    return _ACT_NAMES_MAP[action]


class _ComparatorFSM(object):
    """
    Compare 2 directory snapshots.
    """

    def __init__(self, before, after):
        self.before = before
        self.after = after

    def icompare(self):
        """
        Yields `patch` needed for transform before to after.
        """
        diff = set(i[0] for i in self.before.uniq ^ self.after.uniq)
        for item in diff:
            if item.path in self.before.by_path:  # path present in before
                yield self._in_before(item)  # -> RM | CH | MV(b, a)
            else:  # path present in other
                yield self._not_before__in_after(item)  # -> CR | MV(b, a)

    def _in_before(self, item):
        if item.path in self.after.by_path:  # path present in both
            return (ACT_CHANGE, item)
        else:
            return self._in_before__not_after(item)  # -> RM | MV

    def _in_before__not_after(self, item):
        info = self.before.by_path[item.path]
        if info[3] in self.after.by_inode:  # inode presents in after
            return (ACT_MOVE, item, self.after.by_inode[info[3]][0])
        else:
            return (ACT_DELETE, item)

    def _not_before__in_after(self, item):
        info = self.after.by_path[item.path]
        if info[3] in self.before.by_inode:  # inode presents in before
            return (ACT_MOVE, self.before.by_inode[info[3]][0], item)
        else:
            return (ACT_CREATE, item)


class DirectorySnapshot(object):
    """
    One-moment directory content's snapshot.
    Allow create snapshot.
    """
    __slots__ = ('root', 'recursive', 'by_path', '__by_inode', 'uniq')

    def __init__(self, root, recursive=False, quiet=False):
        """
        Create new snapshot

        @param root: snapshot's root directory
        @type root: filepath.FilePath
        @param quiet: don't throw if root doesn't exists, just create empty
            snapshot.
        """
        self.root = root
        self.recursive = recursive
        self.by_path = {}
        self.__by_inode = None
        self.uniq = set()
        try:  # if 'root' path doesn't exists
            paths = root.walk() if recursive else root.children()
        except filepath.UnlistableError:
            if not quiet:
                raise
            paths = []
        for subpath in paths:
            try:
                info = (subpath,
                        subpath.getModificationTime(),
                        subpath.getsize(),
                        subpath.statinfo.st_ino)
            except OSError:
                pass
            else:
                self.by_path[subpath.path] = info
                self.uniq.add(info)

    @property
    def by_inode(self):
        """
        Generate hash by inode number on-demand.
        """
        if self.__by_inode is None:
            self.__by_inode = {}
            for info in self.by_path.itervalues():
                self.__by_inode[info[3]] = info
        return self.__by_inode

    def __sub__(self, other):
        """
        Show what actions need to do with self for get other.
        """
        return _ComparatorFSM(self, other)

    def __iter__(self):
        return (p[0] for p in self.by_path.itervalues())

    def __repr__(self):
        return u'{}(root={root}, by_path={by_path})' \
                   .format(self.__class__.__name__,
                           root=self.root,
                           by_path=self.by_path)


if __debug__:
    if 'nose' in sys.modules:
        import os
        import tempfile
        import unittest


        class TestDirSnapshot(unittest.TestCase):

            __slots__ = ('__root', '__root_fp')

            def setUp(self):
                self.__root = tempfile.mkdtemp()
                self.__root_fp = filepath.FilePath(self.__root)

            def tearDown(self):
                os.rmdir(self.__root)

            def __create_dir_snapshot_with_test_file(self):
                """
                Returns the tuple of FilePath for the created file
                and the directory snapshot
                """
                with tempfile.NamedTemporaryFile(dir=self.__root) as tmp_file:
                    return (filepath.FilePath(tmp_file.name),
                            DirectorySnapshot(filepath.FilePath(self.__root)))

            def test_create(self):
                before = DirectorySnapshot(filepath.FilePath(self.__root))
                fpath, after = self.__create_dir_snapshot_with_test_file()
                diff = before - after
                self.assertIn((ACT_CREATE, fpath), diff.icompare())

            def test_remove(self):
                fpath, before = self.__create_dir_snapshot_with_test_file()
                after = DirectorySnapshot(filepath.FilePath(self.__root))
                diff = before - after
                self.assertIn((ACT_DELETE, fpath), diff.icompare())

            def test_change(self):
                with tempfile.NamedTemporaryFile(dir=self.__root) as tmp_file:
                    fpath = filepath.FilePath(tmp_file.name)
                    before = DirectorySnapshot(filepath.FilePath(self.__root))
                    tmp_file.write('1')
                    tmp_file.flush()
                    os.fsync(tmp_file.fileno())
                    after = DirectorySnapshot(filepath.FilePath(self.__root))
                diff = before - after
                self.assertIn((ACT_CHANGE, fpath), diff.icompare())
