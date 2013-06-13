#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Extra functions than enhance the functionality of default C{os.path} module.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import ntpath
import os
import posixpath
import sys

from .itertools_ex import accumulate



#
# Constants
#

__FS_ENC = sys.getfilesystemencoding()



#
# Functions
#

def _from_fs_encoding(path, __fs_enc_override=__FS_ENC):
    r"""Convert a path from the filesystem-specific encoding.

    >>> _from_fs_encoding('\xc3\x91\xc2\x84\xc3\x90\xc2\xb0\xc3\x90'
    ...                       '\xc2\xb9\xc3\x90\xc2\xbb.txt',
    ...                   'UTF-8')
    u'\xd1\x84\xd0\xb0\xd0\xb9\xd0\xbb.txt'
    """
    return path.decode(__fs_enc_override)


def _to_fs_encoding(path, __fs_enc_override=__FS_ENC):
    r"""Convert a path to the filesystem-specific encoding.

    >>> _to_fs_encoding(u'\xd1\x84\xd0\xb0\xd0\xb9\xd0\xbb.txt', 'UTF-8')
    '\xc3\x91\xc2\x84\xc3\x90\xc2\xb0\xc3\x90\xc2\xb9\xc3\x90\xc2\xbb.txt'

    >>> _to_fs_encoding(u'файл.txt', 'UTF-8')
    '\xc3\x91\xc2\x84\xc3\x90\xc2\xb0\xc3\x90\xc2\xb9\xc3\x90\xc2\xbb.txt'
    """
    return path.encode(__fs_enc_override)


def expanduser(path, __fs_enc_override=__FS_ENC):
    """
    Similar to C{os.path.expanduser}, but works properly with unicode paths.
    """
    return _from_fs_encoding(
               os.path.expanduser(
                   _to_fs_encoding(path,
                                   __fs_enc_override=__fs_enc_override)),
               __fs_enc_override=__fs_enc_override)


def relpath_nodot(subdir, dir, __ospath_override=os.path):
    """
    Similarly to C{os.path.relpath}, calculate the path of C{subdir} relatively
    to C{dir}; but, if the result is ".", return just an empty string, to split
    the file paths better.

    >>> relpath_nodot('/1/2/3', '/1/2', __ospath_override=posixpath)
    '3'
    >>> relpath_nodot('/1/3', '/1/3', __ospath_override=posixpath)
    ''
    >>> relpath_nodot('/1/3/', '/1/3', __ospath_override=posixpath)
    ''

    @param __ospath_override: the particular module implementing
        the C{os.path} interface, default: C{os.path}
    """
    _relpath = __ospath_override.relpath(subdir, dir)
    return _relpath if _relpath != '.' else ''


def normpath_nodot(path, __ospath_override=os.path):
    """
    Similarly to C{os.path.normpath}, normalize the path;
    but, if the result is ".", return just an empty string, to split
    the file paths better.

    >>> normpath_nodot('.././', __ospath_override=posixpath)
    '..'
    >>> normpath_nodot('.', __ospath_override=posixpath)
    ''
    >>> normpath_nodot('', __ospath_override=posixpath)
    ''

    @param __ospath_override: the particular module implementing
        the C{os.path} interface, default: C{os.path}
    """
    _normpath = __ospath_override.normpath(path)
    return _normpath if _normpath != '.' else ''


def __convert_path(src_path, from_module, to_module):
    r"""
    Convert the path from one path encoding notation to another,
    given src and dst os.path implementation modules.
    After conversion, the path is normalized.

    If the from_module is the same as the to_module, the original name
    is just normalized and then returned.

    >>> __convert_path('', from_module=posixpath, to_module=ntpath) == \
    ... __convert_path('', from_module=ntpath, to_module=posixpath) == \
    ... __convert_path('', from_module=posixpath, to_module=posixpath) == \
    ... __convert_path('', from_module=ntpath, to_module=ntpath) == \
    ... ''
    True

    >>> __convert_path('/home/user',
    ...                from_module=posixpath, to_module=ntpath)
    '\\home\\user'
    >>> __convert_path('/home/user',
    ...                from_module=posixpath, to_module=posixpath)
    '/home/user'
    >>> __convert_path('home/user',
    ...                from_module=posixpath, to_module=posixpath)
    'home/user'

    >>> __convert_path(r'C:\\Windows\\System32\\calc.exe',
    ...                from_module=ntpath, to_module=posixpath)
    'C:/Windows/System32/calc.exe'
    >>> __convert_path('C:/Windows/System32/calc.exe',
    ...                from_module=posixpath, to_module=ntpath)
    'C:\\Windows\\System32\\calc.exe'
    >>> __convert_path('C:/home/d:/docs',
    ...                from_module=posixpath, to_module=ntpath)
    'C:\\home\\d:docs'

    >>> __convert_path(r'C:', from_module=ntpath, to_module=posixpath)
    'C:'
    >>> __convert_path(r'C:\\', from_module=ntpath, to_module=posixpath)
    'C:'
    >>> __convert_path(r'C:', from_module=posixpath, to_module=ntpath)
    'C:\\'

    >>> __convert_path('\home\user', from_module=ntpath, to_module=posixpath)
    '/home/user'
    >>> __convert_path(r'*', from_module=posixpath, to_module=ntpath) == \
    ... __convert_path(r'*', from_module=ntpath, to_module=posixpath) == \
    ... __convert_path(r'*', from_module=ntpath, to_module=ntpath) == \
    ... __convert_path(r'*', from_module=posixpath, to_module=posixpath) == \
    ... '*'
    True
    """
    if src_path == '':
        # There is a special case if src_path is empty:
        # it should stay empty in any other encoding!
        return ''
    elif from_module is to_module:
        # If converting the path to the same notation, don't do anything
        # serious, just normalize it.
        return to_module.normpath(src_path)  # TODO: use normpath_nodot maybe?
    else:
        # Really convert the path

        # First item likely needs to be processed specially
        def first(item):
            if item == '':
                # If the first item is empty, this should become os.sep,
                # as this is an absolute path.
                return to_module.sep
            elif (to_module.__name__ == ntpath.__name__ and
                  len(item) == 2 and
                  item[-1] == ':'):
                # If the first item looks like "C:" and we are converting
                # to ntpath, we need to append backslash, as this is
                # an absolute path.
                return item + to_module.sep
            else:
                return item

        # If the first item (i = 0) is empty (p == ''), this should become
        # equal to os.sep, as this is an absolute path.
        path_parts = \
            (first(p) if i == 0 else p
                 for i, p in enumerate(src_path.split(from_module.sep)))
        # Loop over all paths and apply path.join;
        # then normalize the path as well.
        #
        # TODO: use normpath_nodot maybe?
        return to_module.normpath(reduce(to_module.join,
                                         path_parts))


def encode_posix_path(os_dep_path):
    """Given an OS-dependent path, create an OS-independent (POSIX) one.

    @type os_dep_path: basestring
    @rtype: basestring
    """
    return __convert_path(os_dep_path,
                          from_module=os.path, to_module=posixpath)


def decode_posix_path(os_indep_path):
    """Given an OS-independent (POSIX) path, create an OS-dependent one.

    @type os_indep_path: basestring
    @rtype: basestring
    """
    return __convert_path(os_indep_path,
                          from_module=posixpath, to_module=os.path)



def __sanitize_path_win32(os_dep_path):
    r"""Win32-specific implementation of sanitize_path().

    >>> __sanitize_path_win32('C:\\Documents and Settings\\host\\Desktop\\.C:')
    'C:\\Documents and Settings\\host\\Desktop\\.C_'
    """
    drive, path = ntpath.splitdrive(os_dep_path)
    # There should not be colons after the disk definition.
    path = path.replace(':', '_')
    return ntpath.join(drive, path)


def __sanitize_path_posix(os_dep_path):
    """Linux-specific implementation of sanitize_path()."""
    return os_dep_path


def sanitize_path(os_dep_path):
    """
    Given an OS-dependent path, sanitize it so that it is fully valid and
    (technically) can be used for a file.
    """
    if sys.platform == 'win32':
        return __sanitize_path_win32(os_dep_path)

    elif sys.platform.startswith('linux') or sys.platform == 'darwin':
        return __sanitize_path_posix(os_dep_path)

    else:
        raise NotImplementedError(u'sanitize_path({!r})/{}'
                                      .format(os_dep_path, sys.platform))


def get_intermediate_paths_posix(leaf_path):
    """
    Given a posixpath to a file, return all the directories on the path to it.

    >>> isinstance(get_intermediate_paths_posix('a/b/c/d/e/f/g'),
    ...            col.Iterable)
    True
    >>> isinstance(get_intermediate_paths_posix('g'), col.Iterable)
    True
    >>> isinstance(get_intermediate_paths_posix(''), col.Iterable)
    True

    >>> list(get_intermediate_paths_posix('a/b/c/d/e/f/g'))
    ['a', 'a/b', 'a/b/c', 'a/b/c/d', 'a/b/c/d/e', 'a/b/c/d/e/f']
    >>> list(get_intermediate_paths_posix('a/b'))
    ['a']
    >>> list(get_intermediate_paths_posix('a'))
    []
    >>> list(get_intermediate_paths_posix(''))
    []
    """
    # Do we have the directory at all?
    dirname = posixpath.dirname(leaf_path)
    return accumulate(dirname.split(posixpath.sep), posixpath.join) \
               if dirname \
               else iter([])
