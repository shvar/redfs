#!/usr/bin/python
"""
Various low-level system-specific hacks, common for all system components.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import numbers
import os
import sys
import platform
import subprocess
from types import NoneType

from contrib.dbc import contract_epydoc

if sys.platform == 'win32':
    # for get_free_space_at_path
    import win32api  # pylint:disable=F0401

elif sys.platform.startswith('linux'):
    from ctypes import CDLL  # for __fallocate_posix_fallocate

from .utils import open_wb



#
# Constants
#

logger = logging.getLogger(__name__)

SUPPORTED_REACTOR_NAMES = ('epoll', 'kqueue', 'qt4', 'win32', 'zmq')



#
# Functions
#

@contract_epydoc
def __install_named_reactor(reactor_name, reactor_specific=None):
    """Setup a proper Twisted reactor, given its name.

    @precondition: reactor_name in SUPPORTED_REACTOR_NAMES

    @param reactor_specific: some arguments which are specific
        for a particular reactor (a bit hacky, yes).
        If C{reactor_name} == 'qt4': contains particular C{QCoreApplication}
            subclass to be used for primary application creation.
            May be C{None}.

    @raises ImportError: if the reactor cannot be installed.
    @raises NotImplementedError: if such reactor name is unsupported.
    """
    logger.debug('Attempting to install %s reactor...', reactor_name)

    if reactor_name == 'epoll':
        from twisted.internet import epollreactor
        epollreactor.install()

    elif reactor_name == 'kqueue':
        from txkqr import kqreactor_ex
        kqreactor_ex.install()

    elif reactor_name == 'qt4':
        qapp_class = reactor_specific
        if qapp_class is not None:
            app = qapp_class(sys.argv)

        from contrib.qt4reactor import qt4reactor
        qt4reactor.install()

    elif reactor_name == 'win32':
        from twisted.internet import win32eventreactor
        win32eventreactor.install()

    elif reactor_name == 'zmq':
        from zmqr import zmqreactor
        zmqreactor.install()

    else:
        logger.debug('Cannot install %s reactor, using default', reactor_name)
        raise NotImplementedError('Unsupported reactor {!r}'
                                      .format(reactor_name))

    logger.debug('Installed %s reactor', reactor_name)


@contract_epydoc
def install_reactor(force_reactor_name=None,
                    threadpool_size=None,
                    reactor_specific=None):
    """
    Guess and setup the proper Twisted reactor,
    depending upon the operating system.

    @param force_reactor_name: use this reactor. If C{None}, no forcing.
    @type force_reactor_name: basestring, NoneType
    @precondition: force_reactor_name in SUPPORTED_REACTOR_NAMES

    @param threadpool_size: suggest this size of reactor threadpool.

    @param reactor_specific: some arguments which are specific
        for a particular reactor (a bit hacky, yes).
        See C{__install_named_reactor()} for details.
    """
    assert 'twisted.internet.reactor' not in sys.modules

    if force_reactor_name:
        __install_named_reactor(force_reactor_name,
                                reactor_specific=reactor_specific)
    else:
        # We can customize the reactor
        if sys.platform == 'win32':
            # Under Windows, always use Win32-specific reactor
            __install_named_reactor(
                'win32', reactor_specific=reactor_specific)

        elif sys.platform.startswith('linux'):
            # Under Linux, use special zmq-reactor if python-zmq is installed,
            # otherwise use epoll()-reactor.
            try:
                # __install_named_reactor(
                #     'zmq', reactor_specific=reactor_specific)
                raise NotImplementedError()
            except (ImportError, NotImplementedError):
                __install_named_reactor(
                    'epoll', reactor_specific=reactor_specific)

        elif sys.platform == 'darwin':
            # For some time, let's qt4 be the default reactor on Darwin.
            # __install_named_reactor('qt4', reactor_specific=reactor_specific)
            try:
                __install_named_reactor(
                    'qt4', reactor_specific=reactor_specific)
            except ImportError:
                logger.debug('Using the default reactor due to impossibility '
                             'to import qt4-reactor')

        else:
            # Use default reactor, likely select()-based one
            logger.debug('Using the default reactor')

    if threadpool_size is not None:
        from twisted.internet import reactor
        logger.debug('Suggesting threadpool size of %i', threadpool_size)
        # pylint:disable=E1101,C0103
        _suggestThreadPoolSize = reactor.suggestThreadPoolSize
        # pylint:enable=E1101,C0103
        _suggestThreadPoolSize(threadpool_size)


def get_free_space_at_path(path):
    """
    Given a path on the file system, return the (best approximation of the)
    free space at the filesystem mounted at that path.

    @param path: The path which filesystem needs to be checked for free space.

    @rtype: numbers.Integral

    >>> # Just validate the function being called.
    >>> bool(get_free_space_at_path('.'))
    True
    """
    path = os.path.abspath(path)
    # We have a path to test for free space; but if the path itself does not
    # exist yet, we should browse higher and higher from that path
    # until we find out the path that exists

    if not os.path.exists(path):
        parent = os.path.dirname(path)
        if parent == path:
            # Oopsie, cannot browse to the parent anymore.
            return 0
        else:
            return get_free_space_at_path(parent)
    else:
        if sys.platform == 'win32':
            n_free_user, n_total, n_free = win32api.GetDiskFreeSpaceEx(path)
            return n_free_user
        elif sys.platform.startswith('linux') or sys.platform == 'darwin':
            _statvfs = os.statvfs(path)
            return _statvfs.f_frsize * _statvfs.f_bavail
        else:
            raise NotImplementedError(sys.platform)


def __fallocate_seek_write(path, size):
    """
    Implementation of fallocate() function:
    create a sparse file using seek() and write().
    """
    with open_wb(path) as fh:
        fh.seek(size - 1)
        fh.write('\x00')


def __fallocate_random(path, size):
    """
    Implementation of fallocate() function:
    create a sparse file writing the required amount of random bytes.
    """
    with open_wb(path) as fh:
        fh.write(os.urandom(size))


if sys.platform.startswith('linux'):
    def __fallocate_posix_fallocate(path, size):
        """
        Implementation of fallocate() function:
        create a sparse file using posix_fallocate() libc function.
        """
        libc = CDLL('libc.so.6')
        with open_wb(path) as fh:
            libc.posix_fallocate(fh.fileno(), 0, size)


def fallocate(path, size):
    """
    Preallocate a file on the FS, given the path and the desired size
    of the file.
    The allocation is performed in the best possible OS-specific way;
    in non-debug mode, the file is allocated to actually occupy
    the desired size;
    in debug mode, the file is allocated as a sparse file and may not
    occupy all the size.

    @param filename: The path to the file which should be allocated.
    @type filename: basestring

    @param size: The size of the file to allocate.
    @type size: numbers.Integral
    """
    if __debug__:
        __fallocate_seek_write(path, size)
    else:
        if sys.platform.startswith('linux') or sys.platform == 'darwin':
            __fallocate_posix_fallocate(path, size)
        elif sys.platform == 'win32':
            __fallocate_seek_write(path, size)
        else:
            raise NotImplementedError(sys.platform)


def get_install_file_ext():
    """
    Return the expected extension of installation file.
    """
    if sys.platform == 'win32':
        return 'exe'
    elif sys.platform.startswith('linux'):
        return 'deb'
    elif sys.platform == 'darwin':
        return 'dmg'
    else:
        raise NotImplementedError(sys.platform)


def linux_force_get_olson_tz_name():
    assert sys.platform.startswith('linux'), NotImplementedError(sys.platform)
    with open('/etc/timezone', 'r') as fh:
        return fh.read().strip()


def spawn_separate_process_nowait(*args, **kwargs):
    if platform.system() == 'Windows':
        # from http://msdn.microsoft.com/en-us/library/windows/desktop/ms684863%28v=vs.85%29.aspx
        CREATE_NEW_PROCESS_GROUP = 0x00000200  # note: could get it from subprocess
        DETACHED_PROCESS = 0x00000008          # 0x8 | 0x200 == 0x208
        kwargs.update(creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
    elif sys.version_info < (3, 2):  # assume posix
        kwargs.update(preexec_fn=os.setsid)
    else:  # Python 3.2+ and Unix
        kwargs.update(start_new_session=True)

    p = subprocess.Popen(*args,
                         shell=True,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         **kwargs)
    assert not p.poll()
    return p


#
# Main
#

if __name__ == '__main__':
    print(get_free_space_at_path('.'))
