#!/usr/bin/python
"""
@todo: we need os.readlink support on windows or somethink like this
        http://stackoverflow.com/questions/1447575/symlinks-on-windows
        https://github.com/juntalis/ntfslink-python
        https://github.com/sid0/ntfs
@todo: we had to implement windows .lnk support
    http://stackoverflow.com/questions/1447575/symlinks-on-windows
@todo: we had to implement OS X alias support
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import os
import stat
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from threading import RLock

from twisted.internet import reactor, error, threads
from twisted.python import filepath

from PySide import QtCore

from contrib.dbc import contract_epydoc

from common.utils import exceptions_logged, in_main_thread
from common.twisted_utils import callFromThread, callInThread, callLater

from host import system as host_system
from .abstract_fsnotify_manager import AbstractFSNotifyManager
from .events import CreateEvent, DeleteEvent, ModifyEvent



#
# Constants
#

logger = logging.getLogger(__name__)

# time for BufferedDeduplicationProxy to accumulate events before deduplication
# and forward.
EVENT_ACCUMULATION_TIME = timedelta(seconds=0.2)
MAX_EVENT_DELAY = EVENT_ACCUMULATION_TIME



#
# Classes
#

DirContents = col.namedtuple('DirContents', ['dirs', 'files', 'links'])
"""
Contains the C{frozenset}-s of the names of directories, files and links
inside some directory.
"""


class DirData(object):
    """
    @ivar dirs: set of dir paths. Contains C{basestring}-s.
    @ivar files: set of file paths. Contains C{basestring}-s.
    @ivar lock: a lock that prevents multiple simultaneous operations
        with the same C{DirData} object.
    """
    __slots__ = ('path', 'dirs', 'files', 'lock')


    def __init__(self, path, dirs, files):
        """Constructor.

        @type path: basestring
        @type dirs: col.Iterable
        @type files: col.Iterable
        """
        self.path = path
        self.dirs = set(dirs)
        self.files = set(files)
        self.lock = RLock()



class AbstractWatcher(object):
    """Abstract fs watcher class.

    @todo: must be made thread-safe and used from threadpool threads
        whenever possible!
    """
    __metaclass__ = ABCMeta

    # cannot use __slots__ here unfortunately


    def __init__(self, handlers=None):
        assert in_main_thread()
        self._handlers = []
        if handlers:
            self.add_handlers(handlers)


    @abstractmethod
    def watch_path(self, path):
        assert in_main_thread()
        assert isinstance(path, unicode), (path)


    @abstractmethod
    def unwatch_path(self, path):
        assert in_main_thread()
        assert isinstance(path, unicode), (path)


    def add_handler(self, handler):
        """handlers responsible for releasing reactor as soon as possible"""
        assert in_main_thread()
        self._handlers.append(handler)


    def remove_handler(self, handler):
        assert in_main_thread()
        self._handlers.remove(handler)


    def add_handlers(self, handlers):
        assert in_main_thread()
        for handler in handlers:
            self.add_handler(handler)


    def remove_handlers(self, handlers):
        assert in_main_thread()
        for handler in handlers:
            self.remove_handler(handler)


    def watch_paths(self, paths):
        """Start watching several paths.

        @type paths: col.Iterable
        """
        assert in_main_thread()
        for path in paths:
            self.watch_path(path)


    def unwatch_paths(self, paths):
        """Stop watching several paths.

        @type paths: col.Iterable
        """
        assert in_main_thread()
        for path in paths:
            self.unwatch_path(path)



class QtWatcher(AbstractWatcher):
    """Watcher implementation to work with QFileSystemWatcher.

    @todo: must be made thread-safe and be executable from any thread!
    """
    __slots__ = ('__watcher',)


    def __init__(self, handlers=None):
        super(QtWatcher, self).__init__(handlers=handlers)

        self.__watcher = QtCore.QFileSystemWatcher()
        self.__watcher.fileChanged.connect(self.__on_event)
        self.__watcher.directoryChanged.connect(self.__on_event)


    def watch_path(self, path):
        assert in_main_thread()
        super(QtWatcher, self).watch_path(path)
        logger.verbose('QtWatcher.watch_path(%r)', path)
        self.__watcher.addPath(path)


    def unwatch_path(self, path):
        assert in_main_thread()
        super(QtWatcher, self).unwatch_path(path)
        logger.verbose('QtWatcher.unwatch_path(%r)', path)
        self.__watcher.removePath(path)


    @exceptions_logged(logger)
    def __on_event(self, event):
        assert in_main_thread()
        for handler in self._handlers:
            handler(event)



class BufferedDeduplicationProxy(AbstractWatcher):
    """
    Buffers incoming events, after accum_time seconds removes duplicate
    events and forwards remaining.

    Produces events with delay less or equal to C{accum_time}.

    @todo: must be made thread-safe and be executable from any thread!
    """

    __slots__ = ('__watcher', '__accum_time', '__forward_now_events',
                 '__events_to_delay', '__forward_later_events')

    def __init__(self, watcher, accum_time, handlers=None):
        """Constructor.

        @type accum_time: timedelta
        """
        assert in_main_thread()
        super(BufferedDeduplicationProxy, self).__init__(handlers=handlers)
        assert isinstance(watcher, AbstractWatcher)

        self.__watcher = watcher
        self.__watcher.add_handler(self.__on_event)

        self.__accum_time = accum_time
        self.__forward_now_events = set()
        self.__events_to_delay = set()
        self.__forward_later_events = set()
        self.__relaunch_delayed_forward()


    def __relaunch_delayed_forward(self):
        assert in_main_thread()
        self._delayed_forward = \
            reactor.callLater(self.__accum_time.total_seconds(),
                              self._forward_events)


    def __on_events(self, events):
        for event in events:
            self.__on_event(event)


    def __on_event(self, event):
        assert in_main_thread()
        if event in self.__events_to_delay:
            self.__forward_later_events.add(event)
        else:
            self.__forward_now_events.add(event)
            self.__events_to_delay.add(event)
        self.do_forward_events()


    def do_forward_events(self):
        assert in_main_thread()
        events = self.__forward_now_events
        self.__forward_now_events = set()
        for event in events:
            self._forward_event(event)
        if not self._delayed_forward.active():
            self.__relaunch_delayed_forward()


    @exceptions_logged(logger)
    def _forward_events(self):
        assert in_main_thread()
        events = self.__forward_later_events
        self.__forward_later_events = set()
        self.__events_to_delay = set()
        for event in events:
            self._forward_event(event)


    def _forward_event(self, event):
        for handler in self._handlers:
            handler(event)


    def watch_path(self, path):
        super(BufferedDeduplicationProxy, self).watch_path(path)
        self.__watcher.watch_path(path)


    def unwatch_path(self, path):
        super(BufferedDeduplicationProxy, self).unwatch_path(path)
        self.__watcher.unwatch_path(path)



class FileSystemWatcher(QtCore.QObject):
    """I Transform events about change at path to CreateEvent,
    DeleteEvent or ModifyEvent.
    I produce events with delay less or equal to MAX_EVENT_DELAY.
    """

    def __init__(self):
        """Constructor.

        B{Must} be initialized in main thread, because of C{QFileSystemWatcher}
        requirements.
        """
        assert in_main_thread()
        super(FileSystemWatcher, self).__init__()
        self._path_to_callback = {}

        self._dirs = {}
        self._dirs_lock = RLock()

        self._fs_watcher = BufferedDeduplicationProxy(
                               QtWatcher(),
                               accum_time=EVENT_ACCUMULATION_TIME)
        self._fs_watcher.add_handler(self._on_path_changed)


    @exceptions_logged(logger)
    def _on_path_changed(self, path):
        try:
            mode = os.stat(path).st_mode
        except OSError:
            logger.debug('Path does not exist, %r', path)
            # try to unwatch this path as if it was dir..
            callInThread(self._unwatch_dir, path, True)
        else:
            logger.debug('Received event about path %r', path)
            if stat.S_ISDIR(mode):
                callInThread(self._handle_changes_in_dir, path)
            elif stat.S_ISREG(mode):
                callInThread(self._handle_changes_in_file, path)
            else:
                logger.debug('I do not know what to do with fh with mode %r',
                             mode)


    def _watch_dir(self, path, do_event_sending=False):
        """Internal implementation of starting watching a directory.

        Runs in threadpool thread only.
        """
        assert not in_main_thread()
        path = unicode(path)  # ensure that all paths are unicode
        # check if this path exists
        try:
            mode = os.stat(path).st_mode
        except OSError:
            logger.debug('Path does not exist, %r', path)
            return
        else:
            if host_system.is_link_to_dir(path):
                logger.warning('This watcher does not support links %r', path)
                return
            assert stat.S_ISDIR(mode), (path, mode)

        # check if this path already watched
        if path in self._dirs:
            logger.debug('Dir already added %s', path)
            return

        logger.verbose('Watch dir %r', path)

        dir_contents = self._get_dir_contents(path)
        dir_data = DirData(path, dir_contents.dirs, dir_contents.files)

        with self._dirs_lock:
            if path not in self._dirs:
                self._dirs[path] = dir_data
            else:
                return

        if dir_contents.files:
            logger.verbose('Watch files: %r', dir_contents.files)
        self._add_paths_to_watcher(dir_contents.files | {path})
        if do_event_sending:
            self._send_event(CreateEvent, path)
            for file_path in dir_contents.files:
                self._send_event(CreateEvent, file_path)

        for dir_path in dir_contents.dirs:
            self._watch_dir(dir_path, do_event_sending)


    def watch_dir(self, path, cb):
        """Start watching a directory.

        @type path: basestring
        @type cb: col.Callable
        """
        self._path_to_callback[path] = cb
        callInThread(self._watch_dir, path, do_event_sending=False)


    def _unwatch_dir(self, path, recursive=True):
        """Internal implementation of stopping watching a directory.

        Runs in threadpool thread only.
        """
        assert not in_main_thread()
        with self._dirs_lock:
            try:
                dir_data = self._dirs.pop(path)
            except KeyError:
                logger.debug('Unknown dir %r', path)
                return
        with dir_data.lock:
            self._remove_paths_from_watcher([path] + list(dir_data.files))
            dirs_to_unwatch = dir_data.dirs.copy() if recursive else []
            files_to_unwatch = dir_data.files.copy()
        for dir_path in dirs_to_unwatch:
            self._unwatch_dir(dir_path, True)
        for file_path in files_to_unwatch:
            self._send_event(DeleteEvent, file_path)
        self._send_event(DeleteEvent, path)


    def unwatch_dir(self, path, recursive=True):
        """Stop watching a directory.

        @type path: basestring
        @type cb: col.Callable
        """
        del self._path_to_callback[path]
        callInThread(self._unwatch_dir, path, recursive)


    @exceptions_logged(logger)
    def _handle_changes_in_file(self, path):
        assert not in_main_thread()
        try:
            mode = os.stat(path).st_mode
        except OSError:
            # file deletion will handled on directory changed signal
            return
        else:
            assert stat.S_ISREG(mode), (path, mode)

        self._send_event(ModifyEvent, path)


    @exceptions_logged(logger)
    def _handle_changes_in_dir(self, path):
        assert not in_main_thread()
        try:
            mode = os.stat(path).st_mode
        except OSError:
            self._unwatch_dir(path, True)
            self._send_event(DeleteEvent, path)
            return
        else:
            assert stat.S_ISDIR(mode), (path, mode)

        dir_contents = self._get_dir_contents(path)
        # @todo: handle links

        with self._dirs_lock:
            _dirs_copy = dict(self._dirs)  # to use in logging
            dir_data = self._dirs.get(path)

        if dir_data is None:
            logger.error('%r is not in dirs (%r)', path, _dirs_copy)
            del _dirs_copy
        else:
            with dir_data.lock:
                new_dirs = dir_contents.dirs - dir_data.dirs
                removed_dirs = dir_data.dirs - dir_contents.dirs
                new_files = dir_contents.files - dir_data.files
                removed_files = dir_data.files - dir_contents.files
                dir_data.dirs = dir_contents.dirs
                dir_data.files = dir_contents.files
                # Also, create the copies for outside-of-the-lock use
                inner_dirs = frozenset(dir_contents.dirs)
                inner_files = frozenset(dir_contents.files)

            if new_dirs or removed_dirs or new_files or removed_files:
                self._send_event(ModifyEvent, path)

            # this is requered, as file or dir can be removed, and then created
            # anew with same name ..
            self._remove_paths_from_watcher(inner_dirs | inner_files)
            self._add_paths_to_watcher(inner_dirs | inner_files)

            for file_path in new_files:
                self._send_event(CreateEvent, file_path)
            for file_path in removed_files:
                self._send_event(DeleteEvent, file_path)

            for dir_path in removed_dirs:
                self._unwatch_dir(dir_path, True)
            for dir_path in new_dirs:
                self._watch_dir(dir_path, do_event_sending=True)


    def _get_dir_contents(self, path):
        """Get the contents of a directory.

        @param path: the path to a directory to get the contents.
        @type path: basestring

        @returns: C{DirContents} with the sets of dirs, files and links
            inside a C{path}.
        @rtype: DirContents

        @raises Exception: if the contents of a directory cannot be read.
        """
        assert not in_main_thread()
        dirs = set()
        files = set()
        links = set()

        names = os.listdir(path)

        for name in names:
            pathname = os.path.join(path, name)
            try:
                mode = os.lstat(pathname).st_mode
            except OSError:
                continue  # the path promptly disappeared
            if stat.S_ISDIR(mode):
                # It's a directory, recurse into it
                dirs.add(pathname)
            elif stat.S_ISREG(mode):
                # It's a file
                files.add(pathname)
            elif stat.S_ISLNK(mode):
                # It's a symbolic link
                links.add(pathname)
            else:
                logger.debug('Skipping unknown fd mode type: %r, path: %r',
                             mode, pathname)

        # Converts the sets to frozensets, for security.
        return DirContents(dirs=frozenset(dirs),
                           files=frozenset(files),
                           links=frozenset(links))


    @exceptions_logged(logger)
    def _send_event(self, event_cls, path):
        # @todo: more real timestamp
        event = event_cls(filepath.FilePath(path),
                          datetime.utcnow() - MAX_EVENT_DELAY)
        logger.verbose('sending event: %r', event)
        for exp_path, callback in self._path_to_callback.items():
            if path == exp_path or path.startswith(os.path.join(exp_path, '')):
                callback(event)


    def _add_paths_to_watcher(self, paths):
        callFromThread(self._fs_watcher.watch_paths, paths)


    def _remove_paths_from_watcher(self, paths):
        """
        @type paths: col.Iterable
        """
        callFromThread(self._fs_watcher.unwatch_paths, paths)


    def _temporary_stop_watching(self, seconds, paths):
        """
        @type paths: col.Iterable
        """
        paths = list(paths)  # will be reused
        self._remove_paths_from_watcher(paths)
        callLater(seconds, self._add_paths_to_watcher, paths)



class Qt4FSNotifyManager(AbstractFSNotifyManager):
    """
    I produce events with some delay.
    """

    __slots__ = ('__notifier',)


    def __init__(self):
        self.__notifier = FileSystemWatcher()


    @contract_epydoc
    def watch(self, path, cb):
        """Add a directory to the watch list.

        @param path: the path to the directory to watch.
        @type path: basestring

        @param cb: a callback to call when an FS even in a directory C{path}
            happens.
        @type cb: col.Callable
        """
        self.__notifier.watch_dir(path, cb)
