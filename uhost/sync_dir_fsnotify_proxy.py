#!/usr/bin/python
"""
This is a proxy between the synced directory and the FS notify manager(s)
to support the symlinks at the top of the directory (but no lower than
at the top).
"""

#
# Imports
#

from __future__ import absolute_import
from collections import namedtuple
from datetime import timedelta
from functools import partial
from itertools import chain
import collections as col
import logging
import os
import stat

from twisted.application.internet import TimerService
from twisted.internet import threads
from twisted.python import filepath

from contrib.dbc import contract_epydoc

from common.utils import exceptions_logged, in_main_thread, reprefix
from common.twisted_utils import callInThread

from host import system as host_system

from .fs_notify.abstract_fsnotify_manager import AbstractFSNotifyManager
from .fs_notify.events import CreateEvent, MoveEvent, Event



#
# Constants
#

logger = logging.getLogger(__name__)

POLL_SYNC_DIR_FOR_SYMLINKS_PERIOD = timedelta(seconds=20)



#
# Functions
#

def _get_dir_content_recursively(path):
    """ignores dir links"""
    dirs = set()
    files = set()

    names = os.listdir(path)

    for name in names:
        pathname = os.path.join(path, name)
        try:
            mode = os.stat(pathname).st_mode
        except os.error:
            logger.debug(u'Path disappeared: %s', pathname)
            continue  # the path promptly disappeared
        if stat.S_ISDIR(mode):
            if not host_system.is_link_to_dir(pathname):
                inner_dirs, inner_files = \
                    _get_dir_content_recursively(pathname)
                dirs.update(inner_dirs, [pathname])
                files.update(inner_files)
        elif stat.S_ISREG(mode):
            files.add(pathname)
        else:
            logger.debug(u'Skipping unknown fd mode type: %s, path: %s',
                         mode, pathname)
    return dirs, files



#
# Classes
#

LinksMapPair = namedtuple('LinksMapPair', ['links', 'reverse_links'])

SymlinkInfo = namedtuple('SymlinkInfo', ['base_dir', 'linked_path'])
"""
base_dir: in which base directory (if there are multiple ones) this symlink
    is stored?
linked_path: to what path does this link refers?
"""


class SyncDirFSNotifyProxy(AbstractFSNotifyManager):
    """
    This proxy tracks the top-level directory, the symlinks in it (note:
    in top-level directory only!), and remaps events occuring in the symlinked
    directory to the final path, and vice versa.
    It uses a different, OS-specific implementation of the FS Notify Manager,
    to track

    @type __inner_fsnotify_manager: AbstractFSNotifyManager

    @ivar __watched_dir_to_cb: mapping from basestring to Callbable
    @ivar __implicitly_watched_dirs: set of basestring

    # @ivar __watched_dir_to_cb: mapping from basestring to WatchInfo
    # @ivar __implicitly_watched_dirs: mapping from basestring to WatchInfo

    @ivar __links_map_pair: a pair of:
        1. C{col.Mapping} - the mapping from the symlink source (inside some
           of the watched dirs) to the information about the symlink
           (C{SymlinkInfo}).
        2. C{col.Mapping} -  the mapping from the symlink destination
           to the set of symlink sources.
    @type __links_map_pair: LinksMapPair
    """

    __slots__ = ('__inner_fsnotify_manager',
                 '__watched_dir_to_cb', '__implicitly_watched_dirs',
                 '__poll_sync_dir_timer',
                 '__links_map_pair')


    @contract_epydoc
    def __init__(self, fsnotify_manager):
        """Constructor.

        @type fsnotify_manager: AbstractFSNotifyManager
        """
        self.__inner_fsnotify_manager = fsnotify_manager
        self.__watched_dir_to_cb = {}  # mapping from path to callback
        self.__implicitly_watched_dirs = set()
        self.__poll_sync_dir_timer = \
            TimerService(POLL_SYNC_DIR_FOR_SYMLINKS_PERIOD.total_seconds(),
                         lambda: callInThread(
                                     self.__on_poll_sync_dir_timer))
        self.__links_map_pair = LinksMapPair(links={}, reverse_links={})


    def watch(self, path, cb):
        """Start watching for FS changes in C{path} explicitly."""
        self.__watch(path, cb, do_start_timer=True)


    def __watch(self, path, cb, do_start_timer=False):
        """All internal operations to start watching a directory for changes.
        @note: if C{do_start_timer=True}, it automatically starts the
            sync dir poll timer on the very first attempt to watch a dir.
        @type do_start_timer: bool
        """
        cls = self.__class__

        if path in self.__watched_dir_to_cb:
            logger.warning(u'Path already watched: %r', path)
            return
        # Start ("explicitly") watch for the path itself.
        logger.debug(u'Starting to (expl.) watch for %r with %r', path, cb)
        self.__watched_dir_to_cb[path] = cb
        # remapped_cb = self._remapping_path_wrapper(path, cb)

        # Explicitly watched paths have their cb's passed without alteration.
        self.__inner_fsnotify_manager.watch(path, cb)

        # Maps full path of symlink to SymlinkInfo,..
        links_in_this_path_pair = self.__rescan_watched_dir_for_map(path)
        assert isinstance(links_in_this_path_pair, LinksMapPair), \
               repr(links_in_this_path_pair)

        cls._inmerge_link_map_pair(into=self.__links_map_pair,
                                   merge=links_in_this_path_pair)

        # ... and start ("implicitly") watch for every symlink
        # (its target, actually)
        for link, linkinfo in links_in_this_path_pair.links.iteritems():
            assert path == linkinfo.base_dir, \
                   (path, linkinfo.base_dir)
            logger.debug(u'Starting to (impl.) watch %r in %r with %r',
                         linkinfo.linked_path, path, cb)
            self.__implicitly_watched_dirs.add(linkinfo.linked_path)
            # For every watched symlink, we call our own cb.
            self.__inner_fsnotify_manager.watch(
                linkinfo.linked_path,
                partial(self.__cb_with_symlink_reversing,
                        original_cb=cb,
                        linked_base_dir=linkinfo.linked_path))

        # The service starts the calls when it starts.
        # self.__links_map_pair should already be initialized before starting
        # this service!
        if not self.__poll_sync_dir_timer.running and do_start_timer:
            logger.debug(u'Started to poll watched directories (%r)',
                         self.__watched_dir_to_cb.keys())
            self.__poll_sync_dir_timer.startService()


    @contract_epydoc
    def __cb_with_symlink_reversing(self, event, original_cb, linked_base_dir):
        r"""
        >>> from uhost.fs_notify.events import \
        ...     CreateEvent, DeleteEvent, MoveEvent, Event
        >>> src_path = '/mnt/net/homes/john/hello'
        >>> dst_path = '/mnt/net/homes/john/goodby'
        >>> create_event = CreateEvent(filepath.FilePath(src_path))
        >>> move_event = MoveEvent(filepath.FilePath(src_path),
        ...                        filepath.FilePath(dst_path))
        >>> delete_event = DeleteEvent(filepath.FilePath(dst_path))

        >>> def printer(event):
        ...     print event

        >>> class DummySyncDirFSNotifyProxy(SyncDirFSNotifyProxy):
        ...    def __init__(self):
        ...        self._SyncDirFSNotifyProxy__links_map_pair =\
        ...            LinksMapPair(links={
        ...                             '/home/john':
        ...                                 SymlinkInfo(
        ...                                     base_dir='/home',
        ...                                     linked_path=
        ...                                         '/mnt/net/homes/john')},
        ...                         reverse_links={
        ...                             '/mnt/net/homes/john': {'/home/john'}}
        ...                         )
        ...    def cb_with_symlink_reversing(self, event, original_cb,
        ...                                  linked_base_dir):
        ...        self._SyncDirFSNotifyProxy__cb_with_symlink_reversing(
        ...            event, original_cb, linked_base_dir)

        >>> d = DummySyncDirFSNotifyProxy()

        >>> d.cb_with_symlink_reversing(create_event, printer,
        ...     '/mnt/net/homes/john')  # doctest: +ELLIPSIS
        CreateEvent(path=FilePath('/home/john/hello'), ts=...)

        >>> d.cb_with_symlink_reversing(
        ... move_event, printer, '/mnt/net/homes/john'
        ... ) # doctest:+NORMALIZE_WHITESPACE +ELLIPSIS
        MoveEvent(from_path=FilePath('/home/john/hello'),
                  to_path=FilePath('/home/john/goodby'),
                  ts=...)

        >>> d.cb_with_symlink_reversing(
        ... delete_event, printer, '/mnt/net/homes/john'
        ... )  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        DeleteEvent(path=FilePath('/home/john/goodby'),
                    ts=...)

        @type original_cb: col.Callable
        @type linked_base_dir: basestring
        @type event: Event
        """
        try:
            link_sources = self.__links_map_pair.reverse_links[linked_base_dir]
        except:
            logger.exception(u"Seems like we don't watch for %r already",
                             linked_base_dir)
        else:
            for link_source in link_sources:
                substituted_path = reprefix(event.path.path,
                                            old_prefix=linked_base_dir,
                                            new_prefix=link_source)

                if isinstance(event, MoveEvent):
                    substituted_to_path = reprefix(event.to_path.path,
                                                   old_prefix=linked_base_dir,
                                                   new_prefix=link_source)
                new_event = event.copy()
                new_event.path = filepath.FilePath(substituted_path)
                if isinstance(event, MoveEvent):
                    new_event.to_path = filepath.FilePath(substituted_to_path)

                logger.debug(u'Calling cb while substituting %r to %r: %r',
                             linked_base_dir, link_source, event)

                original_cb(new_event)


    @staticmethod
    def _inmerge_link_map_pair(into, merge):
        r"""
        Merge to C{merge} pair into C{into} pair (attention,
        it is modified inline!)

        >>> a1 = LinksMapPair(links={
        ...                       '/etc/networking':
        ...                           SymlinkInfo(
        ...                               base_dir='/etc',
        ...                               linked_path='/mnt/net/etc/net'),
        ...                       '/etc/networking-copy':
        ...                           SymlinkInfo(
        ...                               base_dir='/etc',
        ...                               linked_path='/mnt/net/etc/net'),
        ...                       '/home/john':
        ...                           SymlinkInfo(
        ...                               base_dir='/home',
        ...                               linked_path='/mnt/net/homes/john'),
        ...                   },
        ...                   reverse_links={
        ...                       '/mnt/net/etc/net': {'/etc/networking',
        ...                                            '/etc/networking-copy'},
        ...                       '/mnt/net/homes/john': {'/home/john'},
        ...                   })
        >>> a2 = LinksMapPair(links={
        ...                       '/etc/samba':
        ...                           SymlinkInfo(
        ...                               base_dir='/etc',
        ...                               linked_path='/mnt/net/etc/smb'),
        ...                       '/etc/networking-copy2':
        ...                           SymlinkInfo(
        ...                               base_dir='/etc',
        ...                               linked_path='/mnt/net/etc/net'),
        ...                       '/home/alex':
        ...                           SymlinkInfo(
        ...                               base_dir='/home',
        ...                               linked_path='/mnt/net/homes/alex'),
        ...                   },
        ...                   reverse_links={
        ...                       '/mnt/net/etc/smb':
        ...                           {'/etc/samba'},
        ...                       '/mnt/net/etc/net':
        ...                           {'/etc/networking-copy2'},
        ...                       '/mnt/net/homes/alex':
        ...                           {'/home/alex'},
        ...                   })
        >>> SyncDirFSNotifyProxy._inmerge_link_map_pair(into=a2, merge=a1)
        >>> a1  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        LinksMapPair(links={'/home/john':
                                SymlinkInfo(base_dir='/home',
                                            linked_path='/mnt/net/homes/john'),
                            '/etc/networking-copy':
                                SymlinkInfo(base_dir='/etc',
                                            linked_path='/mnt/net/etc/net'),
                            '/etc/networking':
                                SymlinkInfo(base_dir='/etc',
                                            linked_path='/mnt/net/etc/net')},
            reverse_links={'/mnt/net/etc/net':
                              set(['/etc/networking-copy', '/etc/networking']),
                           '/mnt/net/homes/john':
                               set(['/home/john'])})
        >>> a2  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        LinksMapPair(links={'/home/john':
                                SymlinkInfo(base_dir='/home',
                                            linked_path='/mnt/net/homes/john'),
                            '/etc/networking':
                                SymlinkInfo(base_dir='/etc',
                                            linked_path='/mnt/net/etc/net'),
                            '/etc/samba':
                                SymlinkInfo(base_dir='/etc',
                                            linked_path='/mnt/net/etc/smb'),
                            '/etc/networking-copy':
                                SymlinkInfo(base_dir='/etc',
                                            linked_path='/mnt/net/etc/net'),
                            '/home/alex':
                                SymlinkInfo(base_dir='/home',
                                            linked_path='/mnt/net/homes/alex'),
                            '/etc/networking-copy2':
                                SymlinkInfo(base_dir='/etc',
                                            linked_path='/mnt/net/etc/net')},
            reverse_links={'/mnt/net/etc/net':
                               set(['/etc/networking-copy',
                                    '/etc/networking',
                                    '/etc/networking-copy2']),
                           '/mnt/net/homes/john':
                               set(['/home/john']),
                           '/mnt/net/etc/smb':
                               set(['/etc/samba']),
                           '/mnt/net/homes/alex':
                               set(['/home/alex'])})

        @param into: target pair to merge the data (C{merge} arg) into.
        @type into: LinksMapPair

        @param merge: the pair being merged into C{into} pair.
        @type merge: LinksMapPair
        """
        into.links.update(merge.links)
        # Merging new_rev_links_map is a bit more complex.
        for k, v in merge.reverse_links.iteritems():
            into.reverse_links.setdefault(k, set()).update(v)


    @exceptions_logged(logger)
    def __on_poll_sync_dir_timer(self):
        cls = self.__class__
        assert not in_main_thread()

        logger.debug(u'Polling watched directories (%r)',
                     self.__watched_dir_to_cb.keys())

        # Loop over the watched directories, recreate the symlink map
        new_links_map_pair = LinksMapPair({}, {})
        for watched_dir in self.__watched_dir_to_cb.iterkeys():
            links_map_pair_for_watched_dir = \
                self.__rescan_watched_dir_for_map(watched_dir)

            cls._inmerge_link_map_pair(into=new_links_map_pair,
                                       merge=links_map_pair_for_watched_dir)

        # Update the map with the total result
        logger.debug(u'The total symlink map is %r', new_links_map_pair)
        links_set_old = set(self.__links_map_pair.links)
        links_set_new = set(new_links_map_pair.links)
        removed_links = links_set_old.difference(links_set_new)
        created_links = links_set_new.difference(links_set_old)
        self.__links_map_pair = new_links_map_pair

        if created_links:
            logger.debug(u'New links: %r', created_links)

        for link in created_links:
            linkinfo = new_links_map_pair.links[link]
            cb = self.__watched_dir_to_cb[linkinfo.base_dir]
            self.__implicitly_watched_dirs.add(linkinfo.linked_path)
            dirs, files = _get_dir_content_recursively(link)
            for path in chain(dirs, files, [linkinfo.linked_path]):
                cb(CreateEvent(filepath.FilePath(path)))
            self.__inner_fsnotify_manager.watch(
                linkinfo.linked_path,
                partial(self.__cb_with_symlink_reversing,
                        original_cb=cb,
                        linked_base_dir=linkinfo.linked_path))


        if removed_links:
            logger.warning(u'These links were removed,'
                               u' but original path maybe still watched %r,'
                               u' unwatching is not implemented.',
                           removed_links)

        #@todo: TODO: handle removed_links



    def __rescan_watched_dir_for_map(self, watched_dir):
        """
        Rescan anew the given directory and get the bidirectional mapping
        of all the found symlinks.

        @rtype: LinksMapPair
        """
        pair = LinksMapPair({}, {})

        logger.debug(u'Rescanning %r for symlinks', watched_dir)
        try:
            # Make sure the result is in Unicode
            dir_contents = [os.path.join(watched_dir, rel)
                                for rel in os.listdir(unicode(watched_dir))]
        except:
            logger.exception(u'Cannot list %r', watched_dir)
        else:
            # For every item inside the directory, check whether it is
            # a symlink/junction point
            for item in dir_contents:
                try:
                    if host_system.is_link_to_dir(item):
                        dest = os.path.abspath(
                                   host_system.read_link_to_dir(item))

                        logger.debug(u'Found %r -> %r', item, dest)

                        SyncDirFSNotifyProxy._inmerge_link_map_pair(
                            into=pair,
                            merge=LinksMapPair(
                                      links={item: SymlinkInfo(
                                                       base_dir=watched_dir,
                                                       linked_path=dest)},
                                      reverse_links={dest: {item}}))

                except:
                    logger.exception(u'Cannot test %r in %r',
                                     item, watched_dir)

        return pair
