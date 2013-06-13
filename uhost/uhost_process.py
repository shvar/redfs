#!/usr/bin/python
"""
The main process running for an (Untrusted) Host instance.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import os
import stat
import sys
from datetime import datetime, timedelta
from functools import partial
from itertools import imap, groupby, chain
from operator import attrgetter, itemgetter
from os.path import isdir
from threading import Lock, RLock
from types import NoneType

from twisted.application.internet import TimerService
from twisted.internet import error as internet_error, reactor, threads
from twisted.python.filepath import FilePath

from contrib.dbc import contract_epydoc

from common import os_ex
from common.chunks import LocalPhysicalFileStateRel
from common.datasets import DatasetOnPhysicalFiles
from common.inhabitants import UserGroup
from common.itertools_ex import inonempty, sorted_groupby
from common.path_ex import relpath_nodot
from common.typed_uuids import DatasetUUID
from common.types import EventfulDict
from common.twisted_utils import (
    callFromThread, callInThread, DelayServiceForCallback)
from common.utils import coalesce, exceptions_logged, gen_uuid, in_main_thread
from common.vfs import RelVirtualFile

from host import db, settings as host_settings
from host.db import HostQueries
from host.host_process import (
    AFTER_REACTOR_START, AUTO_START_BACKUPS_AFTER, HostApp)

from . import ux
from .fs_notify import events as fs_events, FSNotifyManager
from .sync_dir_fsnotify_proxy import SyncDirFSNotifyProxy



#
# Constants
#

logger = logging.getLogger(__name__)
logger_status_cooling_down_to_store = \
    logging.getLogger('status.cooling_down_to_store')
logger_status_network_connection = \
    logging.getLogger('status.network_connection')

START_SYNCING_AFTER = timedelta(seconds=5) - AFTER_REACTOR_START
"""
After launching the HostApp main loop, how much time must pass
before checking the tracked directory for changes and, probably, start
a backup?

5 seconds is hopefully long enough (for now) so it doesn't start before
all pending datasets are started; and short enough for
not to be annoying for the user.
"""
assert START_SYNCING_AFTER >= \
           AUTO_START_BACKUPS_AFTER + timedelta(seconds=1), \
       (START_SYNCING_AFTER, AUTO_START_BACKUPS_AFTER)
"""
If backups are auto-started later than syncing, then they'll auto-start
to backup the dataset which was just created by the sync.
"""

# How old should be the latest change, to consider that the file is not
# mass-edited and its state can be written into the DB?
try:
    from host_magic import FILE_COOL_DOWN_TO_STORE  # pylint:disable=F0401
except ImportError:
    FILE_COOL_DOWN_TO_STORE = timedelta(seconds=10)

# What is the delay between the attempts to write multiple file states
# to the DB?
try:
    # pylint:disable=F0401
    from host_magic import FILE_BUNCH_COOL_DOWN_TO_STORE
    # pylint:enable=F0401
except ImportError:
    FILE_BUNCH_COOL_DOWN_TO_STORE = timedelta(seconds=3)

# How old should be the latest change, to consider that the file editing
# is completed and it can be backed up?
try:
    from host_magic import FILE_COOL_DOWN_TO_BACKUP  # pylint:disable=F0401
except ImportError:
    FILE_COOL_DOWN_TO_BACKUP = \
        timedelta(minutes=1) \
        - (FILE_COOL_DOWN_TO_STORE + FILE_BUNCH_COOL_DOWN_TO_STORE)

# How often an attempt to create dataset is performed
try:
    from host_magic import CREATE_DATASET_PERIOD  # pylint:disable=F0401
except ImportError:
    CREATE_DATASET_PERIOD = timedelta(minutes=1)

PUBLISH_HOST_STATE_PERIOD = timedelta(seconds=3)


_TMP_ENABLE_DUMMY_DATA_TRANSFER_VENTILATOR = False



#
# Classes
#

class UHostApp(HostApp):
    """Untrusted Host application.

    @param __fsnotify_manager: the manager of FS change notificadtions.
    @type __fsnotify_manager: AbstractFSNotifyManager

    @ivar __cooling_down_to_store: the mapping of the files which are cooling
        down after some changes has been done, before their new state
        is written into the DB.
        The key of the mapping is the file path; the value is
        the C{IDelayedCall} object to write the state.
    @type __cooling_down_to_store: col.MutableMapping

    @ivar __ignore_file_paths: the mapping of path -> ts where:
        path - path to not to backup (currently restoring)
        ts - ignore all events happened before this ts.
    @type __ignore_file_paths: col.Mapping
    """

    __slots__ = ('__fsnotify_manager', '__syncer_starter',
                 '__cooling_down_to_store', '__cooling_down_to_store_lock',
                 '__ignore_file_paths', '__ignore_file_paths_lock',
                 '__file_states_ready_to_write',
                 '__file_states_ready_to_write_lock',
                 '__network_connection_is_working', '__all_timer_services',)


    def __init__(self, *args, **kwargs):
        """Constructor."""
        super(UHostApp, self).__init__(*args, **kwargs)

        # The manager that just tracks the FS
        __internal_fsnotify_manager = FSNotifyManager()
        # The manager that supports the symlinks on toplevel.
        self.__fsnotify_manager = \
            SyncDirFSNotifyProxy(fsnotify_manager=__internal_fsnotify_manager)

        self.__syncer_starter = None
        self.auto_start_sync = True

        on_non_empty_cooling_down_to_store = \
            lambda: logger_status_cooling_down_to_store.info(
                        'Some files cooled down to store recently',
                         extra={'status': True})
        on_empty_cooling_down_to_store = \
            lambda: logger_status_cooling_down_to_store.info(
                        'No more files cooling down to store',
                        extra={'status': False})
        self.__cooling_down_to_store = \
            EventfulDict(_on_non_empty_cb=on_non_empty_cooling_down_to_store,
                         _on_empty_cb=on_empty_cooling_down_to_store)
        self.__cooling_down_to_store_lock = RLock()

        self.__file_states_ready_to_write = {}
        self.__file_states_ready_to_write_lock = Lock()

        self.__ignore_file_paths = dict()
        self.__ignore_file_paths_lock = RLock()

        self.ux = ux.UX()
        ux_handlers = [ux.RestoreFSM(self.ux.handle_event),
                       ux.BackupFSM(self.ux.handle_event),
                       ux.SyncFSM(self.ux.handle_event),
                       ux.IdleFSM(self.ux.handle_event),
                       ux.NetworkConnectionFSM(self.ux.handle_event),
                       ux.OccupationHandler(self.ux.handle_event),
                       ux.UXEventForwarder()]
        for ux_handler in ux_handlers:
            self.ux.add_handler(ux_handler)

        self.__network_connection_is_working = True

        logger.debug('Will attempt to create a dataset once every %s',
                     CREATE_DATASET_PERIOD)
        logger.debug('Will publish connection state every %s',
                     PUBLISH_HOST_STATE_PERIOD)
        self.__all_timer_services = [
            # We should not create a dataset until the previous attempt
            # of dataset creation has been completed.
            # Thus DelayServiceForCallback rather than TimerService.
            DelayServiceForCallback(period=CREATE_DATASET_PERIOD,
                                    callback=self.__on_create_dataset_timer),
            TimerService(PUBLISH_HOST_STATE_PERIOD.total_seconds(),
                         exceptions_logged(logger)(self.__publish_host_state))
        ]


    def __repr__(self):
        return u'<UHostApp for {!r}>'.format(self.host)


    def _when_starting_normal_mode(self):
        """Overrides method from C{HostApp}."""
        assert in_main_thread()

        super(UHostApp, self)._when_starting_normal_mode()

        _callLater = reactor.callLater  # pylint:disable=E1101,C0103

        self.__syncer_starter = \
            _callLater(START_SYNCING_AFTER.total_seconds(),
                       lambda: callInThread(self.__start_syncing))


    @exceptions_logged(logger)
    def start(self):
        """Overrides the method from C{HostApp}."""
        super(UHostApp, self).start()

        for timer_service in self.__all_timer_services:
            timer_service.startService()


    @exceptions_logged(logger)
    def _stop(self):
        """Overrides the method from C{HostApp}."""
        assert in_main_thread()

        super(UHostApp, self)._stop()

        logger.debug('Untrusted Host process: stopping...')

        for timer_service in self.__all_timer_services:
            timer_service.stopService()

        if self.__syncer_starter is not None \
                and self.__syncer_starter.active():
            # We are stopping the app earlier than we had a chance to start
            # syncing. We should not occasionally start syncing after
            # the app has been stopped!
            self.__syncer_starter.cancel()


    @exceptions_logged(logger)
    def __start_syncing(self):
        """
        Scan the watched directory for the changes happened offline,
        and launch the online dir watcher
        """
        assert not in_main_thread()
        cls = self.__class__
        logger.debug('Starting syncing...')

        if __debug__ and _TMP_ENABLE_DUMMY_DATA_TRANSFER_VENTILATOR:
            from .mocks import _start_dummy_data_transfer_ventilator
            _start_dummy_data_transfer_ventilator()

        watched_directories = host_settings.get_selected_paths()

        if watched_directories is None:
            logger.debug('No paths to backup setting, not syncing')
        else:
            _watched_dirs_len = len(watched_directories)
            if _watched_dirs_len != 1:
                logger.error('%i directories to be synced instead of 1, '
                                 'not syncing anything then: %s',
                             _watched_dirs_len, watched_directories)
            else:
                _ugroup_uuid = self.host.user.base_group.uuid

                watched_dir = watched_directories.iterkeys().next()
                with db.RDB() as rdbw:
                    base_dir_id = \
                        HostQueries.HostFiles.add_or_get_base_directory(
                            watched_dir, _ugroup_uuid, rdbw)

                logger.debug('Checking the directory %s for offline changes',
                             watched_dir)
                cls.take_base_directory_snapshot(watched_dir, _ugroup_uuid)
                logger.debug('Snapshot taken!')

                # We are here after all preparations has been done (and maybe,
                # even the backup was started for the files which were changed
                # offline before the host restart).
                #
                # At this point, we are going to start the directory tracker.
                logger.debug('Enabling the FS notification watcher for %r',
                             watched_dir)

                callFromThread(self.__fsnotify_manager.watch,
                                watched_dir,
                                partial(self.__on_fs_change,
                                        watched_dir, base_dir_id))
                callFromThread(self.__do_next_iteration_of_file_state_bunch)


    def ignore_fs_events_for_path(self, path, before_ts=datetime.max):
        """Ignore FS notification events for some path.

        @type path: basestring
        @param before_ts: the FS events (for C{path}) before this timestamp
            should be ignored.
        @type before_ts: datetime
        """
        __path_to_ignore = path.lower() if sys.platform in ['darwin', 'win32']\
                                        else path

        # Even inserting is under the lock, otherwise the isolation level
        # is just "read committed".
        with self.__ignore_file_paths_lock:
            self.__ignore_file_paths[__path_to_ignore] = before_ts
        logger.verbose('Ignoring FS events for %r (%s)',
                       path, before_ts)


    def stop_ignoring_fs_events_for_path(self, path, required_ts):
        """Stop ignoring FS notification events for some path.

        @type path: basestring
        @param required_ts: we should stop ignoring events only if the
            C{required_ts} values is exactly this.
        @type required_ts: datetime
        """
        done = False
        with self.__ignore_file_paths_lock:
            try:
                ignore_ts = self.__ignore_file_paths[path]
            except KeyError:
                ignore_ts = None
            else:
                if required_ts == ignore_ts:
                    del self.__ignore_file_paths[path]
                    done = True
        # Don't do any logging within the log, do it outside
        if done:
            logger.verbose('No more ignoring FS events for %r (%s)',
                           path, required_ts)
        else:
            logger.verbose('Tried to un-ignore FS events for %r, but %r != %r',
                           path, required_ts, ignore_ts)


    def if_should_ignore_path(self, path, ts):
        """Test if we should ignore an FS event for some C{path}
        (which occured on C{ts} time).

        @type path: basestring
        @type ts: datetime
        @rtype: bool
        """
        __path_to_check = path.lower() if sys.platform in ['darwin', 'win32'] \
                                       else path

        logger.verbose('Should we ignore FS events for %r (%s)?', path, ts)

        yes_should_ignore = False
        try:  # EAFP!
            ignore_before_ts = self.__ignore_file_paths[__path_to_check]
        except KeyError:
            pass
        else:
            if ts <= ignore_before_ts:
                yes_should_ignore = True

        return yes_should_ignore


    def __get_recently_changed_paths(self):
        """
        Returns the recently changed, "hot", file paths,
        that will probably get into the next dataset.

        @note: pretty heavy function, use with care!

        @rtype: set

        @note: obsoleted!
        """
        to_backup_paths = []
        max_birth = datetime.utcnow()
        with db.RDB() as rdbw:
            _dirs_with_ugroups = \
                HostQueries.HostFiles.get_base_directories_with_ugroups(rdbw)

            # Eagerly evaluate, to be able to run next queries.
            dirs_with_ugroups = list(_dirs_with_ugroups)

            for base_dir, ugroup in dirs_with_ugroups:
                new_paths = \
                    (os.path.join(base_dir, f.rel_path)
                         for f in HostQueries.HostFiles
                                             .get_files_for_backup_older_than(
                                                  max_birth, ugroup.uuid,
                                                  rdbw))
                to_backup_paths.extend(new_paths)

        # to_backup_paths now contains the non-backed-up, but already
        # snapshotted paths from the DB.

        # Two buffers before snapshotting the files
        with self.__cooling_down_to_store_lock:
            cooling_down_paths = self.__cooling_down_to_store.keys()

        with self.__file_states_ready_to_write_lock:
            ready_to_write_paths = self.__file_states_ready_to_write.keys()

        # The values may have duplicates, deduplicate them with set()
        return frozenset(chain(to_backup_paths,
                               cooling_down_paths,
                               ready_to_write_paths))


    def is_path_hot(self, path):
        """
        Check whether the path is "hot", was recently changed and will (likely)
        be re-backupped soon.

        @type path: basestring
        @rtype: bool
        """

        # 1. Is the path already snapshotted in the DB?
        with db.RDB() as rdbw:
            if HostQueries.HostFiles.is_file_going_to_backup(path, rdbw):
                return True

        # 2. Is the path in any intermediate buffers?

        # 2.1
        with self.__cooling_down_to_store_lock:
            if path in self.__cooling_down_to_store:
                return True
        # 2.2
        with self.__file_states_ready_to_write_lock:
            if path in self.__file_states_ready_to_write:
                return True

        # Otherwise, the path is not hot
        return False


    @contract_epydoc
    def __on_fs_change(self, base_dir, base_dir_id, event):
        """A callback function called whenever the FS change event occurs.

        We don't need C{@exceptions_logged()} - the fs_notify manager
        is wrapped already.

        @param base_dir: the base directory we are watching.

        @param base_dir_id: the (already known) ID field (in the DB)
            of C{base_dir}. The record containing the C{base_dir}
            must be present in the DB already.

        @param event: the FS event that has occured.
        @type event: fs_events.Event

        @note: may be called in either reactor thread or secondary thread,
            no guarantees.

        @todo: implement C{FILE_COOL_DOWN_TO_BACKUP} logic.
        """
        cls = self.__class__

        logger.verbose('Received FS event %r', event)

        assert isinstance(event.path, FilePath), repr(event.path)

        if event.path.isfile() or isinstance(event, (fs_events.DeleteEvent,
                                                     fs_events.MoveEvent)):
            file_path = event.path.path

            if self.if_should_ignore_path(file_path, event.ts):
                # Should not be backed up
                logger.debug('Noticed %r being changed but ignoring',
                             file_path)
            elif not cls._should_file_path_be_backed_up(file_path):
                logger.debug('Ignoring file %r', file_path)
            else:
                logger.verbose('Noticed FS change in %r', file_path)
                if isinstance(event,
                              (fs_events.CreateEvent,
                               fs_events.ModifyEvent,
                               fs_events.DeleteEvent)):
                    self.__heat_fs_path(base_dir,
                                        base_dir_id,
                                        old_path=file_path,
                                        new_path=file_path)

                elif isinstance(event, fs_events.MoveEvent):
                    old_path = event.from_path.path
                    assert old_path == file_path, (old_path, file_path)
                    new_path = event.to_path.path

                    with self.__cooling_down_to_store_lock:
                        # At the moment, we are considering MoveEvent
                        # a combination of DeleteEvent and CreateEvent.
                        self.__on_fs_change(base_dir,
                                            base_dir_id,
                                            fs_events.DeleteEvent(
                                                event.from_path))
                        self.__on_fs_change(base_dir,
                                            base_dir_id,
                                            fs_events.CreateEvent(
                                                event.to_path))
                        # Also, we need to reheat the path
                        # of the moved entry...
                        self.__heat_fs_path(base_dir,
                                            base_dir_id,
                                            old_path=event.from_path.path,
                                            new_path=event.to_path.path)
                    # But what about the files, which were under the directory
                    # in 'from_path', and now are inside the 'to_path',
                    # but nobody knows that yet?
                    if isdir(new_path):
                        cls.take_directory_snapshot(dir_path=new_path,
                                                    base_dir_path=base_dir,
                                                    base_dir_id=base_dir_id)

                else:
                    logger.critical('Event %r is not handled', event)

        else:
            logger.debug('In %r, path %r is not file, not supported yet.',
                         event, event.path)
            # TODO: ticket:141 - support directories


    @contract_epydoc
    def __heat_fs_path(self, base_dir, base_dir_id, old_path, new_path):
        """
        "Reheat" (or just start heating) the file path
        in the cooling-down mechanism.

        @param old_path: the previous name of the path.
            May be equal to C{new_path}, but not necessarily.
        @type old_path: basestring

        @param new_path: the new name of the path.
            May be equal to C{old_path}, but not necessarily.
        @type new_path: basestring

        @note: may be called either in reactor thread or in threadpool thread,
            no guarantees.
        """
        callFromThread(self.__heat_fs_path_in_reactor,
                       base_dir, base_dir_id, old_path, new_path)


    @exceptions_logged(logger)
    def __heat_fs_path_in_reactor(self,
                                  base_dir, base_dir_id, old_path, new_path):
        """
        Similar to C{__heat_fs_path}, but runs definitely in reactor thread.
        """
        assert in_main_thread()

        with self.__cooling_down_to_store_lock:
            _call_to_store_fs_change = \
                self.__cooling_down_to_store.get(old_path)

            if _call_to_store_fs_change is not None:
                # Some file is being cooled down already.
                # Remove it from the collection,
                # and stop the associated callLater object.
                logger.debug('Reheating the file: %r (%r)',
                             old_path, new_path)
                del self.__cooling_down_to_store[old_path]
                try:
                    _call_to_store_fs_change.cancel()
                except (internet_error.AlreadyCancelled,
                        internet_error.AlreadyCalled):
                    pass
            else:
                # Cooling down a completely new file..
                logger.verbose('Starting to cool down the file: %r',
                               new_path)

            # pylint:disable=E1101,C0103
            _callLater = reactor.callLater
            # pylint:enable=E1101,C0103
            self.__cooling_down_to_store[new_path] = \
                _callLater(FILE_COOL_DOWN_TO_STORE.total_seconds(),
                           lambda: callInThread(
                                       self.__store_fs_change_after_cooling,
                                       base_dir, base_dir_id, new_path))


    @exceptions_logged(logger)
    def __store_fs_change_after_cooling(self,
                                        base_dir, base_dir_id, file_path):
        """
        After a file operation has been "cooled down" (i.e. no more operation
        occured for the same file during some period of time), write it
        to the DB.
        """
        assert not in_main_thread()

        logger.verbose('Cooled down the file %r, storing it', file_path)

        with self.__cooling_down_to_store_lock:
            _call_to_store_fs_change = \
                self.__cooling_down_to_store.get(file_path)

            # There may be a tiniest period between a moment when
            # the callLater already fired, and when it was removed by,
            # say, __heat_fs_path().
            # So even though this callLater fired, at this point
            # _call_to_store_fs_change might be None.

            if _call_to_store_fs_change is not None:
                try:
                    del self.__cooling_down_to_store[file_path]
                except KeyError:
                    # Funny, how comes?
                    logger.warning('Inside the lock, the key %r disappeared '
                                       'during storing',
                                   file_path)

        # TODO: frankly, C{_call_to_store_fs_change is None}
        # would mean that this state has moved away or something, that is,
        # it should not be stored to the DB.
        # But this needs further investigation, and for now, let's just
        # store it in the DB.

        # Get the up-to-date file state
        _st = os_ex.safe_stat(file_path)
        if _st is not None:
            # Path is present
            file_size = _st.st_size
            file_mtime = os_ex.stat_get_mtime(_st)
        else:
            # Path is deleted
            file_size = None
            file_mtime = datetime.utcnow()

        # This path is ok to snapshot
        rel_dir = relpath_nodot(os.path.dirname(file_path), base_dir)

        # TODO: ticket:141 - LocalPhysicalFileStateRel must be able
        # to support directories; but for now, just don't write
        # the directories.
        # But we still support the deleted paths (_st = None)!
        if _st is None or not os_ex.stat_isdir(_st):
            _state = LocalPhysicalFileStateRel(
                         rel_dir=rel_dir,
                         rel_file=os.path.basename(file_path),
                         size=file_size,
                         time_changed=file_mtime,
                         isdir=False)

            # ... and do you think we write it to the DB right now?
            # Sorry no, we'll just put it to a regularly-dumped buffer.
            with self.__file_states_ready_to_write_lock:
                # Be sure to copy, to prevent any alterations of the state!
                self.__file_states_ready_to_write[file_path] = (base_dir_id,
                                                                _state)


    def __do_next_iteration_of_file_state_bunch(self):
        """Go the next iteration of file state bunch write to the DB."""
        assert in_main_thread()

        # pylint:disable=E1101,C0103
        _callLater = reactor.callLater
        # pylint:enable=E1101,C0103
        _callLater(FILE_BUNCH_COOL_DOWN_TO_STORE.total_seconds(),
                   self.__on_next_iteration_of_file_state_bunch)


    @exceptions_logged(logger)
    def __on_next_iteration_of_file_state_bunch(self):
        assert in_main_thread()
        d = threads.deferToThread(
                lambda: exceptions_logged(logger)(
                            self.__try_save_next_bunch_of_file_states)())
        d.addBoth(lambda ignore:
                      exceptions_logged(logger)(
                          self.__do_next_iteration_of_file_state_bunch)())


    def __try_save_next_bunch_of_file_states(self):
        """Check if we have multiple states to store to the DB, and do it."""
        assert not in_main_thread()

        with self.__file_states_ready_to_write_lock:
            all_states = self.__file_states_ready_to_write.values()
            self.__file_states_ready_to_write = {}

        # "states" contains tuples like (base_dir_id, state).
        # Group them by base_dir_id, and write multiple file states at once.

        if all_states:
            logger.debug('Writing %i file state(s) at once',
                         len(all_states))

            grouped_by_base_dir = sorted_groupby(all_states,
                                                 key=itemgetter(0))

            for base_dir_id, per_base_dir in grouped_by_base_dir:
                states_to_write = imap(itemgetter(1), per_base_dir)

                logger.debug('Writing states for base dir %r', base_dir_id)
                with db.RDB() as rdbw:
                    HostQueries.HostFiles.add_file_states(
                        base_dir_id, states_to_write, rdbw)
            logger.debug('Wrote the states')


    def __publish_host_state(self):
        if self.network_connection_is_working:
            logger_status_network_connection.info(
                'Network connection currently working',
                extra={'status': 'ok'})
        else:
            logger_status_network_connection.info(
                'Network connection currently not working',
                extra={'status': 'fail'})


    @property
    def network_connection_is_working(self):
        return self.__network_connection_is_working

    @network_connection_is_working.setter
    @contract_epydoc
    def network_connection_is_working(self, value):
        """
        @type value: bool
        """
        old_value = self.__network_connection_is_working
        self.__network_connection_is_working = bool(value)

        if value == old_value:
            # Same state, no switch
            pass
        elif value:
            logger_status_network_connection.info(
                'Network connection is working now',
                extra={'status': 'ok'})
        else:
            logger_status_network_connection.info(
                'Network connection error',
                extra={'status': 'fail'})


    @exceptions_logged(logger)
    def _received_http_response(self, response_tuple, orig_message):
        """Overrides method from C{HostApp}."""
        assert in_main_thread()
        self.network_connection_is_working = True

        return super(UHostApp, self)._received_http_response(response_tuple,
                                                             orig_message)


    @exceptions_logged(logger)
    @contract_epydoc
    def _error_on_send_http_request(self, failure, message):
        """Overrides method from C{HostApp}."""
        assert in_main_thread()

        if failure.check(internet_error.ConnectionRefusedError,
                         internet_error.ConnectionDone,
                         internet_error.ConnectionLost,
                         internet_error.DNSLookupError):
            self.network_connection_is_working = False

        return super(UHostApp, self)._error_on_send_http_request(failure,
                                                                 message)


    @exceptions_logged(logger)
    def __on_create_dataset_timer(self):
        assert not in_main_thread()

        if (self.is_started and
            self.do_send_messages and
            self.do_heartbeats_revive):
           # We are running the "main living loop"
            logger.debug('Trying to create dataset...')
            try:
                self.__backup_snapshotted_files_if_needed()
            except Exception:
                logger.exception('A error occured during '
                                     'regular backup attempt.')


    def __backup_snapshotted_files_if_needed(self):
        """
        Look over the existing snapshots and, if some files require backing up,
        start backup for them.
        """
        assert not in_main_thread()

        max_birth = datetime.utcnow() - FILE_COOL_DOWN_TO_BACKUP

        with db.RDB() as rdbw:
            dirs_with_ugroups = \
                HostQueries.HostFiles.get_base_directories_with_ugroups(rdbw)

            dirs_with_ugroups = list(dirs_with_ugroups)  # to close rdbw

        for base_dir, ugroup in dirs_with_ugroups:
            logger.debug('Checking for non-backed data in %r for %r',
                         base_dir, ugroup)

            with db.RDB() as rdbw:
                need_backup, files = \
                    inonempty(HostQueries.HostFiles
                                         .get_files_for_backup_older_than(
                                              max_birth, ugroup.uuid, rdbw))

                    # files is an iterable over LocalPhysicalFileStateRel

                # eagerly evaluate to leave RDB context
                files = list(files)

            # Now we've received the flag whether some file exist which
            # need to be backed up; and also we've received the iterator
            # over the file names to be backed up, together with their
            # sizes and last change time.
            # The extra data (size, last change time) is needed so that we
            # backup only the specific version of the file, but won't
            # backup if it has been changed since (it will be backed up
            # in a different transaction).

            if not need_backup:
                logger.debug('No changes to be synced for %r', ugroup)
            else:
                logger.debug('On auto sync, creating new dataset and '
                                 'launching backup for %r',
                             ugroup)

                self.__backup_some_phys_files(base_dir, files, ugroup)


    @contract_epydoc
    def __backup_some_phys_files(self, base_dir, files, ugroup,
                                 __do_start_backup=True):
        r"""Given some files, create a new dataset and start to backup them.

        >>> # ugroup = UserGroup(
        >>> #     uuid=UserGroupUUID('00000000-bbbb-0000-0000-000000000001'),
        >>> #     name='AlphA',
        >>> #     private=True,
        >>> #     enc_key='\x01\xe6\x13\xdab)\xd2n\xd6\xafTH\x03h\x02\x12'
        >>> #             '\x17D\x1a\xeb\x8b6\xc0\x9b\xa6\x7f\xcc\x06N\xcf'
        >>> #             '\x8b\xcd'
        >>> # )

        >>> # __backup_some_phys_files(
        >>> #     base_dir='u'/home/john/FreeBrie',
        >>> #     files=[
        >>> #         LocalPhysicalFileStateRel(
        >>> #             rel_dir='',
        >>> #             rel_file=u'f1.mp3',
        >>> #             size=13829879,
        >>> #             time_changed=datetime(2012, 11, 5, 12,12,41,904430)),
        >>> #         LocalPhysicalFileStateRel(
        >>> #             rel_dir='',
        >>> #             rel_file=u'f2.avi',
        >>> #             size=3522710,
        >>> #             time_changed=datetime(2012, 11, 5, 12,12,41,988433)),
        >>> #         LocalPhysicalFileStateRel(
        >>> #             rel_dir=u'a/b',
        >>> #             rel_file=u'bbb',
        >>> #             size=4,
        >>> #             time_changed=datetime(2012, 10, 11, 15 33 42 19808)),
        >>> #         LocalPhysicalFileStateRel(
        >>> #             rel_dir=u'a/b/c',
        >>> #             rel_file=u'ccc',
        >>> #             size=4,
        >>> #             time_changed=datetime(2012, 10, 11, 15 33 41 979807))
        >>> #     ],
        >>> #     ugroup=ugroup)

        @todo: complete the unit test, which is half-done!

        @param base_dir: the directory being backed up.
        @type base_dir: basestring

        @param files: the iterable over the files which should be backed up.
            Contains C{LocalPhysicalFileStateRel} objects.
            The caller should ensure that C{files} is non-empty!
        @type files: col.Iterable

        @type ugroup: UserGroup

        @return: the created dataset (if succeeded).
        @rtype: DatasetOnPhysicalFiles, NoneType
        """
        logger.debug('__backup_some_phys_files(%r, %r)',
                     base_dir, ugroup)

        # Group files by rel_dir; then ignore base_dir,
        # keep only rel_dir, rel_file, size and time_changed
        files_grouped_by_rel_dir = \
            ((RelVirtualFile(rel_dir=f.rel_dir,
                             filename=f.rel_file,
                             # If we can read real stat, read it;
                             # otherwise we'll emulate it with fake_stat
                             stat=coalesce(os_ex.safe_stat(  # real stat
                                               os.path.join(base_dir,
                                                            f.rel_path)),
                                           os_ex.fake_stat(  # deleted file
                                               st_mode=None,
                                               atime=f.time_changed,
                                               mtime=f.time_changed,
                                               ctime=f.time_changed,
                                               size=None)),
                             stat_getter=lambda f=f:
                                             os_ex.safe_stat(
                                                 os.path.join(base_dir,
                                                              f.rel_path)),
                             file_getter=lambda f=f:
                                             open(os.path.join(base_dir,
                                                               f.rel_path),
                                                  'rb'))
                 for f in per_rel_dir)
                     for rel_dir, per_rel_dir
                         in sorted_groupby(files, attrgetter('rel_dir')))

        # Example:
        # files_grouped_by_rel_dir = [
        #     [
        #         RelVirtualFile(...),
        #         RelVirtualFile(...),
        #         RelVirtualFile(...)
        #     ],
        #     [
        #         RelVirtualFile(...),
        #     [
        #         RelVirtualFile(...)
        #     ]
        # ]
        _path_map = {base_dir: {'ifiles': files_grouped_by_rel_dir,
                                'stat': os_ex.safe_stat(base_dir)}}

        ds_uuid = DatasetUUID.safe_cast_uuid(gen_uuid())
        ds = self.select_paths_for_backup(ds_name='',
                                          ds_uuid=ds_uuid,
                                          ugroup_uuid=ugroup.uuid,
                                          sync=True,
                                          paths_map=_path_map)
        if ds is not None and __do_start_backup:
            self.start_backup(ds_uuid)

        return ds
