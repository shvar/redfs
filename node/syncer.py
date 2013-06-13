#!/usr/bin/python
"""
The code to synchronize the datasets/files between multiple hosts.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from datetime import datetime, timedelta
from types import NoneType
from uuid import UUID

from twisted.internet import error as internet_error, reactor, threads

from contrib.dbc import contract_epydoc

from common.abstract_peerbook import AbstractPeerBook
from common.abstractions import AbstractApp
from common.db import Queries
from common.inhabitants import Node, Host, HostAtNode, User, UserGroup
from common.itertools_ex import inonempty
from common.peerbook_in_memory import HOST_DEATH_TIME
from common.typed_uuids import DatasetUUID, PeerUUID
from common.utils import exceptions_logged, in_main_thread
from common.twisted_utils import DelayServiceForCallback

from trusted import db, docstore as ds
from trusted.db import TrustedQueries
from trusted.docstore.fdbqueries import FDBQueries



#
# Constants
#

logger = logging.getLogger(__name__)

TRY_SYNC_UNEXPECTED_DATASETS_PERIOD = timedelta(seconds=10)
"""
How often is Fast DataBase checked for the new unexpected (created outside
of the Node) dataset to be synced.
"""



#
# Classes
#

class Syncer(object):
    """The manager which synchronizes the datasets between multiple hosts.

    It tries to mimic the C{t.a.s.IService}, in particular supporting
    C{.startService()} and C{.stopService()} methods.
    """

    __slots__ = ('__known_hosts', '__app',
                 '__unexpected_datasets_sync_service')


    @contract_epydoc
    def __init__(self, known_hosts, app):
        """Constructor.

        @type known_hosts: AbstractPeerBook
        @type app: AbstractApp
        """
        self.__known_hosts = known_hosts
        self.__app = app
        self.__unexpected_datasets_sync_service = \
            DelayServiceForCallback(
                callback=self.__on_next_unexpected_ds_sync_iteration,
                period=TRY_SYNC_UNEXPECTED_DATASETS_PERIOD)


    def startService(self):
        """To mimic C{t.a.s.IService} interface."""
        assert in_main_thread()

        logger.debug('Starting...')
        self.__unexpected_datasets_sync_service.startService()


    def stopService(self):
        """To mimic C{t.a.s.IService} interface."""
        assert in_main_thread()

        logger.debug('Stopping...')
        self.__unexpected_datasets_sync_service.stopService()


    @exceptions_logged(logger)
    def __on_next_unexpected_ds_sync_iteration(self):
        """
        Handle the next iteration of the syncer attempts to sync
        the unexpected datasets.
        """
        assert not in_main_thread()

        # Do something on every iteration.
        logger.verbose('Try to sync unexpected datasets...')

        upload_to_process = True
        while upload_to_process:
            with ds.FDB() as fdbw:
                upload_to_process = \
                    FDBQueries.Uploads.dequeue_upload_to_synchronize(fdbw)
            if upload_to_process is not None:

                group_uuid = upload_to_process.group_uuid
                upload_uuid = upload_to_process.upload_uuid
                ds_uuid = upload_to_process.ds_uuid

                try:
                    logger.debug('Got the upload operation %r, trying to sync',
                                 upload_to_process)
                    # Is it a sync-dataset?
                    # We don't care, cause
                    # self.restore_dataset_to_lacking_hosts() will deal
                    # with it itself.
                    self.restore_dataset_to_lacking_hosts(
                        me=self.__app.server_process.me,
                        host=None,
                        ds_uuid=ds_uuid)
                except:
                    logger.exception('Something happened during syncing '
                                         'dataset %s',
                                     ds_uuid)
                else:
                    # After successfully (!) syncing the dataset,
                    # finally remove the upload operation.
                    with ds.FDB() as fdbw:
                        FDBQueries.Uploads.remove_upload_after_sync(
                            group_uuid, upload_uuid, fdbw)
                    logger.info('Considering the uploaded dataset %s synced',
                                ds_uuid)


    @contract_epydoc
    def __restore_datasets_to_host(self, me, host, ds_uuids):
        """
        Launch RESTORE transaction(s) to restore several datasets
        (with dataset uuids in C{ds_uuids}) to the host C{host}.

        @param me: my node
        @type me: Node

        @type host: Host
        @type ds_uuids: col.Iterable
        """
        tr_manager = self.__app.tr_manager
        ds_uuids_present, ds_uuids_asked_to_restore = inonempty(ds_uuids)

        if not ds_uuids_present:
            logger.debug('Actually, nothing to restore to %r', host)
        else:
            sync_ds_uuids_for_this_host = \
                {state.ds_uuid
                    for state in tr_manager.get_tr_states(class_name='RESTORE',
                                                          dst_uuid=host.uuid)
                    if state.is_sync}

            if sync_ds_uuids_for_this_host:
                logger.debug('Restoring something to %r, while '
                                 'the following RESTORE transactions are '
                                 'already syncing to it: %r',
                             host, sync_ds_uuids_for_this_host)

            # Let's evaluate the sequence to be able to multiply reiterate it,
            # as well as count its length.
            ds_uuids_asked_to_restore = frozenset(ds_uuids_asked_to_restore)
            assert ds_uuids_asked_to_restore, repr(ds_uuids_asked_to_restore)

            ds_uuids_to_restore = \
                ds_uuids_asked_to_restore - sync_ds_uuids_for_this_host
            logger.verbose('While asked to restore %i dataset(s) (%r), '
                               'will in fact restore %i one(s) (%r)',
                           len(ds_uuids_asked_to_restore),
                           ds_uuids_asked_to_restore,
                           len(ds_uuids_to_restore),
                           ds_uuids_to_restore)

            # # If we are syncing a sole dataset, let's sync it;
            # # if there are multiple ones, let's merge them.
            if len(ds_uuids_to_restore) == 1:
                ds_uuids_will_restore = ds_uuids_to_restore
            else:
                with db.RDB() as rdbw:
                    ds_uuids_will_restore = \
                        TrustedQueries.TrustedDatasets.merge_sync_datasets(
                            host.uuid, ds_uuids_to_restore, rdbw)
                    # To get it outside RDB wrapper,...
                    ds_uuids_will_restore = list(ds_uuids_will_restore)
                logger.debug('Merged DS UUIDs: %r', ds_uuids_will_restore)

            logger.debug('Will in fact restore these datasets: %r',
                         ds_uuids_will_restore)
            for ds_uuid in ds_uuids_will_restore:
                logger.debug('Restoring files from %s to %r', ds_uuid, host)

                r_tr = tr_manager.create_new_transaction(
                           name='RESTORE',
                           src=me,
                           dst=host,
                           parent=None,
                           # RESTORE-specific
                           ds_uuid=ds_uuid,
                           # None means "all files"
                           file_paths_for_basedirs=None,
                           wr_uuid=None)


    @contract_epydoc
    def host_just_became_alive(self, me, host_uuid):
        """Given a host, restore (to it) all datasets which are missing on it.

        @param me: my node
        @type me: Node

        @param host_uuid: the UUID of the host which just became alive.
        @type host_uuid: PeerUUID
        """
        assert not in_main_thread()

        host = self.__known_hosts[host_uuid]
        logger.verbose('Reviving %r', host)

        tr_manager = self.__app.tr_manager

        with db.RDB() as rdbw:
            suspended = TrustedQueries.TrustedUsers.is_user_suspended_by_host(
                            host.uuid, rdbw)

        if not suspended:
            # Do we have any datasets to restore to this host?
            with db.RDB() as rdbw:
                _ds_uuids_to_restore = \
                    TrustedQueries.TrustedDatasets \
                                  .get_ds_uuids_not_synced_to_host(
                                       host.uuid, rdbw)

                # Let's hope that there is not too many datasets UUIDs,
                # and eagerly calculate the list (so we can get out
                # of Database Wrapper context).
                ds_uuids_to_restore = list(_ds_uuids_to_restore)
                del _ds_uuids_to_restore  # help GC

            datasets_exist = bool(ds_uuids_to_restore)

            logger.debug('%r just became alive at %r: %sneed to sync',
                         host, me, '' if datasets_exist else 'do not ')

            self.__restore_datasets_to_host(me, host, ds_uuids_to_restore)


    @contract_epydoc
    def restore_dataset_to_lacking_hosts(self, me, host, ds_uuid):
        """
        Given a dataset (its UUID), restore it to every host which lacks it
        (if it is a sync dataset.).

        @param me: my node
        @type me: Node

        @param host: host which just completed the backup and is going
            to restore the data, or C{None} if not applicable.
        @type host: Host, NoneType

        @type ds_uuid: DatasetUUID
        """
        logger.debug('Restoring DS %s to all lacking hosts (if needed), '
                         'except %r',
                     ds_uuid, host)

        with db.RDB() as rdbw:
            # If the dataset is non-syncing, _host_uuids_to_restore
            # will be empty.
            host_uuid = host.uuid if host else None
            _host_uuids_to_restore = \
                TrustedQueries.TrustedDatasets \
                              .get_host_uuids_lacking_sync_dataset(
                                   ds_uuid, rdbw=rdbw)

            # But, if we have a host that definitely just completed the backup,
            # we can filter it out.
            # At the same step, we eagerly evaluate it, so we can get out
            # of RDB wrapper.
            host_uuids_to_restore = list(_host_uuids_to_restore) \
                                        if host_uuid is None \
                                        else [u for u in _host_uuids_to_restore
                                                if u != host_uuid]

        if host_uuids_to_restore:
            logger.debug('Will probably restore dataset %s to %r',
                         ds_uuid, host_uuids_to_restore)

            for restore_host_uuid in host_uuids_to_restore:
                restore_host_uuid = \
                    PeerUUID.safe_cast_uuid(restore_host_uuid)
                if self.__known_hosts.is_peer_alive(restore_host_uuid):
                    logger.debug('Restoring dataset %s to host %r',
                                 ds_uuid, restore_host_uuid)
                    r_tr = self.__app.tr_manager.create_new_transaction(
                               name='RESTORE',
                               src=me,
                               dst=self.__known_hosts[restore_host_uuid],
                               parent=None,
                               # RESTORE-specific
                               ds_uuid=ds_uuid,
                               # None means "all files"
                               file_paths_for_basedirs=None,
                               wr_uuid=None)
                else:
                    logger.debug("Could've restored dataset %s to host %r, "
                                     'but the host is not alive',
                                 ds_uuid, restore_host_uuid)
        else:
            logger.debug('Dataset %s is likely not auto-syncable', ds_uuid)
