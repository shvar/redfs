#!/usr/bin/python
"""
The mixin for async host process API useful from other transactions, GUI, etc.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import operator
import os
import posixpath
from abc import ABCMeta, abstractproperty
from itertools import chain
from types import NoneType
from uuid import UUID

from contrib.dbc import consists_of, contract_epydoc

from twisted.internet import defer, threads

from common.abstract_dataset import AbstractDataset
from common.abstract_transaction_manager import AbstractTransactionManager
from common.chunk_storage import IChunkStorage
from common.db import Queries
from common.inhabitants import Host, Node
from common.utils import coalesce, exceptions_logged, repr_maybe_binary

from protocol import messages

from . import db, transactions
from .db import HostQueries


#
# Constants
#

logger = logging.getLogger(__name__)
logger_status = logging.getLogger('status.host_process')
logger_status_info_datasets = logging.getLogger('status.queried.datasets')
logger_status_restore = logging.getLogger('status.restore')



#
# Classes
#

class HostAppAsyncAPIMixin(object):
    """
    A useful mixin for the HostApp, containing all the business-logic functions
    which can be called by the other parties in an async way.

    That is, every method of this class returns C{defer.Deferred}.
    """

    __metaclass__ = ABCMeta


    @abstractproperty
    def tr_manager(self):
        """The mixin assumes this property exists.

        @rtype: AbstractTransactionManager
        """
        pass

    @abstractproperty
    def chunk_storage(self):
        """The mixin assumes this property exists.

        @rtype: IChunkStorage
        """
        pass

    @abstractproperty
    def host(self):
        """The mixin assumes this property exists.

        @rtype: Host
        """
        pass

    @abstractproperty
    def primary_node(self):
        """The mixin assumes this property exists.

        @rtype: Node
        """
        pass


    @contract_epydoc
    def query_datasets(self, on_received=None):
        """Request all the available datasets for this host.

        @param on_received: the handler which is called when the datasets
            are received. If C{None}, nothing is called, the data is just
            output via the logger.
        @type on_received: col.Callable, NoneType

        @rtype: defer.Deferred
        """
        _query = {'select': '*',
                  'from': 'datasets'}

        nifn_tr = self.tr_manager.create_new_transaction(
                      name='NEED_INFO_FROM_NODE',
                      src=self.host,
                      dst=self.primary_node,
                      # NEED_INFO_FROM_NODE-specific
                      query=_query)

        @exceptions_logged(logger)
        def error_handler(failure):
            logger_status.error(
                'Datasets could not be queried from the cloud: %r %s',
                failure, failure.getErrorMessage(),
                extra={'_type': 'error during querying datasets'})


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(ni_state):
            """
            @type ni_state: transactions.NeedInfoFromNodeTransaction_Host.State
            @precondition: consists_of(ni_state.ack_result, AbstractDataset)
            """
            assert isinstance(ni_state.ack_result, col.Iterable), \
                   repr(ni_state.ack_result)
            if on_received is not None:
                on_received(ni_state.ack_result)
            logger_status_info_datasets.info('Datasets: %r',
                                             ni_state.ack_result,
                                             extra={
                                                 'type': 'all_datasets',
                                                 'value': ni_state.ack_result
                                             })


        # Do not use addCallbacks() here
        nifn_tr.completed.addCallback(success_handler)
        nifn_tr.completed.addErrback(error_handler)

        return nifn_tr.completed


    @contract_epydoc
    def query_dataset_files(self, ds_uuid, on_received=None):
        """Request the files in the particular dataset for this host.

        @param ds_uuid: the UUID of the dataset in the cloud.
        @type ds_uuid: UUID

        @param on_received: the handler which is called when
            the file information is received.
            If C{None}, nothing is called, the data is just output
            via the logger.
        @type on_received: col.Callable, NoneType

        @rtype: defer.Deferred
        """
        _query = {'select': '*',
                  'from': 'files',
                  'where': {'dataset': ds_uuid.hex}}

        nifn_tr = self.tr_manager.create_new_transaction(
                      name='NEED_INFO_FROM_NODE',
                      src=self.host,
                      dst=self.primary_node,
                      # NEED_INFO_FROM_NODE-specific
                      query=_query)

        @exceptions_logged(logger)
        def error_handler(failure):
            logger_status.error(
                'Dataset files could not be queried from the cloud: \r %s',
                failure, failure.getErrorMessage(),
                extra={'_type': 'error during querying files from dataset'})


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(ni_state):
            """
            @type ni_state: transactions.NeedInfoFromNodeTransaction_Host.State
            """
            assert isinstance(ni_state.ack_result, col.Mapping), \
                   repr(ni_state.ack_result)
            # ni_state.ack_result now stores the dictionary mapping
            # the base directories to the files. But we don't need that!
            # Let's remap it to the root directories.
            by_root_disk = {}

            # Loop over the files in all basedirs,
            # and add them to the by_root_disk
            # which maps the root disk to the list of the files
            # on this disk.
            for f in chain.from_iterable(ni_state.ack_result.itervalues()):
                by_root_disk.setdefault(f.full_path.split(os.path.sep)[0]
                                            or posixpath.sep,
                                        []) \
                            .append(f)
                # But the rel_path in the file now is relative to the
                # previous base_dir rather than the current root_dir!
                # That's wrong.
                f.rel_path = f.full_path[f.full_path.index(os.path.sep) + 1:]

            # Just for beauty, sort the files for each directory
            sort_key_func = operator.attrgetter('full_path')
            for files in by_root_disk.itervalues():
                files.sort(key=sort_key_func)

            if on_received is not None:
                on_received(ds_uuid, by_root_disk)
            logger_status_info_datasets.info('Files',
                                             extra={
                                                 'type': 'files_from_dataset',
                                                 'ds_uuid': ds_uuid,
                                                 'value': by_root_disk
                                             })


        # Do not use addCallbacks() here
        nifn_tr.completed.addCallback(success_handler)
        nifn_tr.completed.addErrback(error_handler)

        return nifn_tr.completed


    @contract_epydoc
    def delete_datasets_from_node(self,
                                  ds_uuids_to_delete, on_completed=None):
        """Request the node to delete the datasets

        @precondition: consists_of(ds_uuids_to_delete, UUID)
                       # ds_uuids_to_delete

        @param on_completed: the handler which is called when the operation
            is completed. If C{None}, nothing is called, the data is just
            output via the logger.
        @precondition: on_completed is None or callable(on_completed)

        @rtype: defer.Deferred
        """
        eon_tr = self.tr_manager.create_new_transaction(
                     name='EXECUTE_ON_NODE',
                     src=self.host,
                     dst=self.primary_node,
                     # EXECUTE_ON_NODE-specific
                     ds_uuids_to_delete=ds_uuids_to_delete)


        @exceptions_logged(logger)
        def error_handler(failure):
            logger.debug('Could not delete the datasets from the Node: %r %s',
                         failure, failure.getErrorMessage())


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(eon_state):
            """
            @type eon_state: transactions.ExecuteOnNodeTransaction_Host.State
            @type eon_state.ack_deleted_ds_uuids: col.Iterable
            @precondition: consists_of(eon_state.ack_deleted_ds_uuids, UUID)
            """
            if on_completed is not None:
                on_completed(eon_state.ack_deleted_ds_uuids)


        # Do not use addCallbacks() here
        eon_tr.completed.addCallback(success_handler)
        eon_tr.completed.addErrback(error_handler)

        return eon_tr.completed


    @contract_epydoc
    def set_setting(self, setting_name, setting_value, on_received=None):
        """Given a setting name and a (raw string) value, set the setting.

        @type on_received: col.Callable, NoneType

        """
        old_value = HostQueries.HostSettings.get(setting_name)


        @exceptions_logged(logger)
        def update_in_thread(old_value, new_value):
            self.chunk_storage.update_dummy_chunks_size(old_value, new_value)

        @exceptions_logged(logger)
        def migrate_in_thread(old_path, new_path):
            self.chunk_storage.migrate_chunks(old_path, new_path)


        # Special setting-aware processing (before the value is actually set).
        if setting_name == Queries.Settings.MAX_STORAGE_SIZE_MIB:
            d = threads.deferToThread(update_in_thread,
                                      old_value, setting_value)
        elif setting_name == Queries.Settings.CHUNK_STORAGE_PATH:
            d = threads.deferToThread(migrate_in_thread,
                                      old_value, setting_value)
        else:
            d = defer.Deferred()
            d.callback(True)


        @exceptions_logged(logger)
        def __on_finish(res):
            with db.RDB() as rdbw:
                HostQueries.HostSettings.set(setting_name,
                                             setting_value,
                                             direct=True,
                                             rdbw=rdbw)
            logger_status.info('Setting %r has been changed from %s to %s',
                               setting_name,
                               repr_maybe_binary(old_value),
                               repr_maybe_binary(setting_value),
                               extra={'_type': 'setting_has_been_set',
                                      'value': setting_name})


        d.addBoth(__on_finish)

        if on_received is not None:
            d.addBoth(on_received)

        return d


    @contract_epydoc
    def query_overall_cloud_stats(self, on_received=None):
        """Request the overall cloud statistics.

        @param on_received: the handler which is called when the data
            is received.
            If C{None}, nothing is called, the data is just output
            via the logger.
        @type on_received: col.Callable, NoneType

        @rtype: defer.Deferred
        """
        _query = {'select': ('total_hosts_count',
                             'alive_hosts_count',
                             'total_mb',
                             'used_mb'),
                  'from': 'cloud_stats'}

        nifn_tr = self.tr_manager.create_new_transaction(
                      name='NEED_INFO_FROM_NODE',
                      src=self.host,
                      dst=self.primary_node,
                      # NEED_INFO_FROM_NODE-specific
                      query=_query)


        @exceptions_logged(logger)
        def error_handler(failure):
            logger_status.warning(
                'Statistics could not be queried from the cloud: %r %s',
                failure, failure.getErrorMessage(),
                extra={'_type': 'error during querying datasets'})


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(ni_state):
            """
            @type ni_state: transactions.NeedInfoFromNodeTransaction_Host.State
            """
            assert isinstance(ni_state.ack_result, col.Mapping), \
                   repr(ni_state.ack_result)
            if on_received is not None:
                on_received(ni_state.ack_result)
            logger_status.info('Cloud statistics: %r',
                               ni_state.ack_result,
                               extra={'_type': 'cloud_stats',
                                      'value': ni_state.ack_result})


        # Do not use addCallbacks() here
        nifn_tr.completed.addCallback(success_handler)
        nifn_tr.completed.addErrback(error_handler)

        return nifn_tr.completed


    @contract_epydoc
    def query_data_replication_stats(self,
                                     ds_uuid, path, recursive=False,
                                     on_received=None):
        """
        Request the replication statistics
        for the particular (or any) files in the particular (or any) dataset
        for this host.

        @param ds_uuid: the UUID of the dataset in the cloud
                        (or C{None} if any dataset is ok).
        @type ds_uuid: UUID, NoneType

        @param path: the path which needs the statistics (or C{None} if
            any path in the dataset is ok).
        @type path: basestring, NoneType

        @param recursive: whether the path should be treated recursive.
        @type recursive: bool

        @param on_received: the handler which is called when the data
                            is received.
            If C{None}, nothing is called, the data is just output
            via the logger.
        @type on_received: col.Callable, NoneType
        """
        _query = {'select': ('file_count',
                             'file_size',
                             'uniq_file_count',
                             'uniq_file_size',
                             'full_replicas_count',
                             'chunk_count',
                             'chunk_replicas_count',
                             'hosts_count'),
                  'from': 'data_stats',
                  'where': {'dataset': ds_uuid.hex if ds_uuid is not None
                                                   else '*',
                            'path': coalesce(path, '*'),
                            'rec': recursive}}

        nifn_tr = self.tr_manager.create_new_transaction(
                      name='NEED_INFO_FROM_NODE',
                      src=self.host,
                      dst=self.primary_node,
                      # NEED_INFO_FROM_NODE-specific
                      query=_query)


        @exceptions_logged(logger)
        def error_handler(failure):
            logger_status.error(
                'The statistics for the %r/%r could not '
                    'be queried from the cloud: \r %s',
                ds_uuid, path,
                failure, failure.getErrorMessage(),
                extra={'_type': 'error_on_querying_statistics_from_dataset'})


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(ni_state):
            """
            @type ni_state: transactions.NeedInfoFromNodeTransaction_Host.State
            """
            if on_received is not None:
                on_received(ds_uuid, path, ni_state.ack_result)
            logger_status.info('Data statistics %r',
                               ni_state.ack_result,
                               extra={'_type': 'data_stats',
                                      'ds_uuid': ds_uuid,
                                      'path': path,
                                      'value': ni_state.ack_result})


        # Do not use addCallbacks() here
        nifn_tr.completed.addCallback(success_handler)
        nifn_tr.completed.addErrback(error_handler)


    @contract_epydoc
    def start_backup(self, ds_uuid, on_completed=None):
        """Start the backup of the preselected directories.

        @type ds_uuid: UUID

        @param on_completed: The handler which is called
            when the WANT_BACKUP transaction is completed.
            If None, nothing is called, the data is just output via the logger.
        @type on_completed: col.Callable, NoneType

        @rtype: defer.Deferred
        """
        wb_tr = self.tr_manager.create_new_transaction(name='WANT_BACKUP',
                                                       src=self.host,
                                                       dst=self.primary_node,
                                                       # WANT_BACKUP-specific
                                                       ds_uuid=ds_uuid)

        @exceptions_logged(logger)
        def error_handler(failure):
            logger_status.error(
                'Cannot request for backup: %r %s',
                failure, failure.getErrorMessage(),
                extra={'_type': 'error during requesting backup'})


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(wb_state):
            """
            @type wb_state: transactions.WantBackupTransaction_Host.State
            """
            result_is_good = messages.WantBackupMessage.ResultCodes.is_good(
                                 wb_state.ack_result_code)

            if on_completed is not None:
                on_completed(result_is_good)


        # Do not use addCallbacks() here
        wb_tr.completed.addCallback(success_handler)
        wb_tr.completed.addErrback(error_handler)

        return wb_tr.completed


    @contract_epydoc
    def start_restore(self,
                      file_paths_to_restore, ds_uuid, restore_directory,
                      on_completed=None):
        """
        Start the restore of the given files from the given dataset
        into the given directory.

        @param file_paths_to_restore: the list of (relative) file paths
            of the files which should be restored.
        @type file_paths_to_restore: list
        @precondition: all(isinstance(fp, basestring)
                               for fp in file_paths_to_restore)

        @param ds_uuid: the UUID of the dataset to restore files from

        @param restore_directory: what directory to restore the files to.
        @type restore_directory: basestring

        @param on_completed: The handler which is called
            when the WANT_RESTORE transaction is completed.
            If C{None}, nothing is called, the data is just output
            via the logger.
        @type on_completed: col.Callable, NoneType

        @rtype: defer.Deferred
        """
        wr_tr = self.tr_manager.create_new_transaction(
                    name='WANT_RESTORE',
                    src=self.host,
                    dst=self.primary_node,
                    # WANT_RESTORE-specific
                    dataset_uuid=ds_uuid,
                    file_paths=file_paths_to_restore,
                    target_dir=restore_directory)


        @exceptions_logged(logger)
        def error_handler(failure):
            logger_status.error('Restore requesting failed: %r, %s',
                                failure, failure.getErrorMessage(),
                                extra={'_type':
                                           'error during requesting restore'})
            # A second log, specially for the user.
            logger.error('Restore requesting failed: %r, %s',
                         failure, failure.getErrorMessage())


        @exceptions_logged(logger)
        @contract_epydoc
        def success_handler(wr_state):
            """
            @type wr_state: transactions.WantRestoreTransaction_Host.State

            @todo: Check that restore_directory (if exists) is a directory
            """
            result_is_good = messages.WantRestoreMessage.ResultCodes.is_good(
                                 wr_state.ack_result_code)

            if not result_is_good:
                # Specially to be trackable by the user.
                logger.error('Could not restore files!')
            else:
                logger.debug('Files were restored successfully before')

            logger_status_restore.info('Restore status for %r into %r: %s',
                                       wr_tr,
                                       restore_directory,
                                       'success' if result_is_good else 'fail',
                                       extra={'result': result_is_good,
                                              'ds_uuid': ds_uuid,
                                              'target_dir': restore_directory})

            if on_completed is not None:
                on_completed(result_is_good)


        # Do not use addCallbacks() here
        wr_tr.completed.addCallback(success_handler)
        wr_tr.completed.addErrback(error_handler)

        return wr_tr.completed
