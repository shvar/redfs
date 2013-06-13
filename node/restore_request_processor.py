#!/usr/bin/python
"""
The processor for the externally initiated restore requests.
"""

#
# Imports
#

from __future__ import absolute_import
from datetime import timedelta
import logging

from twisted.application.internet import TimerService

from common.db import Queries
from common.server import ServerProcess
from common.twisted_utils import callInThread
from common.utils import exceptions_logged, gen_uuid, in_main_thread

from trusted import db, docstore as ds
from trusted.docstore.fdbqueries import FDBQueries

from . import transactions, settings



#
# Constants
#

logger = logging.getLogger(__name__)

# How often to perform a fast and simple audit?
try:
    from node_magic import WEB_RESTORE_PERIOD  # pylint:disable=F0401
except ImportError:
    WEB_RESTORE_PERIOD = timedelta(seconds=5)



#
# Classes
#

class RestoreRequestProcessor(object):
    """
    The processor for the externally initiated restore requests.
    """
    __slots__ = ('__server_process', '__restore_timer')


    def __init__(self, server_process):
        assert isinstance(server_process, ServerProcess), \
               repr(server_process)
        self.__server_process = server_process

        self.__restore_timer = \
            TimerService(WEB_RESTORE_PERIOD.total_seconds(),
                         exceptions_logged(logger)(callInThread),
                         self.__poll_restore_requests_in_thread)


    def __poll_restore_requests_in_thread(self):
        """Perform another iteration of polling the restore requests."""
        assert not in_main_thread()

        poll_uuid = gen_uuid()
        logger.debug('Polling restore requests (%s)', poll_uuid)

        restore_request = True
        while restore_request is not None:
            with ds.FDB() as fdbw:
                restore_request = \
                    FDBQueries.RestoreRequests \
                              .atomic_start_oldest_restore_request(fdbw=fdbw)

            logger.debug('Poll (%s) returned %r', poll_uuid, restore_request)
            if restore_request is not None:
                # We've indeed have some restore request that needs processing.

                # Create new "virtual" dataset with all the data
                # to be restored.
                with db.RDB() as rdbw:
                    new_ds_uuid = \
                        Queries.Datasets.restore_files_to_dataset_clone(
                            restore_request.base_ds_uuid,
                            restore_request.paths,
                            restore_request.ts_start,
                            rdbw)

                # Now we know the new dataset to be restored.
                # Btw, write it into the docstore.
                # Doesn't need to be atomic, as only a single node
                # may be processing it at a time.
                with ds.FDB() as fdbw:
                    FDBQueries.RestoreRequests.set_ds_uuid(
                        _id=restore_request._id,
                        new_ds_uuid=new_ds_uuid,
                        fdbw=fdbw)

                # After creating the dataset, let's restore it to all host
                # which are alive.
                _syncer = self.__server_process.app.syncer
                _syncer.restore_dataset_to_lacking_hosts(
                    me=self.__server_process.me,
                    host=None,
                    ds_uuid=new_ds_uuid)

        logger.debug('Polling restore requests (%s) - done', poll_uuid)


    def start(self):
        """Start the processor."""
        assert in_main_thread()
        self.__restore_timer.startService()


    def stop(self):
        """Stop the processor."""
        assert in_main_thread()
        self.__restore_timer.stopService()
