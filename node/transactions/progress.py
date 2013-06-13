#!/usr/bin/python
"""
PROGRESS transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
from datetime import timedelta
from time import sleep

from common.abstractions import unpauses_incoming
from common.db import Queries
from common.itertools_ex import repr_long_sized_iterable

from protocol import transactions

from trusted import db
from trusted.db import TrustedQueries
from trusted.docstore.models.transaction_states.progress import \
    ProgressTransactionState_Node

from ._node import AbstractNodeTransaction



#
# Constants
#

logger = logging.getLogger(__name__)

MAX_RDB_WRITE_ATTEMPTS = 5
"""How many times try to write the data to RDB, until it works?"""

SLEEP_ON_RDB_WRITE_RETRY = timedelta(seconds=5)
"""How much time to sleep before next retry to write into DB?
Yes that's C{time.sleep()}, that's bad, we know. But this is rare and really
needed measure.
"""



#
# Classes
#

class ProgressTransaction_Node(transactions.ProgressTransaction,
                               AbstractNodeTransaction):
    """PROGRESS transaction on the Node."""

    __slots__ = ()

    State = ProgressTransactionState_Node


    @unpauses_incoming
    def on_begin(self):
        """
        Received the PROGRESS request from a host.
        """
        assert self.is_incoming()

        _message = self.message
        host = _message.src
        host_uuid = host.uuid
        me = _message.dst

        # Write into database, and do nothing more,
        # as this completes the transaction.
        dataset = _message.dataset
        ds_uuid = dataset.uuid if dataset is not None else None

        # Multiple independent RDB wrappers are used,
        # to decrease the RDB write lock times.

        # [1/5] Store dataset, either on beginning of backup or on end.
        #       Dataset creation should occur BEFORE chunks/blocks writing.
        #       Dataset finalization should occur AFTER chunks/blocks writing.
        if dataset is not None and not _message.completion:
            logger.info('Backup just started.')

            # Maybe the dataset is present already?
            logger.debug('Is the dataset present? DS %s, host %s',
                         ds_uuid, host_uuid)

            with db.RDB() as rdbw:
                dataset_in_db = Queries.Datasets.get_dataset_by_uuid(
                                    ds_uuid, host_uuid, rdbw)
                if dataset_in_db is None:
                    # Adding the dataset
                    dataset_uuid = Queries.Datasets.create_dataset_for_backup(
                                       host_uuid, dataset, rdbw)
                    assert dataset_uuid == ds_uuid, (dataset_uuid, ds_uuid)
                    logger.info('Dataset %s added to RDB.', dataset_uuid)
                else:
                    logger.debug('The dataset %s is present already, '
                                     'not readding.',
                                 ds_uuid)

        # [2/5] Store chunks themselves (may be needed for
        #       bind_blocks_to_files() below).
        if _message.chunks_by_uuid is not None:
            logger.debug('Progress %r with chunks_by_uuid', self)

            try:
                # Run several attempts to write into RDB.
                for i in xrange(MAX_RDB_WRITE_ATTEMPTS):
                    try:
                        with db.RDB() as rdbw:
                            # Actual RDB write
                            Queries.Chunks.add_chunks(
                                _message.chunks_by_uuid.itervalues(), rdbw)
                    except:
                        logger.warning('Attempt %d to write chunks failed', i)
                        if i == MAX_RDB_WRITE_ATTEMPTS - 1:
                            logger.error('Giving up to write chunks')
                            raise
                        else:
                            # Blocking on sleep() is bad, we know.
                            # But this is the last resort.
                            # So let's sleep and retry again.
                            sleep(SLEEP_ON_RDB_WRITE_RETRY.total_seconds())
                    else:
                        logger.debug('Wrote chunks successfully '
                                         'in %d retries!',
                                     i)
                        break  # need no more retries

            except:
                logger.exception('Could not add chunks!')
                # Debug rather than verbose; on a real error, we want
                # as much information as available.
                logger.debug('chunks_by_uuid = %r', _message.chunks_by_uuid)
                raise

        # [3/5] Store chunks-per-host - chunks uploaded to some host.
        if _message.chunks_map_getter is not None:
            chunks_map = _message.chunks_map_getter()
            logger.debug('Progress %r with chunks_map', self)
            for target_host, notifications in chunks_map.iteritems():
                logger.debug('Marking notifications for host %r: %r',
                             target_host, notifications)

                try:
                    # Run several attempts to write into RDB.
                    for i in xrange(MAX_RDB_WRITE_ATTEMPTS):
                        try:
                            with db.RDB() as rdbw:
                                # Actual RDB write
                                TrustedQueries.TrustedChunks \
                                              .chunks_are_uploaded_to_the_host(
                                                   host_uuid, target_host.uuid,
                                                   notifications, rdbw)
                        except:
                            logger.warning('Attempt %d to write '
                                               'chunks-per-host failed',
                                           i)
                            if i == MAX_RDB_WRITE_ATTEMPTS - 1:
                                logger.error('Giving up to write '
                                             'chunks-per-host')
                                raise
                            else:
                                # Blocking on sleep() is bad, we know.
                                # But this is the last resort.
                                # So let's sleep and retry again.
                                sleep(SLEEP_ON_RDB_WRITE_RETRY.total_seconds())
                        else:
                            logger.debug('Wrote chunks-per-host successfully '
                                             'in %d retries!',
                                         i)
                            break

                except:
                    logger.exception('Could not mark chunks as uploaded!')
                    # Debug rather than verbose; on a real error, we want
                    # as much information as available.
                    logger.debug('chunks_by_uuid = %r',
                                 _message.chunks_by_uuid)
                    logger.debug('chunks_map = %r', chunks_map)
                    raise

        # [4/5] Store blocks mapping.
        if _message.blocks_map is not None:
            logger.debug('Progress %r with block_map', self)

            try:
                # Run several attempts to write into RDB.
                for i in xrange(MAX_RDB_WRITE_ATTEMPTS):
                    try:
                        with db.RDB() as rdbw:
                            # Actual RDB write
                            Queries.Blocks.bind_blocks_to_files(
                                host_uuid, ds_uuid, _message.blocks_map, rdbw)
                    except:
                        logger.warning('Attempt %d to write blocks failed',
                                       i)
                        if i == MAX_RDB_WRITE_ATTEMPTS - 1:
                            logger.error('Giving up to write blocks')
                            raise
                        else:
                            # Blocking on sleep() is bad, we know.
                            # But this is the last resort.
                            # So let's sleep and retry again.
                            sleep(SLEEP_ON_RDB_WRITE_RETRY.total_seconds())
                    else:
                        logger.debug('Wrote blocks successfully '
                                         'in %d retries!',
                                     i)
                        break

            except:
                logger.exception('Could not bind blocks!')
                # Debug rather than verbose; on a real error, we want
                # as much information as available.
                logger.debug('chunks_by_uuid = %r', _message.chunks_by_uuid)
                logger.debug('blocks_map = %r', _message.blocks_map)
                raise

        # [5/5] Finalize the dataset if needed, but only if no errors occurred
        #       during chunks/blocks writing.
        if dataset is not None and _message.completion:
            logger.info('Backup just completed!')
            with db.RDB() as rdbw:
                # 1. write the "backup completed" state
                Queries.Datasets.update_dataset(host_uuid, dataset, rdbw)
                # 2. mark that the dataset was synced to the host.
                TrustedQueries.TrustedDatasets.mark_dataset_as_synced_to_host(
                    ds_uuid, host_uuid, rdbw)

        # We are now done with writing the data received from the host.
        # Do the additional operations (with a separate database lock).

        if dataset is not None and _message.completion:
            # Restore the dataset to the hosts which do not
            # contain it yet
            self.manager.app.syncer.restore_dataset_to_lacking_hosts(
                me=me, host=host, ds_uuid=ds_uuid)


    def on_end(self):
        # We completed PROGRESS processing, so let's prepare the reply message.
        self.message_ack = self.message.reply()

        self.manager.post_message(self.message_ack)
