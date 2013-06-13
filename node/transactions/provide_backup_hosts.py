#!/usr/bin/python
"""
PROVIDE_BACKUP_HOST transaction implementation on the Node.

When requested by the Host and given with a list of desired chunk sizes,
the node calculates which hosts can accept these chunks.
It filters out the requested host, and then pseudorandomly distributes
the chunks over the available hosts, keeping in mind the free space on them.

@todo: When distributing the chunks for the backup, it may happen that at
       the same time the replication onto the target host will occupy
       some chunks as well. This will mean that there will be more chunks
       on the host than it allocated.
       So, in future, the Node should track not only the currently available
       free space, but also the Node's plans to occupy it, either via backups
       or via replications.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import random
from functools import partial
from itertools import chain

from contrib.dbc import contract_epydoc

from common.abstractions import unpauses_incoming
from common.utils import coalesce, exceptions_logged

from protocol import transactions
from protocol.messages import ProvideBackupHostsMessage
from protocol.messages.provide_backup_hosts import PerHostChunksData

from trusted.db import TrustedQueries
from trusted.docstore.models.transaction_states.provide_backup_hosts \
    import ProvideBackupHostsTransactionState_Node

from ._node import AbstractNodeTransaction
from node import settings



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

PBHResultData = col.namedtuple('PBHResultData', ('result_code', 'result'))


class NoSpaceError(Exception):
    """
    The exception declaring the files could not be uploaded to the cloud.
    """
    pass



class ProvideBackupHostsTransaction_Node(
          transactions.ProvideBackupHostsTransaction,
          AbstractNodeTransaction):
    """PROVIDE_BACKUP_HOSTS transaction on the Node."""

    __slots__ = ()

    State = ProvideBackupHostsTransactionState_Node


    @unpauses_incoming
    def on_begin(self):
        pass


    @classmethod
    def _calculate_distribution(cls, chunks_map, hosts_free_space_map,
                                rng=None):
        r"""
        Calculate what number of (what kind) of chunks should be uploaded
        to what hosts.

        >>> from uuid import UUID

        >>> from common.logger import add_verbose_level; add_verbose_level()

        >>> rng = random.Random(42)

        >>> # Very simple
        >>> ProvideBackupHostsTransaction_Node._calculate_distribution(
        ...     chunks_map={0: 17, 1: 13},
        ...     hosts_free_space_map={
        ...         UUID('00000000-1111-0000-0000-000000000011'): 1073741824,
        ...     },
        ...     rng=rng
        ... )  # doctest:+NORMALIZE_WHITESPACE
        {UUID('00000000-1111-0000-0000-000000000011'): {0: 17, 1: 13}}

        >>> ProvideBackupHostsTransaction_Node._calculate_distribution(
        ...     chunks_map={0: 17, 1: 13},
        ...     hosts_free_space_map={
        ...         UUID('00000000-1111-0000-0000-000000000011'): 1073741824,
        ...         UUID('00000000-1111-0000-0000-000000000012'): 1073741824,
        ...     },
        ...     rng=rng
        ... )  # doctest:+NORMALIZE_WHITESPACE
        {UUID('00000000-1111-0000-0000-000000000011'): {0: 12, 1: 3},
         UUID('00000000-1111-0000-0000-000000000012'): {0: 5, 1: 10}}

        >>> # Make sure it doesn't corrupt the original mappings:
        >>> _chunks = {0: 5, 1: 7}
        >>> _free_space = {
        ...     UUID('00000000-1111-0000-0000-000000000011'): 12345670,
        ...     UUID('00000000-1111-0000-0000-000000000012'): 23456780,
        ...     UUID('00000000-1111-0000-0000-000000000013'): 34567890,
        ... }
        >>> ProvideBackupHostsTransaction_Node._calculate_distribution(
        ...     chunks_map=_chunks,
        ...     hosts_free_space_map=_free_space,
        ...     rng=rng
        ... )  # doctest:+NORMALIZE_WHITESPACE
        {UUID('00000000-1111-0000-0000-000000000011'): {0: 4, 1: 1},
         UUID('00000000-1111-0000-0000-000000000012'): {1: 2},
         UUID('00000000-1111-0000-0000-000000000013'): {0: 1, 1: 4}}

        >>> _chunks
        {0: 5, 1: 7}
        >>> _free_space  # doctest:+NORMALIZE_WHITESPACE
        {UUID('00000000-1111-0000-0000-000000000011'): 12345670,
         UUID('00000000-1111-0000-0000-000000000012'): 23456780,
         UUID('00000000-1111-0000-0000-000000000013'): 34567890}

        >>> # Finally, make sure that it handles the cloud limits well.
        >>> # 1. This still fits...
        >>> ProvideBackupHostsTransaction_Node._calculate_distribution(
        ...     chunks_map={2: 2, 3: 10},
        ...     hosts_free_space_map={
        ...         UUID('00000000-1111-0000-0000-000000000011'): 50331648,
        ...         UUID('00000000-1111-0000-0000-000000000012'): 41943040,
        ...     },
        ...     rng=rng
        ... )  # doctest:+NORMALIZE_WHITESPACE
        {UUID('00000000-1111-0000-0000-000000000011'): {3: 6},
         UUID('00000000-1111-0000-0000-000000000012'): {2: 2, 3: 4}}
        >>> # 2. But this doesn't fit.
        >>> ProvideBackupHostsTransaction_Node._calculate_distribution(
        ...     chunks_map={2: 2, 3: 10},
        ...     hosts_free_space_map={
        ...         UUID('00000000-1111-0000-0000-000000000011'): 50331648 - 1,
        ...         UUID('00000000-1111-0000-0000-000000000012'): 41943040,
        ...     },
        ...     rng=rng
        ... )
        Traceback (most recent call last):
          ...
        NoSpaceError

        @param chunks_map: the map describing the chunks to be uploaded.
            It maps the chunks size code to the number of such chunks.
            Eg: {0: 17, 1: 13}
        @type chunks_map: col.Mapping

        @param hosts_free_space_map: the mapping from the host UUID
            to the space available on it (in bytes, approximately).
            Eg: {UUID('00000000-1111-0000-0000-000000000011'): 1073741824}
        @type hosts_free_space_map: col.Mapping

        @param rng: a rng to use for distribution.
        @type rng: random.Random

        @return: the mapping from the host UUID (which will accept chunks)
            to the mapping of chunks to upload to it.
            The mapping of uploaded chunks looks like this:
            the chunk size code is mapped to the number of such chunks, eg:
            C{ {0: 8, 1: 5} }.
        @rtype: col.Mapping

        @raises NoSpaceError: if no solution is found to upload the chunks.
        """
        rng = coalesce(rng, random)

        # Make a copy, to never corrupt the original.
        _chunks_map = dict(chunks_map)
        _hosts_free_space_map = dict(hosts_free_space_map)

        logger.debug('Need to allocate %r at %r',
                     _chunks_map, _hosts_free_space_map)

        M = 1024 * 1024

        result = {}

        # Let's loop over the desired chunks (actually, its sizes)
        # by the size descending, and allocate each chunk to some host.
        _size_codes = \
            chain.from_iterable([sz for i in xrange(_chunks_map[sz])]
                                    for sz in sorted(_chunks_map.iterkeys(),
                                                     reverse=True))
        for chunk_size_code in _size_codes:
            this_chunk_size = 2 ** chunk_size_code * M

            # Which hosts still have enough space?
            candidate_host_uuids = \
                [u
                     for u, sz in _hosts_free_space_map.iteritems()
                     if this_chunk_size <= sz]
            logger.verbose('To allocate %r MiB, we have these hosts: %r',
                           this_chunk_size, candidate_host_uuids)
            if not candidate_host_uuids:
                # Oops, we still have chunks unallocated,
                # but no more host can receive it.
                raise NoSpaceError()
            else:
                use_host_uuid = rng.choice(candidate_host_uuids)

                # Mark some space on this host as occupied.
                _hosts_free_space_map[use_host_uuid] -= this_chunk_size

                # Take chunk counts for this host...
                per_host_chunks_counts = result.setdefault(use_host_uuid, {})
                # ... and increase the chunk count for the currently
                # processed chunk.
                per_host_chunks_counts[chunk_size_code] = \
                    per_host_chunks_counts.get(chunk_size_code, 0) + 1

        return result


    def __find_hosts(self):
        """
        @rtype: PBHResultData
        """
        cls = self.__class__

        result_code = ProvideBackupHostsMessage.ResultCodes.OK
        result = {}

        if __debug__:
            rng = random.Random(42)
        else:
            rng = None

        host = self.message.src
        feature_set = settings.get_feature_set()

        if feature_set.p2p_storage:
            _hosts = self.manager.app.known_hosts.alive_peers(
                         filter_uuid=host.uuid)
        else:
            _hosts = self.manager.app.known_hosts.alive_trusted_hosts(
                         for_storage=True)

        # Make sure _hosts is reiterable
        hosts = list(_hosts)
        logger.debug('For backup, will be using the following hosts: %r',
                     hosts)

        uuids_to_urls = {h.uuid: h.urls for h in hosts}

        # To be safe, let's distribute only the hosts which have urls defined
        # and non-empty
        host_uuids_to_distribute = frozenset(h.uuid for h in hosts
                                                    if h.urls)

        hosts_free_space_map = \
            {host_uuid: stat.free_size
                 for host_uuid, stat
                     in TrustedQueries.SpaceStat.get_hosts_space_stat()
                                                .iteritems()
                 if host_uuid in host_uuids_to_distribute}

        data = self.message.chunks_by_size_code

        if not host_uuids_to_distribute:
            # Probably, we cannot complete a backup.
            # But if there is not chunk data to backup,... yes we can!
            if sum(data.itervalues()) == 0:
                result_code = ProvideBackupHostsMessage.ResultCodes.OK
                logger.debug('%r attempts a backup with no chunks, '
                                 'when there are no hosts',
                             host)
            else:
                result_code = \
                    ProvideBackupHostsMessage.ResultCodes.NO_SPACE_ON_CLOUD
                logger.warning('%r attempts a backup, but there is no hosts!',
                               host)

        else:
            # We should try to complete a backup.
            logger.debug('Distributing to: %r', host_uuids_to_distribute)

            try:
                _result = cls._calculate_distribution(
                              chunks_map=data,
                              hosts_free_space_map=hosts_free_space_map,
                              rng=rng)

            except NoSpaceError:
                result_code = ProvideBackupHostsMessage.ResultCodes \
                                                       .NO_SPACE_ON_CLOUD
                result = {}
                logger.debug('Could not allocate all the required chunks '
                                 'due to error %i! %r, %r',
                             result_code, hosts_free_space_map, data)

            else:
                result_code = ProvideBackupHostsMessage.ResultCodes.OK
                result = {host_uuid: PerHostChunksData(
                                         urls=uuids_to_urls[host_uuid],
                                         chunks_by_size=per_host)
                              for host_uuid, per_host in _result.iteritems()}
                logger.debug('Allocated the chunks! %r, %r',
                             data, _result)

            logger.debug('Final result %r', result)

        # By now, we have calculated "result_code" and "result" variables.

        return PBHResultData(result_code=result_code,
                             result=result)


    def on_end(self):
        result_data = self.__find_hosts()

        self.message_ack = self.message.reply()
        self.message_ack.ack_hosts_to_use = result_data.result
        self.message_ack.ack_result_code = result_data.result_code

        self.manager.post_message(self.message_ack)
