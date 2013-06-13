#!/usr/bin/python
"""
The container which is able to store all the available peers (in the memory),
providing the transparent access to the database backend.
"""

#
# Imports
#

from __future__ import absolute_import

import collections as col
import logging
from datetime import datetime, timedelta
from itertools import chain
from threading import RLock
from types import NoneType
from uuid import UUID

from twisted.internet import reactor, threads

from contrib.dbc import contract_epydoc, consists_of

from .abstract_peerbook import AbstractPeerBook, HOST_DEATH_TIME
from .abstractions import AbstractInhabitant
from .db import Queries
from .inhabitants import Host
from .utils import exceptions_logged, in_main_thread
from .twisted_utils import callInThread



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class PeerBookInMemory(AbstractPeerBook):
    """
    The peer storage, transparently holding the peers in the DB,
    and caching the data.

    @note: using .keys()/.values()/.items() is unsafe (and .iter-versions are
           even more unsafe) since getting all the items should occur under
           read-lock (or write-lock for now), for items not to be changed
           during the iteration.

    @ivar _peer_cache: the cache of all the inhabitants (users or node)
                       registered in the system.
    """
    __slots__ = ('_peer_cache',
                 '_flush_timer', '_db_write_lock',
                 '_to_flush_new_uuids', '_to_flush_updated_uuids')


    def __init__(self, *args, **kwargs):
        super(PeerBookInMemory, self).__init__(*args, **kwargs)
        self._flush_timer = None
        self._to_flush_new_uuids = set()
        self._to_flush_updated_uuids = set()
        self._db_write_lock = RLock()

        self.reread_cache()


    def _db_read_all_peers_to_cache(self):
        """Read all the peers from the database and refreshes the cache.

        This method may be overridden if one needs to get a special
        implementation on a subclass of the AbstractInhabitant.
        """
        raise NotImplementedError('FIXME if needed!')
        with self._rdbw_factory() as rdbw:
            _peers = chain(Queries.Inhabitants.get_all_nodes(rdbw),
                           Queries.Inhabitants.get_all_hosts(rdbw))
            self._peer_cache = {inh.uuid: inh
                                    for inh in _peers}


    def _db_write_new_peers(self, peers):
        """Flush the added peers to the database.

        This method may be overridden if one needs to get a special
        implementation on a subclass of the AbstractInhabitant.
        """
        raise NotImplementedError('FIXME if needed!')
        # Need to update the peers properly, depending on whether
        # it is a Host or a Node
        # with self._rdbw_factory() as rdbw:
        #     return Queries.Inhabitants.update_peers(peers, rdbw)


    def _db_write_updated_peers(self, peers):
        """Flush the updated peers to the database.

        This method may be overridden if one needs to get a special
        implementation on a subclass of the AbstractInhabitant.
        """
        raise NotImplementedError('FIXME if needed!')
        # Need to update the peers properly, depending on whether it is a Host
        # or a Node
        # with self._rdbw_factory() as rdbw:
        #     return Queries.Inhabitants.update_peers(peers, rdbw)


    def reread_cache(self):
        """Unconditionally reread all the data from the database.

        @todo: There actually should be separate db_read and db_write locks.
        """
        logger.debug('Rereading RDB cache')
        with self._db_write_lock:
            self._db_read_all_peers_to_cache()
        logger.debug('Rereading done, %i peers', len(self._peer_cache))


    def flush_cache(self):
        """
        Unconditionally write all the changed data into the database.
        """
        assert not in_main_thread()
        logger.debug('Flushing RDB cache')
        with self._db_write_lock:
            if self._to_flush_new_uuids:
                logger.debug('Adding peer(s): %i, %r',
                             len(self._to_flush_new_uuids),
                             self._to_flush_new_uuids)
                self._db_write_new_peers(
                    self._peer_cache[u] for u in self._to_flush_new_uuids)
                self._to_flush_new_uuids.clear()
            else:
                logger.debug('Adding peers not needed')

            if self._to_flush_updated_uuids:
                logger.debug('Updating peer(s): %i, %r',
                             len(self._to_flush_updated_uuids),
                             self._to_flush_updated_uuids)
                self._db_write_updated_peers(
                    self._peer_cache[u] for u in self._to_flush_updated_uuids)
                self._to_flush_updated_uuids.clear()
            else:
                logger.debug('Updating peers: not needed')

            logger.debug('Wrote peers, resetting timer')
            self._flush_timer = None


    @exceptions_logged(logger)
    def __on_flush_timer(self):
        assert not in_main_thread()
        self.flush_cache()


    @contract_epydoc
    def __mark_upserted(self, peer):
        """
        Given a peer, mark it as either added or change in the cache,
        and raise a flush timer if needed.

        @type peer: AbstractInhabitant
        """
        if self._flush_timer is None:
            _callLater = reactor.callLater  # pylint:disable=E1101,C0103
            self._flush_timer = \
                _callLater(10.0,
                           lambda: callInThread(self.__on_flush_timer))
            logger.debug('Starting flush timer...')


    @contract_epydoc
    def mark_added(self, peer):
        """
        Given a peer, mark it as added to the cache,
        and raise a flush timer if needed.

        @type peer: AbstractInhabitant
        """
        with self._db_write_lock:
            self._to_flush_new_uuids.add(peer.uuid)
            self.__mark_upserted(peer)


    @contract_epydoc
    def mark_changed(self, peer):
        """
        Given a peer, mark it as changed in the cache,
        and raise a flush timer if needed.

        @type peer: AbstractInhabitant
        """
        with self._db_write_lock:
            self._to_flush_updated_uuids.add(peer.uuid)
            self.__mark_upserted(peer)


    def __iter__(self):
        return iter(self._peer_cache)


    @contract_epydoc
    def __contains__(self, key):
        """
        @type key: UUID
        """
        return key in self._peer_cache


    @contract_epydoc
    def __getitem__(self, key):
        """
        @type key: UUID
        """
        return self._peer_cache[key]


    @contract_epydoc
    def __setitem__(self, key, value):
        """
        @type key: UUID
        @type value: AbstractInhabitant
        """
        with self._db_write_lock:
            if key in self._peer_cache:
                self.mark_changed(value)
            else:
                self.mark_added(value)
            self._peer_cache[key] = value


    @contract_epydoc
    def __delitem__(self, key):
        """
        The items probably shouldn't be deleted.
        @type key: UUID
        """
        raise NotImplementedError()


    def __len__(self):
        return len(self._peer_cache)


    def peers(self):
        with self._db_write_lock:  # should be db_read_lock actually
            # Returns the list rather than the iterable, to release the lock
            # as early as possible.
            return self.values()


    @contract_epydoc
    def _get_alive_peer_uuids(self, filter_uuid=None):
        """
        Get the set of the UUIDs of the peers which are considered alive, i.e.
        they were present online very recently.

        @param filter_uuid: If not empty, then the hosts with this UUID
                            should be ignored.
        @type filter_uuid: NoneType, UUID

        @rtype: set
        @postcondition: consists_of(result, UUID)
        """
        _not_earlier_than = datetime.utcnow() - HOST_DEATH_TIME
        return {uuid
                    for uuid, host in self._peer_cache.iteritems()
                    if (filter_uuid is None or uuid != filter_uuid) and
                       host.last_seen >= _not_earlier_than}


    @contract_epydoc
    def alive_peers(self, filter_uuid=None):
        """
        Implements the @abstractmethod from C{AbstractPeerBook}.
        """
        super(PeerBookInMemory, self).alive_peers(filter_uuid)
        return {self._peer_cache[u]
                    for u in self._get_alive_peer_uuids(filter_uuid)}


    @contract_epydoc
    def is_peer_alive(self, key):
        """
        Implements the @abstractmethod from C{AbstractPeerBook}.
        """
        super(PeerBookInMemory, self).is_peer_alive(key)
        return (key in self._peer_cache and
                datetime.utcnow() - self._peer_cache[key].last_seen
                    < HOST_DEATH_TIME)
