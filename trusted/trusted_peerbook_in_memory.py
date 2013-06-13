#!/usr/bin/python
"""The peer book on the node, implemented as an in-memory storage.

@note: seems obsoleted.
"""

#
# Imports
#

from __future__ import absolute_import

import collections as col
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from common.abstract_peerbook import (
    AbstractTrustedHostsEnabledPeerBookMixin,
    AbstractUsersEnabledPeerBookMixin)
from common.abstractions import AbstractInhabitant
from common.db import Queries
from common.inhabitants import Host, User, HostAtNode
from common.peerbook_in_memory import PeerBookInMemory
from common.utils import gen_uuid

from .db import TrustedQueries, get_prev_time_quant



#
# Logging and constants/variables
#

logger = logging.getLogger(__name__)



#
# Classes
#

class TrustedPeerBookInMemory(PeerBookInMemory,
                              AbstractUsersEnabledPeerBookMixin,
                              AbstractTrustedHostsEnabledPeerBookMixin):
    """The storage of the peers as they are known to the node, memory-based.

    @ivar __users_by_user_name_uc: the cache of all the users registered
        in the system; the key is an uppercased username and the value is
        the C{User} object.
    @type __users_by_user_name_uc: col.MutableMapping

    @ivar __hosts_by_user_name_uc: the cache of all hosts for any given
        user name, uppercased.
        The key is the uppercased username, the value is the set
        of the C{HostAtNode} objects.

    @note: The hosts in C{_peer_cache} are bound to appropriate users
        (stored in C{__users_by_user_name_uc} instance variable).
    """
    __slots__ = ('last_seen_time_quants',
                 '__users_by_user_name_uc', '__hosts_by_user_name_uc')


    def __init__(self, *args, **kwargs):
        super(TrustedPeerBookInMemory, self).__init__(*args, **kwargs)
        self.last_seen_time_quants = defaultdict(lambda: datetime.min)


    @contract_epydoc
    def __setitem__(self, key, value):
        """
        @type key: UUID
        @type value: AbstractInhabitant
        """
        with self._db_write_lock:
            super(TrustedPeerBookInMemory, self).__setitem__(key, value)
            _host = value
            self.__hosts_by_user_name_uc \
                .setdefault(_host.user.name.upper(), set()) \
                .add(_host)


    def _db_read_all_peers_to_cache(self):
        """
        Overridden method, comparing to the implementation in C{PeerBook}:
        it gets the HostAtNode objects rather than AbstractInhabitant;
        also, they are bound to the appropriate users.
        """
        with self._rdbw_factory() as rdbw:
            users = Queries.Inhabitants.get_all_users(rdbw)
            self.__users_by_user_name_uc = {user.name.upper(): user
                                                for user in users}

            _hosts = TrustedQueries.HostAtNode.get_all_hosts(
                         self.__users_by_user_name_uc, rdbw)
            self._peer_cache = {host.uuid: host
                                    for host in _hosts}

            # List is to get the value out of the cursor
            thosts_parts = \
                list(TrustedQueries.HostAtNode.get_all_trusted_hosts(
                         rdbw=rdbw))

        self._thost_uuids_for_storage = {host.uuid
                                             for host in thosts_parts
                                                 if host.for_storage}

        self._thost_uuids_for_restore = {host.uuid
                                             for host in thosts_parts
                                                 if host.for_restore}

        self.__hosts_by_user_name_uc = _hosts_by_user_name = {}
        for host in self._peer_cache.itervalues():
            _hosts_by_user_name.setdefault(host.user.name.upper(), set()) \
                               .add(host)
        logger.debug('Reread done, %i users, %i peers',
                     len(self.__users_by_user_name_uc),
                     len(self.__hosts_by_user_name_uc))
        logger.debug('Users: %r', self.__users_by_user_name_uc.keys())
        logger.debug('Hosts: %r', self.__hosts_by_user_name_uc)


    def _db_write_new_peers(self, peers):
        """
        Overridden method, comparing to the implementation in C{PeerBook}:
        it could've write the C{HostAtNode} objects rather
        than C{AbstractInhabitant}, but instead it will do nothing,
        as all the new peers are written immediately inside C{mark_added()}
        below.
        """
        pass


    def _db_write_updated_peers(self, peers):
        """
        Overridden method, comparing to the implementation in C{PeerBook}:
        it writes the HostAtNode objects rather than AbstractInhabitant.
        """
        return TrustedQueries.HostAtNode.update_peers(peers)


    def mark_added(self, peer):
        """
        Override C{mark_added()} method from the superclass,
        executing the immediate writing the new host into the DB
        instead of storing in the cache.

        @type peer: AbstractInhabitant
        """
        # Note there is no "with self._db_write_lock:" here,
        # and the parent super(TrustedPeerbookInMemory, self).mark_added(peer)
        # is not called, since we are writing the peer immediately into the DB.
        logger.debug('Immediately writing %r to DB', peer)
        TrustedQueries.HostAtNode.add_peers([peer])


    @contract_epydoc
    def __update_host_timestamp(self, key, timestamp,
                                urls=None, timeout=timedelta(seconds=10)):
        """
        Given a host UUID, update its last-seen time to a specific timestamp
        The new last-seen time is immediately written into the DB.
        Probably, the urls are also updated.

        @type key: UUID
        @precondition: key in self._peer_cache

        @timestamp: datetime

        @param urls: Either the list of URLs to use, or None.
                     If urls is None, the urls just will not be updated.
        @type urls: NoneType, list
        """
        # Update the host "last seen" in the memory.
        assert key in self._peer_cache
        host = self._peer_cache[key]
        assert host.uuid == key, (host, key)
        host.last_seen = timestamp
        if urls is not None:
            host.urls = urls
            self.mark_changed(host)

        logger.debug('The host %s marked as last-seen-alive on %s',
                     host.uuid, timestamp)

        # We need to update the presence statistics (directly in the database);
        # but only if it hasn't been updated yet, and if the time is sane.
        if timestamp is not datetime.min:
            _prev_quant = get_prev_time_quant(timestamp)

            if _prev_quant > self.last_seen_time_quants[key]:
                with self._rdbw_factory(timeout=timeout.total_seconds()) \
                        as rdbw:
                    TrustedQueries.PresenceStat \
                                  .insert_presence_stat(key, _prev_quant,
                                                        rdbw=rdbw)

                self.last_seen_time_quants[key] = _prev_quant


    @contract_epydoc
    def mark_as_just_seen_alive(self, key, urls):
        """
        Implements the @abstractmethod from C{AbstractPeerBook}.

        @type key: UUID
        @precondition: key in self._peer_cache
        """
        super(TrustedPeerBookInMemory, self).mark_as_just_seen_alive(key, urls)
        self.__update_host_timestamp(key, datetime.utcnow(), urls)


    @contract_epydoc
    def mark_as_just_logged_out(self, key):
        """
        Implements the @abstractmethod from C{AbstractPeerBook}.

        @type key: UUID
        @precondition: key in self._peer_cache
        """
        super(TrustedPeerBookInMemory, self).mark_as_just_logged_out(key)
        self.__update_host_timestamp(key, datetime.min)


    @contract_epydoc
    def get_user_by_name(self, username):
        """
        Implements the @abstractmethod from
        C{AbstractUsersEnabledPeerBookMixin}.

        @rtype: User, NoneType
        """
        super(TrustedPeerBookInMemory, self).get_user_by_name(username)
        return self.__users_by_user_name_uc.get(username.upper(), None)


    @contract_epydoc
    def get_user_hosts(self, username):
        """
        Implements the @abstractmethod from
        C{AbstractUsersEnabledPeerBookMixin}.

        @rtype: col.Iterable
        @postcondition: consists_of(result, Host) # result
        """
        super(TrustedPeerBookInMemory, self).get_user_hosts(username)
        logger.debug('Getting hosts for %r', username)
        return frozenset(self.__hosts_by_user_name_uc.get(username.upper(),
                                                          {}))


    @contract_epydoc
    def create_new_host_for_user(self, username):
        """
        Implements the @abstractmethod from
        C{AbstractUsersEnabledPeerBookMixin}.

        @rtype: Host
        """
        super(TrustedPeerBookInMemory, self).create_new_host_for_user(username)
        uuid_created = False

        while not uuid_created:
            host_uuid = gen_uuid()
            # Ensure it doesn't start with 0x00 byte
            uuid_created = (host_uuid.bytes[0] != '\x00')

        host = HostAtNode(uuid=host_uuid,
                          urls=[],
                          # Host-specific
                          name=str(host_uuid),
                          user=self.get_user_by_name(username),
                          # HostAtNode-specific
                          last_seen=None)
        self[host_uuid] = host
        return host


    @contract_epydoc
    def alive_trusted_hosts(self, for_storage=None, for_restore=None):
        """
        Implements the @abstractmethod from
        C{AbstractTrustedHostsEnabledPeerBookMixin}.

        @precondition: for_storage or for_restore # (for_storage, for_restore)

        @type for_storage: bool, NoneType
        @type for_restore: bool, NoneType

        @rtype: col.Iterable
        """
        super(TrustedPeerBookInMemory, self).alive_trusted_hosts(for_storage,
                                                              for_restore)
        thosts_to_take = set()

        if for_storage is not None:
            if for_storage:
                thosts_to_take |= self._thost_uuids_for_storage
            else:
                raise NotImplementedError()  # TODO if ever needed

        if for_restore is not None:
            if for_restore:
                thosts_to_take |= self._thost_uuids_for_restore
            else:
                raise NotImplementedError()  # TODO if ever needed

        uuids_to_scheck = \
            set(self._get_alive_peer_uuids(filter_uuid=None)) & thosts_to_take

        return {self._peer_cache[u] for u in uuids_to_scheck}
