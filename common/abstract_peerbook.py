#!/usr/bin/python
"""
The general interface which should be satisfied by the implementations
of Peer Book.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import numbers
from datetime import timedelta
from abc import ABCMeta, abstractmethod



#
# Constants
#

HOST_DEATH_TIME = timedelta(minutes=1)
"""
After this time period passed without the heartbeats,
the host is pronounced legally dead.
"""



#
# Classes
#

class AbstractPeerBook(col.MutableMapping):
    """
    An abstract Peer Book implementation, tracking the peers.
    """

    __metaclass__ = ABCMeta

    __slots__ = ('_rdbw_factory',)


    def __init__(self, rdbw_factory):
        self._rdbw_factory = rdbw_factory


    @abstractmethod
    def __getitem__(self, key):
        """
        @type key: UUID

        @returns: peer by its UUID.
        @rtype value: AbstractInhabitant
        """
        pass


    @abstractmethod
    def __setitem__(self, key, value):
        """
        @type key: UUID
        @type value: AbstractInhabitant
        """
        pass


    @abstractmethod
    def __delitem__(self, key):
        """
        The items probably shouldn't ever be deleted.
        @type key: UUID
        """
        raise NotImplementedError()


    def __iter__(self):
        raise NotImplementedError()


    def __len__(self):
        return self.peers_count()


    @abstractmethod
    def peers(self):
        """Return all peers. Use with care!"""
        pass


    def peers_count(self):
        """The number of peers.

        The function provides the generic calculation algorithm,
        but is a subject to performance-wise improvement override.

        @rtype: numbers.Integral
        """
        return sum(1 for i in self.peers())


    @abstractmethod
    def alive_peers(self, filter_uuid):
        """
        Get the set of the peers which are considered alive, i.e.
        they were present online very recently.

        @param filter_uuid: If not empty, then the hosts with this UUID
                            should be ignored.
        @type filter_uuid: (NoneType, UUID)

        @rtype: set
        @postcondition: consists_of(result, AbstractInhabitant)
        """
        pass


    def alive_peers_count(self):
        """The number of alive peers.

        The function provides the generic calculation algorithm,
        but is a subject to performance-wise improvement override.

        @rtype: numbers.Integral
        """
        return sum(1 for i in self.alive_peers())


    @abstractmethod
    def is_peer_alive(self, key):
        """
        @param key: The uuid of the peer which is verified for being alive.
        @type key: UUID

        @return: Whether the verified peer exists and is alive now.s
        @rtype: bool
        """
        pass


    @abstractmethod
    def mark_as_just_seen_alive(self, key, urls):
        """Given a host UUID, update its last-seen time.

        The new last-seen time is immediately written into the DB.

        @type key: UUID

        @param urls: either the list of URLs to use, or C{None}.
                     If urls is C{None}, the urls just will not be updated.
        """
        pass


    @abstractmethod
    def mark_as_just_logged_out(self, key):
        """
        Given a host UUID, update its last-seen time like it's just logged out.
        The new last-seen time is immediately written into the DB.

        @type key: UUID
        """
        pass



class AbstractUsersEnabledPeerBookMixin(object):
    """
    An abstract mixin which may be multiply inherited (together with
    C{AbstractPeerBook}) to enhance it with tracking the users.
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def get_user_by_name(self, username):
        """
        Given a (case-insensitive) username, return appropriate C{User} object
        (or C{None}).

        @rtype: User, NoneType
        """
        pass


    @abstractmethod
    def get_user_hosts(self, username):
        """
        Given a (case-insensitive) username, return all hosts which are
        available for this user.

        @rtype: col.Iterable
        @postcondition: consists_of(result, Host) # result
        """
        pass


    @abstractmethod
    def create_new_host_for_user(self, username):
        """
        For an existing user, create a new host, and return it.
        Note that the UUIDs starting with 0x00 are system-reserved
        and should not be used for hosts.

        @rtype: Host
        """
        pass



class AbstractTrustedHostsEnabledPeerBookMixin(object):
    """
    An abstract mixin which may be multiply inherited (together with
    C{AbstractPeerBook}) to enhance it with tracking the trusted hosts.
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def alive_trusted_hosts(self, for_storage, for_restore):
        """Get the Trusted Hosts which are alive at the moment.

        @precondition: for_storage or for_restore # (for_storage, for_restore)

        @param for_storage: whether the expected Trusted Hosts must be usable
            for in-house storage.
            C{True} for "must be usable", C{False} for "must be unusable",
            C{None} for "do not care".
        @type for_storage: bool, NoneType

        @param for_restore: whether the expected Trusted Hosts must be usable
            for web restore process.
            C{True} for "must be usable", C{False} for "must be unusable",
            C{None} for "do not care".
        @type for_restore: bool, NoneType

        @returns: the iterable over the trusted hosts which are alive now.
        @rtype: col.Iterable
        """
