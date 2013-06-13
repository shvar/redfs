#!/usr/bin/python
"""The peer book on the node, using FastDB as a backend."""

#
# Imports
#

from __future__ import absolute_import

import collections as col
import logging
from datetime import datetime
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.abstract_peerbook import (
    HOST_DEATH_TIME, AbstractPeerBook, AbstractUsersEnabledPeerBookMixin,
    AbstractTrustedHostsEnabledPeerBookMixin)
from common.abstractions import AbstractInhabitant
from common.inhabitants import Host, User
from common.typed_uuids import HostUUID
from common.utils import in_main_thread, gen_uuid

from trusted.data_queries import DataQueries
from trusted.data_wrapper import DataWrapper
from trusted.db import TrustedQueries
from trusted.docstore.fdbqueries import FDBQueries



#
# Constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class PeerBookInFastDB(AbstractPeerBook,
                       AbstractUsersEnabledPeerBookMixin,
                       AbstractTrustedHostsEnabledPeerBookMixin):
    """
    The storage of the peers as they are known to the cloud, using the FastDB.
    """
    __slots__ = ('__fdbw_factory',)


    def __init__(self, fdbw_factory, *args, **kwargs):
        """Constructor.

        @type fdbw_factory: DocStoreWrapperFactory
        """
        super(PeerBookInFastDB, self).__init__(*args, **kwargs)
        self.__fdbw_factory = fdbw_factory


    @contract_epydoc
    def __setitem__(self, key, value):
        """
        @type key: UUID
        @type value: AbstractInhabitant
        """
        raise NotImplementedError()
        with self._db_write_lock:
            super(PeerBookInFastDB, self).__setitem__(key, value)
            _host = value
            self.__hosts_by_user_name_uc \
                .setdefault(_host.user.name.upper(), set()) \
                .add(_host)


    @contract_epydoc
    def __getitem__(self, key):
        """
        @type key: UUID

        @rtype: Host
        """
        with self.__fdbw_factory() as fdbw:
            host_with_user = FDBQueries.Users.get_host_with_user(
                                 host_uuid=HostUUID.safe_cast_uuid(key),
                                 fdbw=fdbw)

        if host_with_user is None:
            return None
        else:
            host_model, user_model = host_with_user

            user = User(name=user_model.name,
                        digest=user_model.digest)

            assert key == host_model.uuid, \
                   (key, user)

            host = Host(uuid=host_model.uuid,
                        urls=host_model.urls,
                        # Host-specific
                        user=user)

            return host


    @contract_epydoc
    def __delitem__(self, key):
        """The items probably shouldn't be deleted.

        @type key: UUID
        """
        raise NotImplementedError()


    @contract_epydoc
    def __len__(self):
        """Implements the @abstractmethod from C{AbstractPeerBook}."""
        return self.peers_count()


    def peers(self):
        """Implements the @abstractmethod from C{AbstractPeerBook}.

        At the moment, C{PeerBookInFastDB.peers} is equal to
        C{PeerBookInFastDB.hosts}.

        @returns: the (possibly non-reiterable) iterable of the Host objects
            available to the system.

        @note: the 'name' and 'user' fields are missing!
        """
        with self.__fdbw_factory() as fdbw:
            host_models = FDBQueries.Users.get_hosts(fdbw=fdbw)
            # list comprehension instead of generator expression,
            # so that fdbw context is released early.
            return [Host(uuid=HostUUID.safe_cast_uuid(host_model.uuid),
                         urls=host_model.urls)
                        for host_model in host_models]


    def peers_count(self):
        """
        Overrides the C{peers_count} from C{AbstractPeerBook}
        with a performance-wise improvement.
        """
        with self.__fdbw_factory() as fdbw:
            return FDBQueries.Users.get_hosts_count(fdbw=fdbw)


    @contract_epydoc
    def alive_peers(self, filter_uuid=None):
        """Implements the @abstractmethod from C{AbstractPeerBook}.
        """
        with self.__fdbw_factory() as fdbw:
            host_models = FDBQueries.Users.get_hosts(
                              oldest_revive_ts=datetime.utcnow()
                                               - HOST_DEATH_TIME,
                              fdbw=fdbw)

            # list comprehension instead of generator expression,
            # so that fdbw context is released early.
            return [Host(uuid=HostUUID.safe_cast_uuid(host_model.uuid),
                         urls=host_model.urls)
                        for host_model in host_models
                        if (filter_uuid is None or
                            host_model.uuid != filter_uuid)]


    @contract_epydoc
    def peers_revived_within_interval(self,
                                      oldest_revive_ts, newest_revive_ts):
        """
        Get all peers which were last revived between C{oldest_revive_ts}
        and C{newest_revive_ts}.

        If any of C{oldest_revive_ts} or C{newest_revive_ts}, the appropriate
        side of the interval is considered open.

        @note: custom, node-specific method.

        @type oldest_revive_ts: datetime, NoneType
        @type newest_revive_ts: datetime, NoneType

        @return: an iterable over C{model.Host} objects.
        @rtype: col.Iterable
        """
        with self.__fdbw_factory() as fdbw:
            host_models = FDBQueries.Users.get_hosts(
                              oldest_revive_ts=oldest_revive_ts,
                              newest_revive_ts=newest_revive_ts,
                              fdbw=fdbw)

            # list comprehension instead of generator expression,
            # so that fdbw context is released early.
            return [Host(uuid=HostUUID.safe_cast_uuid(host_model.uuid),
                         urls=host_model.urls)
                        for host_model in host_models]


    def alive_peers_count(self):
        """
        Overrides the C{alive_peers_count} from C{AbstractPeerBook}
        with a performance-wise improvement.
        """
        with self.__fdbw_factory() as fdbw:
            return FDBQueries.Users.get_hosts_count(
                       oldest_revive_ts=datetime.utcnow() - HOST_DEATH_TIME,
                       fdbw=fdbw)


    @contract_epydoc
    def is_peer_alive(self, key):
        """Implements the @abstractmethod from C{AbstractPeerBook}.

        @type key: UUID

        @rtype: bool
        """
        with self.__fdbw_factory() as fdbw:
            return FDBQueries.Users.is_peer_alive(
                       host_uuid=HostUUID.safe_cast_uuid(key),
                       oldest_revive_ts=datetime.utcnow() - HOST_DEATH_TIME,
                       fdbw=fdbw)


    @contract_epydoc
    def mark_as_just_seen_alive(self, key, urls):
        """Implements the @abstractmethod from C{AbstractPeerBook}.

        @type urls: col.Iterable

        @type key: UUID
        """
        assert not in_main_thread()
        with self.__fdbw_factory() as fdbw:
            last_revive_ts = FDBQueries.Users.update_host_info(
                                 host_uuid=HostUUID.safe_cast_uuid(key),
                                 urls=list(urls),
                                 timestamp=datetime.utcnow(),
                                 fdbw=fdbw)

            if last_revive_ts is None:
                # But is this code path supported now?
                logger.debug('Marking %s as alive for the first time', key)
            else:
                was_dead = datetime.utcnow() - last_revive_ts > HOST_DEATH_TIME
                if was_dead:
                    logger.debug('Marking %s as just seen alive, was dead',
                                 key)
                else:
                    logger.verbose('Marking %s as just seen alive, was alive',
                                   key)


    @contract_epydoc
    def mark_as_just_logged_out(self, key):
        """Implements the @abstractmethod from C{AbstractPeerBook}.

        @type key: UUID
        """
        with self.__fdbw_factory() as fdbw:
            last_revive_ts = FDBQueries.Users.update_host_info(
                                 host_uuid=HostUUID.safe_cast_uuid(key),
                                 urls=[],
                                 timestamp=None,  # "unsee" me!
                                 fdbw=fdbw)


    @contract_epydoc
    def get_user_by_name(self, username):
        """
        Implements the @abstractmethod from
        C{AbstractUsersEnabledPeerBookMixin}.

        @rtype: User, NoneType
        """
        with self.__fdbw_factory() as fdbw:
            user_doc = FDBQueries.Users.find_by_name(username=username,
                                                     fdbw=fdbw)

        return User(name=user_doc.name,
                    digest=user_doc.digest) if user_doc is not None \
                                            else None


    @contract_epydoc
    def get_user_hosts(self, username):
        """
        Implements the @abstractmethod from
        C{AbstractUsersEnabledPeerBookMixin}.

        @rtype: col.Iterable
        @result: a (possibly, non-reiterable) iterable of C{Host} objects.
        """
        with self.__fdbw_factory() as fdbw:
            host_models = FDBQueries.Users.get_hosts(username=username,
                                                     fdbw=fdbw)

            # list comprehension instead of generator expression,
            # so that fdbw context is released early.
            return [Host(uuid=HostUUID.safe_cast_uuid(host_model.uuid),
                         urls=host_model.urls)
                        for host_model in host_models]


    @contract_epydoc
    def create_new_host_for_user(self, username):
        """
        Implements the @abstractmethod from
        C{AbstractUsersEnabledPeerBookMixin}.

        @rtype: Host
        """
        uuid_created = False

        while not uuid_created:
            host_uuid = HostUUID.safe_cast_uuid(gen_uuid())
            # Ensure it doesn't start with 0x00 byte
            uuid_created = (host_uuid.bytes[0] != '\x00')

        host = Host(uuid=host_uuid,
                    urls=[],
                    # Host-specific
                    name=str(host_uuid),
                    user=self.get_user_by_name(username))

        with self._rdbw_factory() as rdbw, self.__fdbw_factory() as fdbw:
            dw = DataWrapper(rdbw=rdbw, fdbw=fdbw, bdbw=None)
            DataQueries.Inhabitants.add_host(username=username,
                                             hostname=str(host_uuid),
                                             host_uuid=host_uuid,
                                             trusted_host_caps=None,
                                             dw=dw)

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
        with self._rdbw_factory() as rdbw, self.__fdbw_factory() as fdbw:
            thosts = list(TrustedQueries.HostAtNode.get_all_trusted_hosts(
                              for_storage=for_storage,
                              for_restore=for_restore,
                              rdbw=rdbw))

            # Forcibly evaluated to the set to get the value out of the cursor
            thost_uuids = frozenset(th.uuid for th in thosts)

            # We've got all the trusted hosts.
            # Now filter only alive ones.
            host_models = FDBQueries.Users.get_hosts(
                              host_uuids=thost_uuids,
                              oldest_revive_ts=datetime.utcnow()
                                               - HOST_DEATH_TIME,
                              fdbw=fdbw)

            # list comprehension instead of generator expression,
            # so that fdbw context is released early.
            return [Host(uuid=HostUUID.safe_cast_uuid(host_model.uuid),
                         urls=host_model.urls)
                        for host_model in host_models]
