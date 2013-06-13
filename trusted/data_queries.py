#!/usr/bin/python
"""Data queries that apply to Relational DB, Fast DB and Big DB simultaneously.
"""

from __future__ import absolute_import
from types import NoneType

from contrib.dbc import contract_epydoc

from common.crypto import gen_rand_key
from common.datatypes import TrustedHostCaps
from common.db import Queries
from common.inhabitants import UserGroup
from common.typed_uuids import HostUUID, UserGroupUUID

from .data_wrapper import DataWrapper
from .db import TrustedQueries
from .docstore.fdbqueries import FDBQueries



#
# Classes
#

class DataQueries(object):
    """
    Queries that utilize Relational DB, Fast DB and Big DB simultaneously.
    """
    __slots__ = ()


    class Inhabitants(object):
        """The namespace for queries regarding any network inhabitants."""

        __slots__ = ()


        @classmethod
        @contract_epydoc
        def add_user_with_group(cls,
                                username, group_uuid, digest, is_trusted,
                                dw):
            """Add the user (and the appropriate group to the data stores).

            @param group_uuid: UUID of the user group.
            @type group_uuid: UserGroupUUID

            @param is_trusted: whether the user stands for the Trusted Hosts.
            @type is_trusted: bool

            @param dw: data wrapper
            @type dw: DataWrapper
            """
            enc_key = gen_rand_key()

            group = UserGroup(uuid=group_uuid,
                              name=username,
                              private=True,
                              enc_key=enc_key)
            TrustedQueries.TrustedUsers.add_user_with_group(username, group,
                                                            digest, dw.rdbw)
            FDBQueries.Users.add_user(username=username,
                                      digest=digest,
                                      is_trusted=is_trusted,
                                      fdbw=dw.fdbw)


        @classmethod
        @contract_epydoc
        def add_host(cls,
                     username, hostname, host_uuid, trusted_host_caps, dw):
            """Add a host to the user.

            @param host_uuid: UUID of the host to add.
            @type host_uuid: HostUUID

            @param trusted_host_caps: the capabilities of the Trusted Host
                (if applicable).
            @type trusted_host_caps: TrustedHostCaps, NoneType

            @param dw: data wrapper
            @type dw: DataWrapper
            """
            TrustedQueries.HostAtNode.add_host(username, hostname,
                                               host_uuid, trusted_host_caps,
                                               rdbw=dw.rdbw)
            FDBQueries.Users.add_host(username=username,
                                      host_uuid=host_uuid,
                                      fdbw=dw.fdbw)


        @classmethod
        @contract_epydoc
        def update_user_digest(cls, username, digest, dw):
            """Update the digest for the user.

            @param username: the name of the user.
            @type username: basestring

            @param digest: the digest of the user.
            @type digest: str
            @precondition: len(digest) == 40

            @param dw: data wrapper
            @type dw: DataWrapper
            """
            assert dw.rdbw is not None and dw.fdbw is not None, repr(dw)
            Queries.Inhabitants.update_user_digest(username, digest,
                                                   rdbw=dw.rdbw)
            FDBQueries.Users.update_user_digest(username=username,
                                                digest=digest,
                                                fdbw=dw.fdbw)
