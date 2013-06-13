#!/usr/bin/python
"""
The FastDB model for the system user.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import numbers
from datetime import datetime
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import consists_of, contract_epydoc

from common.typed_uuids import HostUUID
from common.utils import coalesce

from trusted.ibsonable import IBSONable

from .idocstoredocument import IDocStoreAnyDocument, IDocStoreTopDocument



#
# Classes
#

class Host(IDocStoreAnyDocument, IBSONable):
    """
    The information about a host, its presence and its availability.
    """

    __slots__ = (
        'uuid', 'urls', 'last_seen', 'revive_ts', 'last_msg_sync_ts'
    )

    bson_schema = {
        'uuid': UUID,
        'urls': lambda urls: consists_of(urls, basestring),
        'last_seen': datetime,
        # TODO: change revive_ts to C{datetime} as soon as all records
        # are converted.
        'revive_ts': (datetime, NoneType),
        'last_msg_sync_ts': datetime
    }


    @contract_epydoc
    def __init__(self,
                 uuid, urls,
                 last_seen=datetime.min, revive_ts=datetime.min,
                 last_msg_sync_ts=datetime.min,
                 *args, **kwargs):
        """Constructor.

        @param uuid: the UUID of the Host.
        @type uuid: UUID

        @param urls: the iterable with the urls of the host.
        @type urls: col.Iterable

        @param last_seen: the last time when the host was seen online.
            Is not reset to C{datetime.min} on logout.
        @type last_seen: datetime

        @param revive_ts: the last time when the host was seen revived
            to be considered online.
            Is reset to C{datetime.min} on logout.
        @type revive_ts: datetime

        @param last_msg_sync_ts: the last time when any message
            was successfully synced. to this host.
        @type last_msg_sync_ts: datetime
        """
        super(Host, self).__init__(*args, **kwargs)
        self.uuid = HostUUID.safe_cast_uuid(uuid)
        self.urls = list(urls)
        self.last_seen = last_seen
        self.revive_ts = revive_ts
        self.last_msg_sync_ts = last_msg_sync_ts


    def to_bson(self):
        cls = self.__class__

        doc = super(Host, self).to_bson()
        doc.update({
            'uuid': self.uuid,
            'urls': self.urls,
            'last_seen': self.last_seen,
            'revive_ts': self.revive_ts,
            'last_msg_sync_ts': self.last_msg_sync_ts,
        })

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(Host, cls).from_bson(doc),
                       uuid=HostUUID.safe_cast_uuid(doc['uuid']),
                       urls=doc['urls'],
                       last_seen=doc['last_seen'],
                       # TODO: change to doc['revive_ts'] when all records
                       # are converted.
                       revive_ts=doc.get('revive_ts', datetime.min),
                       last_msg_sync_ts=doc['last_msg_sync_ts'])



class User(IDocStoreTopDocument, IBSONable):
    """
    A system user, together with the information about its hosts.
    """

    __slots__ = (
        'name', 'digest', 'hosts', 'last_msg_read_ts',
        # Optional in the storage
        'trusted',
    )

    bson_schema = {
        'name': basestring,
        'digest': basestring,
        'hosts': lambda hosts: all(Host.validate_schema(h) for h in hosts),
        'hostsN': numbers.Integral,
        'last_msg_read_ts': datetime,
        # optional fields
        'trusted': (bool, NoneType),
    }


    @contract_epydoc
    def __init__(self, name, digest,
                 hosts=None, last_msg_read_ts=None,
                 trusted=False,
                 *args, **kwargs):
        """
        @param name: the name of the user.
        @type name: basestring

        @param digest: the user digest.
        @type digest: str
        @precondition: len(digest) == 40

        @param hosts: the iterable over the C{Host} objects.
        @type hosts: col.Iterable, NoneType

        @type last_msg_read_ts: datetime, NoneType

        @param trusted: whether the hosts of the user are trusted.
        @type trusted: bool
        """
        super(User, self).__init__(*args, **kwargs)
        self.name = name
        self.digest = digest
        # Optional constructor arguments
        self.hosts = list(coalesce(hosts, []))
        self.last_msg_read_ts = coalesce(last_msg_read_ts, datetime.min)
        # Optional document fields
        self.trusted = trusted


    def to_bson(self):
        cls = self.__class__

        doc = super(User, self).to_bson()

        doc.update({
            'name': self.name,
            'name_uc': self.name.upper(),
            'digest': self.digest,
            'hosts': self.hosts,
            'hostsN': len(self.hosts),
            'last_msg_read_ts': self.last_msg_read_ts,
        })

        # Optional fields
        if self.trusted:
            doc.update({'trusted': True})

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        assert cls.validate_schema(doc), repr(doc)
        assert len(doc['hosts']) == doc['hostsN'], repr(doc)

        return partial(super(User, cls).from_bson(doc),
                       name=str(doc['name']),
                       digest=str(doc['digest']),
                       hosts=doc['hosts'],
                       last_msg_read_ts=doc['last_msg_read_ts'],
                         # Optional
                       trusted=doc.get('trusted', False))
